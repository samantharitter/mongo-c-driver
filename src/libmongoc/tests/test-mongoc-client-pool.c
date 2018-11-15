#include <mongoc/mongoc.h>

#include "mongoc/mongoc-client-private.h"
#include "mongoc/mongoc-client-pool-private.h"
#include "mongoc/mongoc-set-private.h"
#include "mongoc/mongoc-util-private.h"

#include "TestSuite.h"
#include "test-libmongoc.h"
#include "test-conveniences.h"
#include "mock_server/future.h"
#include "mock_server/future-functions.h"
#include "mock_server/mock-server.h"


static void
test_mongoc_client_pool_basic (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1");
   pool = mongoc_client_pool_new (uri);
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   mongoc_client_pool_push (pool, client);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}


static void
test_mongoc_client_pool_try_pop (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1");
   pool = mongoc_client_pool_new (uri);
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   BSON_ASSERT (!mongoc_client_pool_try_pop (pool));
   mongoc_client_pool_push (pool, client);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

static void
test_mongoc_client_pool_min_size_zero (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client1;
   mongoc_client_t *client2;
   mongoc_client_t *client3;
   mongoc_client_t *client4;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new (NULL);
   pool = mongoc_client_pool_new (uri);

   client1 = mongoc_client_pool_pop (pool);
   client2 = mongoc_client_pool_pop (pool);
   mongoc_client_pool_push (pool, client2);
   mongoc_client_pool_push (pool, client1);

   BSON_ASSERT (mongoc_client_pool_get_size (pool) == 2);
   client3 = mongoc_client_pool_pop (pool);

   /* min pool size zero means "no min", so clients weren't destroyed */
   BSON_ASSERT (client3 == client1);
   client4 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client4 == client2);

   mongoc_client_pool_push (pool, client4);
   mongoc_client_pool_push (pool, client3);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_client_pool_min_size_dispose (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_client_t *c0, *c1, *c2, *c3;

   capture_logs (true);
   uri = mongoc_uri_new ("mongodb://127.0.0.1/?minpoolsize=2");
   pool = mongoc_client_pool_new (uri);

   c0 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c0);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 1);

   c1 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c1);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 2);

   c2 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 3);

   c3 = mongoc_client_pool_pop (pool);
   BSON_ASSERT (c3);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 4);

   mongoc_client_pool_push (pool, c0); /* queue is [c0] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 1);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 4);

   mongoc_client_pool_push (pool, c1); /* queue is [c1, c0] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 4);

   mongoc_client_pool_push (pool, c2); /* queue is [c2, c1] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 3);

   mongoc_client_pool_push (pool, c3); /* queue is [c3, c2] */
   ASSERT_CMPSIZE_T (mongoc_client_pool_num_pushed (pool), ==, (size_t) 2);
   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 2);

   /* BSON_ASSERT oldest client was destroyed, newest were stored */
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   BSON_ASSERT (client == c3);

   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client);
   BSON_ASSERT (client == c2);

   ASSERT_CMPSIZE_T (mongoc_client_pool_get_size (pool), ==, (size_t) 2);

   /* clean up */
   mongoc_client_pool_push (pool, c2);
   mongoc_client_pool_push (pool, c3);
   mongoc_client_pool_destroy (pool);
   mongoc_uri_destroy (uri);
}

static void
test_mongoc_client_pool_set_max_size (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_array_t conns;
   int i;

   _mongoc_array_init (&conns, sizeof client);

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=10");
   pool = mongoc_client_pool_new (uri);

   for (i = 0; i < 5; i++) {
      client = mongoc_client_pool_pop (pool);
      BSON_ASSERT (client);
      _mongoc_array_append_val (&conns, client);
      BSON_ASSERT (mongoc_client_pool_get_size (pool) == i + 1);
   }

   mongoc_client_pool_max_size (pool, 3);

   BSON_ASSERT (mongoc_client_pool_try_pop (pool) == NULL);

   for (i = 0; i < 5; i++) {
      client = _mongoc_array_index (&conns, mongoc_client_t *, i);
      BSON_ASSERT (client);
      mongoc_client_pool_push (pool, client);
   }

   _mongoc_array_clear (&conns);
   _mongoc_array_destroy (&conns);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

static void
test_mongoc_client_pool_set_min_size (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;
   mongoc_array_t conns;
   int i;

   _mongoc_array_init (&conns, sizeof client);

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=10&minpoolsize=3");
   capture_logs (true);
   pool = mongoc_client_pool_new (uri);
   ASSERT_CAPTURED_LOG (
      "minpoolsize URI option", MONGOC_LOG_LEVEL_WARNING, "is deprecated");

   for (i = 0; i < 10; i++) {
      client = mongoc_client_pool_pop (pool);
      BSON_ASSERT (client);
      _mongoc_array_append_val (&conns, client);
      BSON_ASSERT (mongoc_client_pool_get_size (pool) == i + 1);
   }

   capture_logs (true);
   BEGIN_IGNORE_DEPRECATIONS
   mongoc_client_pool_min_size (pool, 7);
   END_IGNORE_DEPRECATIONS
   ASSERT_CAPTURED_LOG ("mongoc_client_pool_min_size",
                        MONGOC_LOG_LEVEL_WARNING,
                        "mongoc_client_pool_min_size is deprecated");

   for (i = 0; i < 10; i++) {
      client = _mongoc_array_index (&conns, mongoc_client_t *, i);
      BSON_ASSERT (client);
      mongoc_client_pool_push (pool, client);
   }

   BSON_ASSERT (mongoc_client_pool_get_size (pool) == 7);

   _mongoc_array_clear (&conns);
   _mongoc_array_destroy (&conns);
   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

#ifndef MONGOC_ENABLE_SSL
static void
test_mongoc_client_pool_ssl_disabled (void)
{
   mongoc_uri_t *uri = mongoc_uri_new ("mongodb://host/?ssl=true");

   ASSERT (uri);
   capture_logs (true);
   ASSERT (NULL == mongoc_client_pool_new (uri));

   mongoc_uri_destroy (uri);
}
#endif

static bool
_assert_node_is_disconnected (uint32_t id, void *item, void *ctx)
{
   mongoc_cluster_node_t *node = (mongoc_cluster_node_t *) item; 

   ASSERT (!(node->stream));

   return true;
}

static void
test_mongoc_client_pool_reset (void)
{
   mock_server_t *server;
   mongoc_client_t *client;
   mongoc_client_pool_t *pool;
   mongoc_database_t *database;
   mongoc_uri_t *uri;
   future_t *future;
   request_t *request;
   int autoresponder_id;

   server = mock_mongos_new (WIRE_VERSION_OP_MSG);
   autoresponder_id = mock_server_auto_ismaster (server, "{ 'isMaster': 1.0 }");
   mock_server_run (server);

   /* After calling reset, check that connections have been closed. Set
      heartbeat frequency high, so a background scan won't interfere. */
   uri = mongoc_uri_copy (mock_server_get_uri (server));
   mongoc_uri_set_option_as_int32 (uri, "heartbeatFrequencyMS", 99999);
   client = mongoc_client_new_from_uri (uri);

   pool = mongoc_client_pool_new (mock_server_get_uri (server));

   client = mongoc_client_pool_pop (pool);
   ASSERT (client->pool_generation == pool->generation);

   database = mongoc_client_get_database (client, "admin");
   future = future_database_command_simple (
      database, tmp_bson ("{'ping': 1}"), NULL, NULL, NULL);

   request = mock_server_receives_command (
      server, "admin", MONGOC_QUERY_SLAVE_OK, "{'ping': 1}");
   mock_server_replies_simple (request, "{'ok': 1}");

   ASSERT (future_get_bool (future));
   mock_server_remove_autoresponder (server, autoresponder_id);

   /* Test that clients outside of the pool do not get reset until they
      are checked back into the pool */
   mongoc_client_pool_reset (pool);
   ASSERT (mongoc_topology_scanner_is_disconnected (pool->topology->scanner));
   ASSERT (client->pool_generation != pool->generation);

   mongoc_client_pool_push (pool, client);
   client = mongoc_client_pool_pop (pool);
   ASSERT (client->pool_generation == pool->generation);
   ASSERT (mongoc_set_size (client->cluster.nodes) == 0);

   /* Test that clients in the pool when it resets also get reset */
   mongoc_client_pool_reset (pool);
   ASSERT (client->pool_generation != pool->generation);

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_reset (pool);
   client = mongoc_client_pool_pop (pool);
   ASSERT (client->pool_generation == pool->generation);

   future_destroy (future);
   request_destroy (request);
   mongoc_uri_destroy (uri);
   mongoc_database_destroy (database);
   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mock_server_destroy (server);
}

static void
test_mongoc_client_pool_handshake (void)
{
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_uri_t *uri;

   uri = mongoc_uri_new ("mongodb://127.0.0.1/?maxpoolsize=1");
   pool = mongoc_client_pool_new (uri);


   ASSERT (mongoc_client_pool_set_appname (pool, "some application"));
   /* Be sure we can't set it twice */
   capture_logs (true);
   ASSERT (!mongoc_client_pool_set_appname (pool, "a"));
   ASSERT_CAPTURED_LOG ("_mongoc_topology_scanner_set_appname",
                        MONGOC_LOG_LEVEL_ERROR,
                        "Cannot set appname more than once");
   capture_logs (false);

   mongoc_client_pool_destroy (pool);

   /* Make sure that after we pop a client we can't set handshake anymore */
   pool = mongoc_client_pool_new (uri);

   client = mongoc_client_pool_pop (pool);

   /* Be sure a client can't set it now that we've popped them */
   capture_logs (true);
   ASSERT (!mongoc_client_set_appname (client, "a"));
   ASSERT_CAPTURED_LOG ("_mongoc_topology_scanner_set_appname",
                        MONGOC_LOG_LEVEL_ERROR,
                        "Cannot call set_appname on a client from a pool");
   capture_logs (false);

   mongoc_client_pool_push (pool, client);

   /* even now that we pushed the client back we shouldn't be able to set
    * the handshake */
   capture_logs (true);
   ASSERT (!mongoc_client_pool_set_appname (pool, "a"));
   ASSERT_CAPTURED_LOG ("_mongoc_topology_scanner_set_appname",
                        MONGOC_LOG_LEVEL_ERROR,
                        "Cannot set appname after handshake initiated");
   capture_logs (false);

   mongoc_uri_destroy (uri);
   mongoc_client_pool_destroy (pool);
}

void
test_client_pool_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/ClientPool/basic", test_mongoc_client_pool_basic);
   TestSuite_Add (
      suite, "/ClientPool/try_pop", test_mongoc_client_pool_try_pop);
   TestSuite_Add (suite,
                  "/ClientPool/min_size_zero",
                  test_mongoc_client_pool_min_size_zero);
   TestSuite_Add (suite,
                  "/ClientPool/min_size_dispose",
                  test_mongoc_client_pool_min_size_dispose);
   TestSuite_Add (
      suite, "/ClientPool/set_max_size", test_mongoc_client_pool_set_max_size);
   TestSuite_Add (
      suite, "/ClientPool/set_min_size", test_mongoc_client_pool_set_min_size);

   TestSuite_Add (suite, "/ClientPool/reset", test_mongoc_client_pool_reset);

   TestSuite_Add (
      suite, "/ClientPool/handshake", test_mongoc_client_pool_handshake);

#ifndef MONGOC_ENABLE_SSL
   TestSuite_Add (
      suite, "/ClientPool/ssl_disabled", test_mongoc_client_pool_ssl_disabled);
#endif
}
