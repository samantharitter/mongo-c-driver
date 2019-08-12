/*
 * Copyright 2019-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <mongoc/mongoc.h>
#include "mongoc/mongoc-uri-private.h"
#include "mongoc/mongoc-host-list-private.h"

#include "TestSuite.h"
#include "test-libmongoc.h"
#include "test-conveniences.h"

static void
add_multiple_mongoses (mongoc_uri_t *uri)
{
   bson_error_t error;

   /* TODO CDRIVER-3285, fix this to be dynamic */
   ASSERT_OR_PRINT (mongoc_uri_upsert_host_and_port (uri, "localhost:27017", &error),
		    error);
   ASSERT_OR_PRINT (mongoc_uri_upsert_host_and_port (uri, "localhost:27018", &error),
		    error);
}

static void
test_new_transaction_unpins (void *ctx)
{
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_error_t error;
   mongoc_client_session_t *session;
   mongoc_host_list_t *servers;
   mongoc_cursor_t *cursor;
   char *host;
   int i;

   uri = test_framework_get_uri ();
   add_multiple_mongoses (uri);

   /* Increase localThresholdMS and wait until both nodes are discovered
      to avoid false positives. */
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_LOCALTHRESHOLDMS, 1000);
   client = mongoc_client_new_from_uri (uri);

   /* Create a collection. */
   coll = mongoc_client_get_collection (client, "test", "test");
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{}"), NULL, NULL, &error), error);

   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session != NULL, error);

   /* Under one transaction, insert a document. */
   ASSERT_OR_PRINT (mongoc_client_session_start_transaction (session, NULL, &error),
		    error);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{}"), NULL, NULL, &error), error);
   ASSERT_OR_PRINT (mongoc_client_session_commit_transaction (session, NULL, &error),
		    error);

   /* Then, 50 times, start new transactions. Each time we start a new
      transaction, the session should be un-pinned, so by statistics,
      we should balance the new transactions across both mongos. */
   for (i = 0; i < 50; i++) {
      mongoc_host_list_t cursor_host;

      ASSERT_OR_PRINT (mongoc_client_session_start_transaction (session, NULL, &error),
		       error);

      cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), NULL, NULL);
      ASSERT (mongoc_cursor_next (cursor, NULL));
      mongoc_cursor_get_host (cursor, &cursor_host);
      _mongoc_host_list_upsert (servers, &cursor_host);
      
      ASSERT_OR_PRINT (mongoc_client_session_commit_transaction (session, NULL, &error),
		       error);

      mongoc_cursor_destroy (cursor);
   }

   ASSERT (_mongoc_host_list_length (servers) == 2);

   _mongoc_host_list_destroy_all (servers);
   mongoc_uri_destroy (uri);
   mongoc_client_session_destroy (session);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

static void
test_non_transaction_unpins (void *ctx)
{
   mongoc_uri_t *uri;
   mongoc_client_t *client;
   mongoc_collection_t *coll;
   bson_error_t error;
   mongoc_client_session_t *session;
   mongoc_host_list_t *servers;
   mongoc_cursor_t *cursor;
   char *host;
   int i;

   uri = test_framework_get_uri ();
   add_multiple_mongoses (uri);

   /* Increase localThresholdMS and wait until both nodes are discovered
      to avoid false positives. */
   mongoc_uri_set_option_as_int32 (uri, MONGOC_URI_LOCALTHRESHOLDMS, 1000);
   client = mongoc_client_new_from_uri (uri);

   /* Create a collection. */
   coll = mongoc_client_get_collection (client, "test", "test");
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{}"), NULL, NULL, &error), error);

   session = mongoc_client_start_session (client, NULL, &error);
   ASSERT_OR_PRINT (session != NULL, error);

   /* Under one transaction, insert a document. */
   ASSERT_OR_PRINT (mongoc_client_session_start_transaction (session, NULL, &error),
		    error);
   ASSERT_OR_PRINT (mongoc_collection_insert_one (coll, tmp_bson ("{}"), NULL, NULL, &error), error);
   ASSERT_OR_PRINT (mongoc_client_session_commit_transaction (session, NULL, &error),
		    error);

   /* After our initial transaction, the session should become un-pinned
      if we run further operations on the session. By statistics,
      new operations should balance across both mongos. */
   for (i = 0; i < 50; i++) {
      mongoc_host_list_t cursor_host;

      cursor = mongoc_collection_find_with_opts (coll, tmp_bson ("{}"), NULL, NULL);
      ASSERT (mongoc_cursor_next (cursor, NULL));
      mongoc_cursor_get_host (cursor, &cursor_host);
      _mongoc_host_list_upsert (servers, &cursor_host);
      
      mongoc_cursor_destroy (cursor);
   }

   ASSERT (_mongoc_host_list_length (servers) == 2);

   _mongoc_host_list_destroy_all (servers);
   mongoc_uri_destroy (uri);
   mongoc_client_session_destroy (session);
   mongoc_collection_destroy (coll);
   mongoc_client_destroy (client);
}

void
test_mongos_pinning_install (TestSuite *suite)
{
   TestSuite_AddFull (suite,
		      "/mongos_pinning/new_transaction_unpins",
		      test_new_transaction_unpins,
		      NULL,
		      NULL,
		      test_framework_skip_if_no_sessions,
		      test_framework_skip_if_no_crypto,
		      test_framework_skip_if_max_wire_version_less_than_8,
		      test_framework_skip_if_not_mongos);

   TestSuite_AddFull (suite,
		      "/mongos_pinning/non_transaction_unpins",
		      test_non_transaction_unpins,
		      NULL,
		      NULL,
		      test_framework_skip_if_no_sessions,
		      test_framework_skip_if_no_crypto,
		      test_framework_skip_if_max_wire_version_less_than_8,
		      test_framework_skip_if_not_mongos);
}
