#include <mongoc/mongoc.h>

#include "TestSuite.h"
#include "test-conveniences.h"
#include "test-libmongoc.h"


static void
test_client_buildinfo_hang (void)
{
    const char *uri_string = "mongodb://localhost:27017/?ssl=true&sslAllowInvalidCertificates=true&sslCertificateAuthorityFile=ca.pem";
    mongoc_uri_t *uri;
    mongoc_client_pool_t *pool;
    mongoc_client_t *client;
    mongoc_database_t *database;
    mongoc_collection_t *collection;
    mongoc_cursor_t *indexes_cursor;
    bson_t reply, command, indexes, index_keys, opts, weights;
    bson_error_t error;

    mongoc_init();

    uri = mongoc_uri_new_with_error(uri_string, &error);
    if (!uri) {
	ASSERT (false);
    }

    pool = mongoc_client_pool_new(uri);
    ASSERT (pool);

    client = mongoc_client_pool_pop(pool);
    
    // example 1
     database = mongoc_client_get_database(client, "admin");
     bson_init(&command);
     bson_append_int32(&command, "buildInfo", -1, 1);

     if (!mongoc_database_command_simple(database, &command, NULL, &reply, &error)) {
         fprintf(stderr, "failed to run command: %s\n", error.message);
         ASSERT (false);
     }

     fprintf (stderr, "finished buildInfo repro without hanging!\n");
}

void
test_hang_install (TestSuite *suite)
{
   TestSuite_AddLive (suite,
		      "/Client/hang",
		      test_client_buildinfo_hang);
}
