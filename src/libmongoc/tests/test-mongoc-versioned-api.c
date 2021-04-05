/*
 * Copyright 2021 MongoDB, Inc.
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
#include "mongoc-client-private.h"
#include "mongoc-server-api-private.h"
#include "test-libmongoc.h"

#include "unified/runner.h"

static void
_test_mongoc_server_api_copy (void)
{
   mongoc_server_api_t *api;
   mongoc_server_api_t *copy;

   BSON_ASSERT (!mongoc_server_api_copy (NULL));

   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);
   mongoc_server_api_strict (api, false);
   copy = mongoc_server_api_copy (api);

   BSON_ASSERT (api->version == copy->version);
   BSON_ASSERT (!copy->strict);
   BSON_ASSERT (copy->strict_set);
   BSON_ASSERT (!copy->deprecation_errors);
   BSON_ASSERT (!copy->deprecation_errors_set);

   mongoc_server_api_destroy (api);
   mongoc_server_api_destroy (copy);
}

static void
_test_mongoc_server_api_setters (void)
{
   mongoc_server_api_t *api = mongoc_server_api_new (MONGOC_SERVER_API_V1);

   BSON_ASSERT (api->version == MONGOC_SERVER_API_V1);
   BSON_ASSERT (!api->strict_set);
   BSON_ASSERT (!api->deprecation_errors_set);
   BSON_ASSERT (!api->strict);
   BSON_ASSERT (!api->deprecation_errors);

   mongoc_server_api_strict (api, true);
   BSON_ASSERT (api->strict_set);
   BSON_ASSERT (api->strict);

   mongoc_server_api_deprecation_errors (api, false);
   BSON_ASSERT (api->deprecation_errors_set);
   BSON_ASSERT (!api->deprecation_errors);

   mongoc_server_api_destroy (api);
}

static void
_test_mongoc_server_api_client (void)
{
   mongoc_client_t *client;
   mongoc_server_api_t *api;
   bson_error_t error;

   client = test_framework_client_new ("mongodb://localhost");
   BSON_ASSERT (!client->api);

   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);

   ASSERT_OR_PRINT (mongoc_client_set_server_api (client, api, &error), error);
   BSON_ASSERT (client->api);
   BSON_ASSERT (client->api->version == MONGOC_SERVER_API_V1);

   /* Cannot change server API once it is set */
   ASSERT (!mongoc_client_set_server_api (client, api, &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_API_ALREADY_SET,
                          "Cannot set server api more than once");

   /* client gets its own internal copy */
   mongoc_server_api_destroy (api);
   BSON_ASSERT (client->api->version == MONGOC_SERVER_API_V1);

   mongoc_client_destroy (client);
}

static void
_test_mongoc_server_api_client_pool (void)
{
   mongoc_uri_t *uri;
   mongoc_client_pool_t *pool;
   mongoc_client_t *client;
   mongoc_server_api_t *api;
   bson_error_t error;

   uri = mongoc_uri_new ("mongodb://localhost");
   pool = test_framework_client_pool_new_from_uri (uri);

   api = mongoc_server_api_new (MONGOC_SERVER_API_V1);

   ASSERT_OR_PRINT (mongoc_client_pool_set_server_api (pool, api, &error),
                    error);

   /* Cannot change server API once it is set */
   ASSERT (!mongoc_client_pool_set_server_api (pool, api, &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_POOL,
                          MONGOC_ERROR_POOL_API_ALREADY_SET,
                          "Cannot set server api more than once");

   /* Clients popped from pool have matching API */
   client = mongoc_client_pool_pop (pool);
   BSON_ASSERT (client->api);
   BSON_ASSERT (client->api->version == MONGOC_SERVER_API_V1);

   ASSERT (!mongoc_client_set_server_api (client, api, &error));
   ASSERT_ERROR_CONTAINS (error,
                          MONGOC_ERROR_CLIENT,
                          MONGOC_ERROR_CLIENT_API_ALREADY_SET,
                          "Cannot set server api more than once");

   mongoc_client_pool_push (pool, client);
   mongoc_client_pool_destroy (pool);
   mongoc_server_api_destroy (api);
   mongoc_uri_destroy (uri);
}

void
test_client_versioned_api_install (TestSuite *suite)
{
   run_unified_tests (suite, JSON_DIR "/versioned_api");

   TestSuite_Add (
      suite, "/VersionedApi/client", _test_mongoc_server_api_client);
   TestSuite_Add (
      suite, "/VersionedApi/client_pool", _test_mongoc_server_api_client_pool);
   TestSuite_Add (suite, "/VersionedApi/copy", _test_mongoc_server_api_copy);
   TestSuite_Add (
      suite, "/VersionedApi/setters", _test_mongoc_server_api_setters);
}