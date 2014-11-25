/*
 * Copyright 2014 MongoDB, Inc.
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

#include "mongoc-topology-description.h"
#include "mongoc-trace.h"

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_init --
 *
 *       Initialize the given topology description
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_topology_description_init (mongoc_topology_description_t *description)
{
   ENTRY;

   bson_return_if_fail(description);

   description->type = MONGOC_CLUSTER_TYPE_UNKNOWN;
   description->servers = NULL;
   description->set_name = NULL;
   description->compatible = true; // TODO: different default?
   description->compatibility_error = NULL;

   // TODO: handle cluster callback stuff
   // TODO: version?

   EXIT;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_destroy --
 *
 *       Destroy allocated resources within @description
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_topology_description_destroy (mongoc_topology_description_t *description)
{
   ENTRY;

   BSON_ASSERT(description);

   // TODO: what to do here, remove server descriptions?  Who owns these actually?

   EXIT;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_remove_server --
 *
 *       If present, remove this server from this topology description.
 *
 * Returns:
 *       None.
 *
 * Side effects:
 *       Removes server from topology's list of servers.
 *
 *--------------------------------------------------------------------------
 */
void
_mongoc_topology_description_remove_server (mongoc_topology_description_t *description,
                                            mongoc_server_description_t   *server)
{
   mongoc_server_description_t *server_iter = description->servers;
   mongoc_server_description_t *previous_server = NULL;

   while (server_iter) {
      if (server_iter->connection_address == server->connection_address) {
         if (previous_server) {
            previous_server->next = server_iter->next;
         } else {
            description->servers = server_iter->next;
         }
         break;
      }
      previous_server = server_iter;
      server_iter = server_iter->next;
   }
   // TODO: some sort of callback to clusters?
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_sdam_topology_has_server --
 *
 *       Return true if the given server is in the given topology,
 *       false otherwise.
 *
 * Returns:
 *       true, false
 *
 * Side effects:
 *       None.
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_topology_description_has_server (mongoc_topology_description_t *description,
                                         const char                    *address)
{
   mongoc_server_description_t *server_iter = description->servers;

   while (server_iter) {
      if (address == server_iter->connection_address) {
         return true;
      }
      server_iter = server_iter->next;
   }
   return false;
}

/*
 *--------------------------------------------------------------------------
 *
 * _mongoc_topology_description_has_primary --
 *
 *       If topology has a primary, return true, else return false.
 *
 * Returns:
 *       true, false
 *
 * Side effects:
 *       None
 *
 *--------------------------------------------------------------------------
 */
bool
_mongoc_topology_description_has_primary (mongoc_topology_description_t *description)
{
   mongoc_server_description_t *server_iter = description->servers;

   while (server_iter) {
      if (server_iter->type == MONGOC_SERVER_TYPE_RS_PRIMARY) {
         return true;
      }
      server_iter = server_iter->next;
   }
   return false;
}
