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

#include "mongoc-config.h"

#ifdef MONGOC_ENABLE_SSL

#include "mongoc-rand.h"
#include "mongoc-rand-private.h"

#include "mongoc.h"

/*
 *-------------------------------------------------------------------------
 *
 * _mongoc_rand_bytes --
 *
 *       Fills @buf with @num cruptographically-secure random bytes.
 *
 * Returns:
 *       1 on success, 0 on failure, with error in errno system variable.
 *
 *-------------------------------------------------------------------------
 */

int _mongoc_rand_bytes(uint8_t * buf, int num) {
   return _mongoc_rand_bytes_impl(buf, num);
}

/*
 *-------------------------------------------------------------------------
 *
 * _mongoc_pseudo_rand_bytes --
 *
 *       Fills @buf with @num secure pseudo-random bytes.
 *
 * Returns:
 *       1 on success, 0 on failure, with error in errno system variable.
 *
 *-------------------------------------------------------------------------
 */

int _mongoc_pseudo_rand_bytes(uint8_t * buf, int num) {
   return _mongoc_pseudo_rand_bytes_impl(buf, num);
}

void mongoc_rand_seed(const void* buf, int num) {
   return mongoc_rand_seed_impl(buf, num);
}

void mongoc_rand_add(const void* buf, int num, double entropy) {
   return mongoc_rand_add_impl(buf, num, entropy);
}

int mongoc_rand_status(void) {
   return mongoc_rand_status_impl();
}

#endif /* MONGOC_ENABLE_SSL */
