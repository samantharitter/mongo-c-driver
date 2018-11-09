#include <mongoc/mongoc.h>
#include <mongoc/mongoc-queue-private.h>

#include "TestSuite.h"


static void
test_mongoc_queue_basic (void)
{
   mongoc_queue_t q = MONGOC_QUEUE_INITIALIZER;

   _mongoc_queue_push_head (&q, (void *) 1);
   _mongoc_queue_push_tail (&q, (void *) 2);
   _mongoc_queue_push_head (&q, (void *) 3);
   _mongoc_queue_push_tail (&q, (void *) 4);
   _mongoc_queue_push_head (&q, (void *) 5);

   ASSERT_CMPINT (_mongoc_queue_get_length (&q), ==, 5);

   ASSERT (_mongoc_queue_pop_head (&q) == (void *) 5);
   ASSERT (_mongoc_queue_pop_head (&q) == (void *) 3);
   ASSERT (_mongoc_queue_pop_head (&q) == (void *) 1);
   ASSERT (_mongoc_queue_pop_head (&q) == (void *) 2);
   ASSERT (_mongoc_queue_pop_head (&q) == (void *) 4);
   ASSERT (!_mongoc_queue_pop_head (&q));
}


static void
test_mongoc_queue_pop_tail (void)
{
   mongoc_queue_t q = MONGOC_QUEUE_INITIALIZER;

   _mongoc_queue_push_head (&q, (void *) 1);
   ASSERT_CMPUINT32 (_mongoc_queue_get_length (&q), ==, (uint32_t) 1);
   ASSERT_CMPVOID (_mongoc_queue_pop_tail (&q), ==, (void *) 1);

   ASSERT_CMPUINT32 (_mongoc_queue_get_length (&q), ==, (uint32_t) 0);
   ASSERT_CMPVOID (_mongoc_queue_pop_tail (&q), ==, (void *) NULL);

   _mongoc_queue_push_tail (&q, (void *) 2);
   _mongoc_queue_push_head (&q, (void *) 3);
   ASSERT_CMPUINT32 (_mongoc_queue_get_length (&q), ==, (uint32_t) 2);
   ASSERT_CMPVOID (_mongoc_queue_pop_tail (&q), ==, (void *) 2);
   ASSERT_CMPUINT32 (_mongoc_queue_get_length (&q), ==, (uint32_t) 1);
   ASSERT_CMPVOID (_mongoc_queue_pop_tail (&q), ==, (void *) 3);
   ASSERT_CMPUINT32 (_mongoc_queue_get_length (&q), ==, (uint32_t) 0);
   ASSERT_CMPVOID (_mongoc_queue_pop_tail (&q), ==, (void *) NULL);
}

static bool
_queue_for_each_always_true (void *item, void *ctx)
{
   int *number = (int *) item;
   *number = 1;
   return true;
}

static bool
_queue_for_each_modify_three_entries (void *item, void *ctx)
{
   int *number = (int *) item;
   int *counter = (int *) ctx;

   if (*counter > 2) {
      return false;
   }

   (*counter)++;
   *number = 1;

   return true;
}

static void
test_mongoc_queue_for_each (void)
{
   int i;
   int counter;
   int length = 5;
   int numbers[5] = {0};
   mongoc_queue_t q = MONGOC_QUEUE_INITIALIZER;

   /* Sanity check for empty queues */
   _mongoc_queue_for_each (&q, _queue_for_each_always_true, NULL);

   /* Test that method modifies all entries when cb returns true */
   for (i = 0; i < length; i++) {
      _mongoc_queue_push_head (&q, &(numbers[i]));
   }

   _mongoc_queue_for_each (&q, _queue_for_each_always_true, NULL);
   for (i = 0; i < length; i++) {
      ASSERT (numbers[i] == 1);
      numbers[i] = 0;
   }

   /* Test that method stops modifying when cb returns false */
   counter = 0;
   _mongoc_queue_for_each (&q, _queue_for_each_modify_three_entries, &counter);
   counter = 0;
   for (i = 0; i < length; i++) {
      counter += numbers[i];
   }

   ASSERT (counter == 3);
}

void
test_queue_install (TestSuite *suite)
{
   TestSuite_Add (suite, "/Queue/basic", test_mongoc_queue_basic);
   TestSuite_Add (suite, "/Queue/pop_tail", test_mongoc_queue_pop_tail);
   TestSuite_Add (suite, "/Queue/for_each", test_mongoc_queue_for_each);
}
