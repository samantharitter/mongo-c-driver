:man_page: mongoc_client_pool_reset

mongoc_client_pool_reset()
==========================

Synopsis
--------

.. code-block:: c

  void
  mongoc_client_pool_reset (mongoc_client_pool_t *pool);

Cascades calls to :symbol:`mongoc_client_reset` for all clients in the pool, or will reset them the next time they are checked back into the pool. Call this method after forking.

Parameters
----------

* ``pool``: A :symbol:`mongoc_client_pool_t`.

