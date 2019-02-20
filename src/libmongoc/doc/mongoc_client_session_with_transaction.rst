:man_page: mongoc_client_session_with_transaction

mongoc_client_session_with_transaction()
========================================

Synopsis
--------

.. code-block:: c

  bool
  mongoc_client_session_with_transaction (mongoc_client_session_t *session,
		                          mongoc_client_session_with_transaction_cb_t cb,
					  const mongoc_transaction_opt_t *opts,
					  void *ctx,
					  bson_error_t *error);

This method will start a new transaction on ``session``, run ``cb``, and then commit the transaction. If it cannot commit the transaction, the entire sequence may be retried, and ``cb`` may be run multiple times. ``ctx`` will be passed to ``cb`` each time it is called.

This method has an internal time limit of 120 seconds, and will only retry the commit until that time limit is reached. This timeout is not configurable.

``cb`` should not attempt to start new transactions, but should simply run operations meant to be contained within a transaction. The ``cb`` does not need to commit transactions; this is handled by the :symbol:`mongoc_client_session_with_transaction`. If ``cb`` does commit or abort a transaction, however, this method will return without taking further action.

Parameters
----------

* ``session``: A :symbol:`mongoc_client_session_t`.
* ``cb``: A :symbol:`mongoc_client_session_with_transaction_cb_t` callback, which will run inside of a new transaction on the session.
* ``opts``: An optional :symbol:`mongoc_transaction_opt_t`.
* ``ctx``: A ``void*``. This user-provided data will be passed to ``cb``.
* ``error``: An optional location for a :symbol:`bson_error_t` or ``NULL``.

