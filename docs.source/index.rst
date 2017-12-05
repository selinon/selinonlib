Welcome to Selinonlib's documentation!
======================================

Selinonlib is a library and tool that is used in `Selinon <https://github.com/selinon/selinon>`_ - a tool for dynamic data control flow on top of Celery distributed task queue. See `Selinon documentation <https://selinon.readthedocs.io/>`_ for more info. The project is hosted on `GitHub <https://github.com/selinon/>`_.

.. note::

  This documentation is for developers. If you want to get familiar with Selinon check `Selinon documentation <https://selinon.readthedocs.io/>`_ first.

Core Selinonlib
###############

.. autosummary::

   selinonlib.builtin_predicate
   selinonlib.config
   selinonlib.cache_config
   selinonlib.edge
   selinonlib.failure_node
   selinonlib.failures
   selinonlib.flow
   selinonlib.global_config
   selinonlib.helpers
   selinonlib.leaf_predicate
   selinonlib.node
   selinonlib.predicate
   selinonlib.selective_run_function
   selinonlib.storage
   selinonlib.strategy
   selinonlib.system
   selinonlib.task_class
   selinonlib.task

Task result and task state caches
#################################

.. autosummary::

   selinonlib.caches
   selinonlib.caches.fifo
   selinonlib.caches.lifo
   selinonlib.caches.lru
   selinonlib.caches.mru
   selinonlib.caches.rr

Built-in predicates
###################

.. autosummary::

   selinonlib.predicates

Run-time support routines
#########################

.. autosummary::

   selinonlib.routines

CLI simulator implementation
############################

.. autosummary::

   selinonlib.simulator
   selinonlib.simulator.celery_mocks
   selinonlib.simulator.progress
   selinonlib.simulator.queue_pool
   selinonlib.simulator.simulator
   selinonlib.simulator.time_queue

Storage and database adapters
#############################

.. autosummary::

   selinonlib.storages
   selinonlib.storages.in_memory_storage
   selinonlib.storages.mongo
   selinonlib.storages.redis
   selinonlib.storages.s3
   selinonlib.storages.sql_storage

Migrations
##########

.. autosummary::

   selinonlib.migrations
   selinonlib.migrations.migrator
   selinonlib.migrations.tainted_flow_strategy

Predefined scheduling strategies
################################

.. autosummary::

   selinonlib.strategies

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. |date| date::
.. |time| date:: %H:%M

Documentation was automatically generated on |date| at |time|.
