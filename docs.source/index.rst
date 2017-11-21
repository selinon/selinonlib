Welcome to Selinonlib's documentation!
======================================

Selinonlib is a library and tool that is used in `Selinon <https://github.com/selinon/selinon>`_ - a tool for dynamic data control flow on top of Celery distributed task queue. See `Selinon documentation <https://selinon.readthedocs.io/>`_ for more info. The project is hosted on `GitHub <https://github.com/selinon/>`_.

.. note::

  This documentation is for developers. If you want to get familiar with Selinon check `Selinon documentation <https://selinon.readthedocs.io/>`_ first.

Core Selinonlib
###############

.. autosummary::

   selinonlib.builtinPredicate
   selinonlib.config
   selinonlib.cacheConfig
   selinonlib.edge
   selinonlib.failureNode
   selinonlib.failures
   selinonlib.flow
   selinonlib.globalConfig
   selinonlib.helpers
   selinonlib.leafPredicate
   selinonlib.node
   selinonlib.predicate
   selinonlib.selectiveRunFunction
   selinonlib.storage
   selinonlib.strategy
   selinonlib.system
   selinonlib.taskClass
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
   selinonlib.simulator.celeryMocks
   selinonlib.simulator.progress
   selinonlib.simulator.queuePool
   selinonlib.simulator.simulator
   selinonlib.simulator.timeQueue

Storage and database adapters
#############################

.. autosummary::

   selinonlib.storages
   selinonlib.storages.inMemoryStorage
   selinonlib.storages.mongo
   selinonlib.storages.redis
   selinonlib.storages.s3
   selinonlib.storages.sqlStorage

Migrations
##########

.. autosummary::

   selinonlib.migrations
   selinonlib.migrations.migrator
   selinonlib.migrations.taintedFlowStrategy

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
