:mod:`motor` -- Asynchronous Python driver for Tornado and MongoDB
==================================================================

.. automodule:: motor
   :synopsis: Asynchronous Python driver for Tornado and MongoDB

Motor presents a Tornado callback-based API for non-blocking access to
MongoDB.

.. toctree::

   differences
   prerequisites
   examples

Classes
-------

.. toctree::

   motor_connection
   motor_replica_set_connection
   motor_database
   motor_collection
   motor_cursor
   generator_interface

.. TODO: examples of CRUD + commands, etc. etc., following Node driver example,
   particularly emphasize gen.engine, and not creating N connections for N
   inserts
