:class:`MotorDatabase`
======================

.. currentmodule:: motor

.. autoclass:: motor.MotorDatabase

  .. describe:: db[collection_name] || db.collection_name

     Get the `collection_name` :class:`MotorCollection` of
     :class:`MotorDatabase` `db`.

     Raises :class:`~pymongo.errors.InvalidName` if an invalid collection name is used.

  .. automotormethod:: create_collection
  .. automotormethod:: drop_collection
  .. automotormethod:: validate_collection
  .. automethod:: add_son_manipulator
  .. automotorattribute:: slave_okay
  .. automotorattribute:: read_preference
  .. automotorattribute:: safe
  .. automotormethod:: get_lasterror_options
  .. automotormethod:: set_lasterror_options
  .. automotormethod:: unset_lasterror_options
