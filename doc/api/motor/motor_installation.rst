Installing Motor
================

Motor is a module included within PyMongo. While it is in beta, it is in
A. Jesse Jiryu Davis's fork of PyMongo, on the ``motor`` branch:

https://github.com/ajdavis/mongo-python-driver/tree/motor/

.. seealso:: :doc:`Prerequisites <prerequisites>`

Installing with pip
-------------------

First install `pip <http://pypi.python.org/pypi/pip>`_, then install Motor's
prerequisites::

  $ pip install tornado greenlet

Uninstall the official PyMongo::

  $ pip uninstall pymongo

Install the version of PyMongo with Motor::

  $ pip install git+https://github.com/ajdavis/mongo-python-driver.git@motor

To follow Motor's development, upgrade to the latest version from git::

  $ pip install -U git+https://github.com/ajdavis/mongo-python-driver.git@motor

Installation issues
-------------------

See the instructions for :ref:`pymongo-installation` PyMongo
in case of issues. Note that Motor does not fully support Windows, since
Tornado does not.

