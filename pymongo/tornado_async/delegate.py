# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Utilities for wrapping one Python class in another, used by Motor to wrap
PyMongo and make it asynchronous.
"""


class DelegateMeta(type):
    def __new__(cls, name, bases, attrs):
        # Create the class.
        new_class = type.__new__(cls, name, bases, attrs)

        delegated_attrs = {}

        # Add all attributes to the class.
        for name, attr in attrs.items():
            if isinstance(attr, DelegateProperty):
                setattr(new_class, name, attr.make_attr(new_class, name))
                delegated_attrs[name] = attr

        # Information for users of the Delegator class or instance
        new_class.delegated_attrs = delegated_attrs

        return new_class

# TODO: use descriptor?
class DelegateProperty(object):
    def __init__(self, name=None, doc=None, readonly=False):
        self.name = name
        self.doc = doc
        self.readonly = readonly

    def make_attr(self, cls, name):
        name = self.name or name
        def getter(self):
            return getattr(self.delegate, name)

        delegate_attr = property(fget=getter, doc=self.doc)

        if not self.readonly:
            def setter(self, value):
                setattr(self.delegate, name, value)

            delegate_attr = delegate_attr.setter(setter)

        return delegate_attr


class Delegator(object):
    __metaclass__ = DelegateMeta

    # TODO: doc
    def __init__(self, *args, **kwargs):
        self.delegate = kwargs.get('delegate')
