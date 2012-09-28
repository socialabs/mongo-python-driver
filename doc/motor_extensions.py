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

"""Motor specific extensions to Sphinx."""

import inspect
import re

from sphinx.util.inspect import getargspec, safe_getattr
from sphinx.ext.autodoc import MethodDocumenter, AttributeDocumenter

import motor


class MotorAttribute(object):
    def __init__(self, motor_class, name, delegate_property):
        super(MotorAttribute, self).__init__()
        self.motor_class = motor_class
        self.name = name
        self.delegate_property = delegate_property
        self.pymongo_attr = getattr(motor_class.__delegate_class__, name)

    def is_async_method(self):
        return isinstance(self.delegate_property, motor.Async)

    def requires_callback(self):
        return self.is_async_method() and self.delegate_property.cb_required

    @property
    def __doc__(self):
        doc = self.pymongo_attr.__doc__ or ''
        if not self.is_async_method():
            return doc

        # Find or create parameter list like:
        # :Parameters:
        #  - `x`: A parameter
        #  - `**kwargs`: Additional keyword arguments
        #
        # ... and insert the `callback` parameter's documentation at the end of
        # the list, above **kwargs. If no kwargs, just put `callback` at the end

        indent = ' ' * 10
        if self.requires_callback():
            optional = ''
        else:
            optional = ' (optional)'

        callback_doc = ('- `callback`%s: function taking (result, error), '
            'to execute when operation completes\n\n') % optional

        params_pat = re.compile(r'''
            \n\s*:Parameters:\s*\n # parameters header
            .*?                    # params, one per line
            \n\s*                  # blank line
            (?=(\n\s*\.\.\s)|$)    # end of string, '.. note', '.. seealso', etc
        ''', re.VERBOSE | re.DOTALL)
        match = params_pat.search(doc)
        if match:
            # Find a line like ' - `x`: A parameter'
            # We'll use that line to determine the indentation depth at which
            # to insert ' - `callback`'
            params_doc = doc[match.start():match.end()]
            param_match = re.search('^([ \t]*)-\s*`\S+`.*?:.*$', params_doc, re.M)
            if param_match:
                indent = param_match.groups()[0]

            # See if the final line is '**kwargs':
            lines = params_doc.split('\n')
            for kwargs_lineno, line in enumerate(lines):
                if '**kwargs' in line:
                    params_doc = '\n'.join(lines[:kwargs_lineno])
                    kwargs_lines = '\n' + '\n'.join(lines[kwargs_lineno:])
                    break
            else:
                kwargs_lines = ''

            doc = (
                doc[:match.start()] + params_doc.rstrip()
                + '\n' + indent + callback_doc + kwargs_lines + doc[match.end():]
            )
        else:
            # No existing parameters documentation for this method, make one
            doc += '\n' + ' ' * 8 + ':Parameters:\n' + indent + callback_doc

        return doc

    def getargspec(self):
        args, varargs, kwargs, defaults = getargspec(self.pymongo_attr)

        # This part is copied from Sphinx's autodoc.py
        if args and args[0] in ('cls', 'self'):
            del args[0]

        # Add 'callback=None' argument
        defaults = defaults or []
        prop = self.delegate_property
        if isinstance(prop, motor.Async):
            args.append('callback')
            defaults.append(None)

        return (args, varargs, kwargs, defaults)

    def format_args(self):
        if self.is_async_method():
            return inspect.formatargspec(*self.getargspec())
        else:
            return None


def get_pymongo_attr(obj, name, *defargs):
    """getattr() override for Motor DelegateProperty."""
    if isinstance(obj, motor.MotorMeta):
        for cls in inspect.getmro(obj):
            if name in cls.__dict__:
                attr = cls.__dict__[name]
                if isinstance(attr, motor.DelegateProperty):
                    # 'name' set by MotorMeta
                    assert attr.get_name() == name, (
                        "Expected name %s, got %s" % (name, attr.get_name()))
                    return MotorAttribute(obj, name, attr)
    return safe_getattr(obj, name, *defargs)


class MotorMethodDocumenter(MethodDocumenter):
    objtype = 'motormethod'

    @staticmethod
    def get_attr(obj, name, *defargs):
        return get_pymongo_attr(obj, name, *defargs)

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        # Rely on user to only use '.. automotormethod::' on DelegateProperties
        return True

    def format_args(self):
        if isinstance(self.object, MotorAttribute):
            return self.object.format_args()
        else:
            return super(MotorMethodDocumenter, self).format_args()


class MotorAttributeDocumenter(AttributeDocumenter):
    objtype = 'motorattribute'
    directivetype = 'attribute'

    @staticmethod
    def get_attr(obj, name, *defargs):
        return get_pymongo_attr(obj, name, *defargs)

    def import_object(self):
        # Convince AttributeDocumenter that this is a data descriptor
        ret = super(MotorAttributeDocumenter, self).import_object()
        self._datadescriptor = True
        return ret

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        # Rely on user to only use '.. automotorattribute::' on
        # DelegateProperties
        return True
