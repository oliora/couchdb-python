# -*- coding: utf-8 -*-
#
# Copyright (C) 2008-2009 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

"""Utility code for managing design documents."""

from copy import deepcopy
from inspect import getsource
from itertools import groupby
from operator import attrgetter
from textwrap import dedent
from types import FunctionType

__all__ = ['sync_definitions', 'ViewDefinition']
__docformat__ = 'restructuredtext en'


class DefinitionMixin(object):
    r"""Helper class to implement definition types. Provides implementation
    of some common methods of all definitions.
    """
    def __init__(self, design):
        if design.startswith('_design/'):
            design = design[8:]
        self.design = design

    def get_doc(self, db):
        """Retrieve and return the design document corresponding to this
        definition from the given database.

        :param db: the `Database` instance
        :return: a `client.Document` instance, or `None` if the design document
                 does not exist in the database
        :rtype: `Document`
        """
        return db.get('_design/%s' % self.design)

    def sync(self, db):
        """Ensure that the definition in the database matches
        this definition instance.

        :param db: the `Database` instance
        """
        return sync_definitions(db, [self])


class ViewDefinition(DefinitionMixin):
    r"""Definition of a view stored in a specific design document.
    
    An instance of this class can be used to access the results of the view,
    as well as to keep the view definition in the design document up to date
    with the definition in the application code.
    
    >>> from couchdb import Server
    >>> server = Server()
    >>> db = server.create('python-tests')
    
    >>> view = ViewDefinition('tests', 'all', '''function(doc) {
    ...     emit(doc._id, null);
    ... }''')
    >>> view.get_doc(db)

    The view is not yet stored in the database, in fact, design doc doesn't
    even exist yet. That can be fixed using the `sync` method:

    >>> view.sync(db)                                       #doctest: +ELLIPSIS
    [(True, '_design/tests', ...)]
    >>> design_doc = view.get_doc(db)
    >>> design_doc                                          #doctest: +ELLIPSIS
    <Document '_design/tests'@'...' {...}>
    >>> print design_doc['views']['all']['map']
    function(doc) {
        emit(doc._id, null);
    }

    If you use a Python view server, you can also use Python functions instead
    of code embedded in strings:
    
    >>> def my_map(doc):
    ...     yield doc['somekey'], doc['somevalue']
    >>> view = ViewDefinition('test2', 'somename', my_map, language='python')
    >>> view.sync(db)                                       #doctest: +ELLIPSIS
    [(True, '_design/test2', ...)]
    >>> design_doc = view.get_doc(db)
    >>> design_doc                                          #doctest: +ELLIPSIS
    <Document '_design/test2'@'...' {...}>
    >>> print design_doc['views']['somename']['map']
    def my_map(doc):
        yield doc['somekey'], doc['somevalue']
    
    Use the static `sync_many()` method to create or update a collection of
    views in the database in an atomic and efficient manner, even across
    different design documents.

    >>> del server['python-tests']
    """

    def __init__(self, design, name, map_fun, reduce_fun=None,
                 language='javascript', wrapper=None, options=None,
                 **defaults):
        """Initialize the view definition.
        
        Note that the code in `map_fun` and `reduce_fun` is automatically
        dedented, that is, any common leading whitespace is removed from each
        line.
        
        :param design: the name of the design document
        :param name: the name of the view
        :param map_fun: the map function code
        :param reduce_fun: the reduce function code (optional)
        :param language: the name of the language used
        :param wrapper: an optional callable that should be used to wrap the
                        result rows
        :param options: view specific options (e.g. {'collation':'raw'})
        """
        super(ViewDefinition, self).__init__(design)
        self.name = name
        if isinstance(map_fun, FunctionType):
            map_fun = _strip_decorators(getsource(map_fun).rstrip())
        self.map_fun = dedent(map_fun.lstrip('\n'))
        if isinstance(reduce_fun, FunctionType):
            reduce_fun = _strip_decorators(getsource(reduce_fun).rstrip())
        if reduce_fun:
            reduce_fun = dedent(reduce_fun.lstrip('\n'))
        self.reduce_fun = reduce_fun
        self.language = language
        self.wrapper = wrapper
        self.options = options
        self.defaults = defaults

    def __call__(self, db, **options):
        """Execute the view in the given database.
        
        :param db: the `Database` instance
        :param options: optional query string parameters
        :return: the view results
        :rtype: `ViewResults`
        """
        wrapper = options.pop('wrapper', self.wrapper)
        merged_options = self.defaults.copy()
        merged_options.update(options)
        return db.view('/'.join([self.design, self.name]),
                       wrapper=wrapper, **merged_options)

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, '/'.join([
            '_design', self.design, '_view', self.name
        ]))

    @staticmethod
    def _sync_doc(doc, views, remove_missing, languages):
        def get_definition(view):
            retval = {'map': view.map_fun}
            if view.reduce_fun:
                retval['reduce'] = view.reduce_fun
            if view.options:
                retval['options'] = view.options
            return retval
        _sync_dict_field(doc, 'views', views, get_definition, remove_missing, languages)

    @staticmethod
    def sync_many(db, views, remove_missing=False, callback=None):
        """Ensure that the views stored in the database that correspond to a
        given list of `ViewDefinition` instances match the code defined in
        those instances.
        
        This function might update more than one design document. This is done
        using the CouchDB bulk update feature to ensure atomicity of the
        operation.
        
        :param db: the `Database` instance
        :param views: a sequence of `ViewDefinition` instances
        :param remove_missing: whether views found in a design document that
                               are not found in the list of `ViewDefinition`
                               instances should be removed
        :param callback: a callback function that is invoked when a design
                         document gets updated; the callback gets passed the
                         design document as only parameter, before that doc
                         has actually been saved back to the database
        """
        return sync_definitions(db, views, remove_missing, callback)


class UpdateHandlerDefinition(DefinitionMixin):
    r"""Definition of an update handler stored in a specific design document.

    An instance of this class can be used to call an update handler in database,
    as well as to keep the update handler definition in the design document
    up to date with the definition in the application code.

    >>> from couchdb import Server
    >>> server = Server()
    >>> db = server.create('python-tests')

    >>> update_handler = UpdateHandlerDefinition('tests', 'activate', '''function(doc, req) {
    ...     doc.active = true;
    ...     return [doc, "OK"]
    ... }''')
    >>> update_handler.get_doc(db)

    The update handler is not yet stored in the database, in fact, design doc doesn't
    even exist yet. That can be fixed using the `sync` method:

    >>> update_handler.sync(db)                             #doctest: +ELLIPSIS
    [(True, '_design/tests', ...)]
    >>> design_doc = update_handler.get_doc(db)
    >>> design_doc                                          #doctest: +ELLIPSIS
    <Document '_design/tests'@'...' {...}>
    >>> print design_doc['updates']['activate']
    function(doc, req) {
        doc.active = true;
        return [doc, "OK"]
    }

    >>> del server['python-tests']
    """

    def __init__(self, design, name, func, language='javascript',
                 **defaults):
        """Initialize the update handler definition.

        Note that the code in `func` is automatically dedented, that is,
        any common leading whitespace is removed from each line.

        :param design: the name of the design document
        :param name: the name of the update handler
        :param func: the update handler function code
        :param language: the name of the language used
        """
        super(UpdateHandlerDefinition, self).__init__(design)
        self.name = name
        if isinstance(func, FunctionType):
            func = _strip_decorators(getsource(func).rstrip())
        self.func = dedent(func.lstrip('\n'))
        self.language = language
        self.defaults = defaults

    def __call__(self, db, docid=None, **options):
        """Execute the update handler in the given database.

        :param db: the `Database` instance
        :param docid: optional ID of a document to pass to the update handler
        :param options: optional query string parameters
        :return: (headers, body) tuple, where headers is a dict of headers
                 returned from the list function and body is a readable
                 file-like instance
        """
        merged_options = self.defaults.copy()
        merged_options.update(options)
        return db.update_doc('/'.join([self.design, self.name]), docid, **merged_options)

    def __repr__(self):
        return '<%s %r>' % (type(self).__name__, '/'.join([
            '_design', self.design, '_update', self.name
        ]))

    @staticmethod
    def _sync_doc(doc, update_handlers, remove_missing, languages):
        _sync_dict_field(doc, 'updates', update_handlers, attrgetter('func'),
                         remove_missing, languages)


def sync_definitions(db, definitions, remove_missing=False, callback=None):
    """Ensure that the definitions stored in the database that correspond to a
    given list of "definition" instances match the code defined in those instances.

    This function might update more than one design document. This is done
    using the CouchDB bulk update feature to ensure atomicity of the
    operation.

    :param db: the `Database` instance
    :param definitions: a sequence of "definition" instances
    :param remove_missing: whether definitions found in a design document that
                           are not present in given `definitions`
                           should be removed
    :param callback: a callback function that is invoked when a design
                     document gets updated; the callback gets passed the
                     design document as only parameter, before that doc
                     has actually been saved back to the database
    """
    definitions = sorted(definitions, key=attrgetter('design'))

    design_docs = (_DesignDocument.from_definitions(design, defs)
        for design, defs in groupby(definitions, key=attrgetter('design')))

    return _DesignDocument.sync_many(db, design_docs, remove_missing, callback)


class _DesignDocument(object):
    __definition_types = (
        (ViewDefinition, 'views'),
        (UpdateHandlerDefinition, 'update_handlers'),
    )

    @classmethod
    def from_definitions(cls, design, definitions):
        typed_definitions = [(def_type, name, []) for def_type, name in _DesignDocument.__definition_types]

        for definition in definitions:
            unknown_type = True
            for def_type in typed_definitions:
                if isinstance(definition, def_type[0]):
                    def_type[2].append(definition)
                    unknown_type = False
            if unknown_type:
                raise TypeError("Invalid definition type '%s'" % definition.__class__.__name__)

        typed_definitions = dict((name, defs) for _, name, defs in typed_definitions)
        return cls(design, **typed_definitions)

    def __init__(self, design, **kwargs):
        if design.startswith('_design/'):
            design = design[8:]
        self.design = design
        for _, name in _DesignDocument.__definition_types:
            setattr(self, name, kwargs.pop(name, []))
        if kwargs:
            raise TypeError("Invalid keyword argument '%s'" % next(kwargs.iterkeys()))

    def _sync_doc(self, doc, remove_missing):
            languages = set()

            for def_type, name in _DesignDocument.__definition_types:
                def_type._sync_doc(doc, getattr(self, name), remove_missing, languages)

            if len(languages) > 1:
                raise ValueError('Found different language definitions in one '
                                 'design document (%r)', list(languages))
            doc['language'] = list(languages)[0]

    @staticmethod
    def sync_many(db, design_docs, remove_missing=False, callback=None):
        docs = []

        for design_doc in design_docs:
            doc_id = '_design/%s' % design_doc.design
            doc = db.get(doc_id, {'_id': doc_id})
            orig_doc = deepcopy(doc)

            design_doc._sync_doc(doc, remove_missing)

            if doc != orig_doc:
                if callback is not None:
                    callback(doc)
                docs.append(doc)

        return db.update(docs)


def _strip_decorators(code):
    retval = []
    beginning = True
    for line in code.splitlines():
        if beginning and not line.isspace():
            if line.lstrip().startswith('@'):
                continue
            beginning = False
        retval.append(line)
    return '\n'.join(retval)


def _sync_dict_field(doc, field, definitions, definition_getter, remove_missing, languages):
    missing = list(doc.get(field, {}).keys())
    for definition in definitions:
        doc.setdefault(field, {})[definition.name] = definition_getter(definition)
        languages.add(definition.language)
        if definition.name in missing:
            missing.remove(definition.name)

    if remove_missing and missing:
        for name in missing:
            del doc[field][name]
    elif missing and 'language' in doc:
        languages.add(doc['language'])