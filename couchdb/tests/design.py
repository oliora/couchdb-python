# -*- coding: utf-8 -*-
#
# Copyright (C) 2008 Christopher Lenz
# All rights reserved.
#
# This software is licensed as described in the file COPYING, which
# you should have received as part of this distribution.

import doctest
import unittest

from couchdb import design
from couchdb.tests import testutil


class DesignTestCase(testutil.TempDatabaseMixin, unittest.TestCase):

    def test_options(self):
        options = {'collation': 'raw'}
        view = design.ViewDefinition(
            'foo', 'foo',
            'function(doc) {emit(doc._id, doc._rev)}',
            options=options)
        _, db = self.temp_db()
        view.sync(db)
        design_doc = db.get('_design/foo')
        self.assertTrue(design_doc['views']['foo']['options'] == options)

    def test_retrieve_view_defn(self):
        '''see issue 183'''
        view_def = design.ViewDefinition('foo', 'bar', 'baz')
        result = view_def.sync(self.db)
        self.assertTrue(isinstance(result, list))
        self.assertEqual(result[0][0], True)
        self.assertEqual(result[0][1], '_design/foo')
        doc = self.db[result[0][1]]
        self.assertEqual(result[0][2], doc['_rev'])

    def test_retrieve_update_handler_defn(self):
        updater_def = design.UpdateHandlerDefinition('foo', 'bar', 'baz')
        result = updater_def.sync(self.db)
        self.assertTrue(isinstance(result, list))
        self.assertEqual(result[0][0], True)
        self.assertEqual(result[0][1], '_design/foo')
        doc = self.db[result[0][1]]
        self.assertEqual(result[0][2], doc['_rev'])

    def test_retrieve_validator_defn(self):
        validator_def = design.ValidateFunctionDefinition('foo', 'bar')
        result = validator_def.sync(self.db)
        self.assertTrue(isinstance(result, list))
        self.assertEqual(result[0][0], True)
        self.assertEqual(result[0][1], '_design/foo')
        doc = self.db[result[0][1]]
        self.assertEqual(result[0][2], doc['_rev'])

    def test_sync_two_validate_funcs_per_doc(self):
        first_validator = design.ValidateFunctionDefinition('foo', 'bar')
        second_validator = design.ValidateFunctionDefinition('foo', 'bar2')
        _, db = self.temp_db()
        self.assertRaises(ValueError, design.sync_definitions, db, (first_validator, second_validator))

    def test_retrieve_show_func_defn(self):
        show_func_def = design.ShowFunctionDefinition('foo', 'bar', 'baz')
        result = show_func_def.sync(self.db)
        self.assertTrue(isinstance(result, list))
        self.assertEqual(result[0][0], True)
        self.assertEqual(result[0][1], '_design/foo')
        doc = self.db[result[0][1]]
        self.assertEqual(result[0][2], doc['_rev'])

    def test_sync_many(self):
        '''see issue 218'''
        view_func = 'function(doc) { emit(doc._id, doc._rev); }'
        update_func = 'function(doc, req) { return [doc, "OK"]; }'
        validator_func = 'function(newDoc, oldDoc, userCtx, secObj) {}'
        show_func = 'function(doc, req) { return {status: "OK"}; }'
        first_view = design.ViewDefinition('design_doc', 'view_one', view_func)
        second_view = design.ViewDefinition('design_doc_two', 'view_one', view_func)
        third_view = design.ViewDefinition('design_doc', 'view_two', view_func)
        first_updater = design.UpdateHandlerDefinition('design_doc_two', 'update_one', update_func)
        second_updater = design.UpdateHandlerDefinition('design_doc_two', 'update_two', update_func)
        third_updater = design.UpdateHandlerDefinition('design_doc_three', 'update_one', update_func)
        first_validator = design.ValidateFunctionDefinition('design_doc_two', validator_func)
        second_validator = design.ValidateFunctionDefinition('design_doc_four', validator_func)
        first_show_func = design.ShowFunctionDefinition('design_doc_two', 'show_one', show_func)
        second_show_func = design.ShowFunctionDefinition('design_doc_two', 'show_two', show_func)
        third_show_func = design.ShowFunctionDefinition('design_doc_five', 'update_one', show_func)
        _, db = self.temp_db()
        results = design.sync_definitions(
            db, (first_view, second_view, third_view,
                 first_updater, second_updater, third_updater,
                 first_validator, second_validator,
                 first_show_func, second_show_func, third_show_func))
        self.assertEqual(
            len(results), 5, 'There should only be five design documents')

    def test_sync_unknown_definition_type(self):
        func = 'function(doc) { emit(doc._id, doc._rev); }'
        first_def = design.ViewDefinition('design_doc', 'view_one', func)
        second_def = design.ViewDefinition('design_doc_two', 'view_one', func)
        third_def = design.DefinitionMixin('design_doc_two')
        _, db = self.temp_db()
        self.assertRaises(TypeError,
            design.sync_definitions, db, (first_def, second_def, third_def))


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(DesignTestCase))
    suite.addTest(doctest.DocTestSuite(design))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
