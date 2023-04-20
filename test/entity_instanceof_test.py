import unittest
from unittest.mock import patch

from schema import schema_manager
import schema

schema_manager._schema = {
    'ENTITIES': {
        'Aa': {'superclass': 'Bb'},
        'Bb': {'superclass': 'Cc'},
        'Cc': {'superclass': 'Dd'},
        'Dd': {},
        'Ee': {}
    }
}


class TestEntityInstanceof(unittest.TestCase):

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Aa')
    def test_1(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Aa')
        self.assertTrue(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Aa')
    def test_2(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Dd')
        self.assertTrue(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='aa')
    def test_3(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'dd')
        self.assertTrue(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Cc')
    def test_4(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Dd')
        self.assertTrue(assertion)


    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Dd')
    def test_5(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Aa')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Aa')
    def test_6(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Ee')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Ee')
    def test_7(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Aa')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Zz')
    def test_8(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Aa')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Aa')
    def test_9(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Zz')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value=None)
    def test_10(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Aa')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value=None)
    def test_11(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Zz')
        self.assertFalse(assertion)

    def test_12(self):
        assertion: bool = schema_manager.entity_type_instanceof('Aa', 'Dd')
        self.assertTrue(assertion)

    def test_13(self):
        assertion: bool = schema_manager.entity_type_instanceof('Aa', 'Zz')
        self.assertFalse(assertion)
        

if __name__ == '__main__':
    unittest.main()
