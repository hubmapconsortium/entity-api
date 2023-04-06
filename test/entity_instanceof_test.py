import unittest
from unittest.mock import patch

from schema import schema_manager
import schema

schema_manager._schema = {
    'ENTITIES': {
        'A': {'superclass': 'B'},
        'B': {'superclass': 'C'},
        'C': {'superclass': 'D'},
        'D': {},
        'E': {}
    }
}


class TestEntityInstanceof(unittest.TestCase):

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='A')
    def test_1(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'D')
        self.assertTrue(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='C')
    def test_2(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'D')
        self.assertTrue(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='D')
    def test_3(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'A')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='A')
    def test_4(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'E')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='E')
    def test_5(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'A')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='Z')
    def test_6(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'A')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value='A')
    def test_7(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Z')
        self.assertFalse(assertion)

    @patch('schema.schema_neo4j_queries.get_entity_type', return_value=None)
    def test_8(self, mock_get_entity_type):
        assertion: bool = schema_manager.entity_instanceof('dummy uuid', 'Z')
        self.assertFalse(assertion)

if __name__ == '__main__':
    unittest.main()
