import unittest
from unittest.mock import patch, mock_open

# Local modules
from schema_templating import general_schema_template_transformer

#
# Running the tests... At the top level directory type 'nose2 --verbose --log-level debug`
#
# WARNING: ONLY methods beginning with "test_" will be considered tests by 'nose2' :-(
# 

yaml_content = """
               entities:
                 - Dataset
                 - Donor
                 - Sample
               """

class TestGeneralSchemaTemplateTransformer(unittest.TestCase):
    
    def setUp(self):
        self.yaml_file_path = 'fake_file_path'


    @patch("builtins.open", new_callable = mock_open, read_data = yaml_content)
    def test_input_from_yaml(self, new_callable):
        yaml_dict = general_schema_template_transformer.input_from_yaml(self.yaml_file_path)

        self.assertTrue(isinstance(yaml_dict, dict))
        self.assertTrue('entities' in yaml_dict)
        self.assertTrue(isinstance(yaml_dict['entities'], list))
        self.assertEqual(len(yaml_dict['entities']), 3)



if __name__ == "__main__":
    import nose2
    nose2.main()