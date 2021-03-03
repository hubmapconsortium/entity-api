import ast
import json
import logging
import datetime
from neo4j.exceptions import TransactionError
import os

# Local modules
from schema import schema_manager
from schema import schema_errors
from schema import schema_neo4j_queries

logger = logging.getLogger(__name__)

####################################################################################################
## Trigger methods shared among Collection, Dataset, Donor, Sample - DO NOT RENAME
####################################################################################################

"""
Trigger event method of generating current timestamp

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The neo4j TIMESTAMP() function as string
"""
def set_timestamp(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    # Use the neo4j TIMESTAMP() function during entity creation
    # Will be proessed in app_neo4j_queries._build_properties_map() 
    # and schema_neo4j_queries._build_properties_map()
    return property_key, 'TIMESTAMP()'

"""
Trigger event method of setting the entity type of a given entity

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The string of normalized entity type
"""
def set_entity_type(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    return property_key, normalized_type


"""
Trigger event method of getting user sub

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The 'sub' string
"""
def set_user_sub(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'sub' not in new_data_dict:
        raise KeyError("Missing 'sub' key in 'new_data_dict' during calling 'set_user_sub()' trigger method.")
    
    return property_key, new_data_dict['sub']

"""
Trigger event method of getting user email

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The 'email' string
"""
def set_user_email(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'email' not in new_data_dict:
        raise KeyError("Missing 'email' key in 'new_data_dict' during calling 'set_user_email()' trigger method.")
    
    return property_key, new_data_dict['email']

"""
Trigger event method of getting user name

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The 'name' string
"""
def set_user_displayname(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'name' not in new_data_dict:
        raise KeyError("Missing 'name' key in 'new_data_dict' during calling 'set_user_displayname()' trigger method.")
    
    return property_key, new_data_dict['name']

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The uuid created via uuid-api
"""
def set_uuid(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in new_data_dict:
        raise KeyError("Missing 'uuid' key in 'new_data_dict' during calling 'set_uuid()' trigger method.")
    
    return property_key, new_data_dict['uuid']

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The hubmap_id created via uuid-api
"""
def set_hubmap_id(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'hubmap_id' not in new_data_dict:
        raise KeyError("Missing 'hubmap_id' key in 'new_data_dict' during calling 'set_hubmap_id()' trigger method.")
    
    return property_key, new_data_dict['hubmap_id']


####################################################################################################
## Trigger methods shared by Sample, Donor, Dataset - DO NOT RENAME
####################################################################################################


"""
Trigger event method of generating data access level

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the entity types defined in the schema yaml: Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The data access level string
"""
def set_data_access_level(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in new_data_dict:
        raise KeyError("Missing 'uuid' key in 'new_data_dict' during calling 'set_data_access_level()' trigger method.")

    # For now, don't use the constants from commons
    # All lowercase for easy comparision
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'
    
    if normalized_type == 'Dataset':
        # 'contains_human_genetic_sequences' is required on create
        if 'contains_human_genetic_sequences' not in new_data_dict:
            raise KeyError("Missing 'contains_human_genetic_sequences' key in 'new_data_dict' during calling 'set_data_access_level()' trigger method.")

        # Default to protected
        data_access_level = ACCESS_LEVEL_PROTECTED

        # When `contains_human_genetic_sequences` is true, even if `status` is 'Published', 
        # the `data_access_level` is still 'protected'
        if new_data_dict['contains_human_genetic_sequences']:
            data_access_level = ACCESS_LEVEL_PROTECTED
        else:
            # When creating a new dataset, status should always be "New"
            # Thus we don't use Dataset.status == "Published" to determine the data_access_level as public
            data_access_level = ACCESS_LEVEL_CONSORTIUM
    else:
        # Default to consortium for Donor/Sample
        data_access_level = ACCESS_LEVEL_CONSORTIUM
        
        # public if any dataset below it in the provenance hierarchy is published
        # (i.e. Dataset.status == "Published")
        count = schema_neo4j_queries.count_attached_published_datasets(schema_manager.get_neo4j_driver_instance(), normalized_type, new_data_dict['uuid'])

        if count > 0:
            data_access_level = ACCESS_LEVEL_PUBLIC

    return property_key, data_access_level


"""
Trigger event method of setting the group_uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The group uuid
"""
def set_group_uuid(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    group_uuid = None

    # Look for membership in a single "data provider" group and sets to that. 
    # Otherwise if not set and no single "provider group" membership throws error.  
    # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
    if 'hmgroupids' not in new_data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'new_data_dict' during calling 'set_group_uuid()' trigger method.")

    user_group_uuids = new_data_dict['hmgroupids']

    # If group_uuid provided from incoming request, validate it
    if 'group_uuid' in new_data_dict:
        # A bit validation
        try:
            schema_manager.validate_entity_group_uuid(new_data_dict['group_uuid'], user_group_uuids)
        except schema_errors.NoDataProviderGroupException as e:
            # No need to log
            raise schema_errors.NoDataProviderGroupException(e)
        except schema_errors.UnmatchedDataProviderGroupException as e:
            raise schema_errors.UnmatchedDataProviderGroupException(e)

        group_uuid = new_data_dict['group_uuid']
    # When no group_uuid provided
    else:
        try:
            group_info = schema_manager.get_entity_group_info(user_group_uuids)
        except schema_errors.NoDataProviderGroupException as e:
            # No need to log
            raise schema_errors.NoDataProviderGroupException(e)
        except schema_errors.MultipleDataProviderGroupException as e:
            # No need to log
            raise schema_errors.MultipleDataProviderGroupException(e)

        group_uuid = group_info['uuid']

    return property_key, group_uuid

"""
Trigger event method of setting the group_name

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The group name
"""
def set_group_name(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    group_name = None
    
    # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that. 
    # Otherwise if not set and no single "provider group" membership throws error.  
    # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
    if 'hmgroupids' not in new_data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'new_data_dict' during calling 'set_group_name()' trigger method.")
    
    try:
        default_group_uuid = None
        if 'group_uuid' in new_data_dict:
            default_group_uuid = new_data_dict['group_uuid']
        group_info = schema_manager.get_entity_group_info(new_data_dict['hmgroupids'], default_group_uuid)
        group_name = group_info['name']
    except schema_errors.NoDataProviderGroupException as e:
        # No need to log
        raise schema_errors.NoDataProviderGroupException(e)
    except schema_errors.MultipleDataProviderGroupException as e:
        # No need to log
        raise schema_errors.MultipleDataProviderGroupException(e)

    return property_key, group_name
    

####################################################################################################
## Trigger methods shared by Donor and Sample - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting the submission_id
No submission_id for Dataset and Collection

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The submission_id
"""
def set_submission_id(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'submission_id' not in new_data_dict:
        raise KeyError("Missing 'submission_id' key in 'new_data_dict' during calling 'set_submission_id()' trigger method.")
    
    return property_key, new_data_dict['submission_id']


"""
Trigger event method to commit files saved that were previously uploaded with UploadFileHelper.save_file

The information, filename and optional description is saved in the field with name specified by `target_property_key`
in the provided data_dict.  The image files needed to be previously uploaded
using the temp file service (UploadFileHelper.save_file).  The temp file id provided
from UploadFileHelper, paired with an optional description of the file must be provided
in the field `image_files_to_add` in the data_dict for each file being committed
in a JSON array like below ("description" is optional): 

[
  {
    "temp_file_id": "eiaja823jafd",
    "description": "Image file 1"
  },
  {
    "temp_file_id": "pd34hu4spb3lk43usdr"
  },
  {
    "temp_file_id": "32kafoiw4fbazd",
    "description": "Image file 3"
  }
]


Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def commit_image_files(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    return _commit_files('image_files', property_key, normalized_type, user_token, existing_data_dict, new_data_dict)


"""
Trigger event methods for removing files from an entity during update

Files are stored in a json encoded text field with property name 'target_property_key' in the entity dict
The files to remove are specified as file uuids in the `property_key` field

The two outer methods (delete_image_files and delete_metadata_files) pass the target property
field name to private method, _delete_files along with the other required trigger properties

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

-----------
target_property_key: str
    The name of the property where the file information is stored

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def delete_image_files(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    return _delete_files('image_files', property_key, normalized_type, user_token, existing_data_dict, new_data_dict)


"""
Trigger event method to ONLY update descriptions of existing files

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
list: The file info dicts (with updated descriptions) in a list
"""
def update_file_descriptions(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if property_key not in new_data_dict:
        raise KeyError(f"Missing '{property_key}' key in 'new_data_dict' during calling 'update_file_descriptions()' trigger method.")

    if property_key not in existing_data_dict:
        raise KeyError(f"Missing '{property_key}' key in 'existing_data_dict' during calling 'update_file_descriptions()' trigger method.")

    # The property holding the file information must be a json array
    if not isinstance(new_data_dict[property_key], list):
        raise TypeError(f"'{property_key}' value in 'new_data_dict' must be a list during calling 'update_file_descriptions()' trigger method.")

    file_info_by_uuid_dict = {}
    # Convert the string literal to list
    existing_files_list = ast.literal_eval(existing_data_dict[property_key])

    for file_info in existing_files_list:
        file_uuid = file_info['file_uuid']

        file_info_by_uuid_dict[file_uuid] = file_info

    for file_info in new_data_dict[property_key]:
        file_uuid = file_info['file_uuid']
        
        # Existence check in case the file uuid gets edited in the request
        if file_uuid in file_info_by_uuid_dict:
            # Keep filename and file_uuid unchanged
            # Only update the description
            file_info_by_uuid_dict[file_uuid]['description'] = file_info['description']

    # In Python3, dict.values() returns a view of the dictionary's values instead of list
    return property_key, list(file_info_by_uuid_dict.values())



####################################################################################################
## Trigger methods specific to Collection - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting a list of associated datasets for a given collection

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
list: A list of associated dataset dicts with all the normalized information
"""
def get_collection_datasets(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_collection_datasets()' trigger method.")

    datasets_list = schema_neo4j_queries.get_collection_datasets(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Additional properties of the datasets to exclude 
    # We don't want to show too much nested information
    properties_to_skip = ['direct_ancestors', 'collections']
    complete_entities_list = schema_manager.get_complete_entities_list(user_token, datasets_list, properties_to_skip)

    return property_key, schema_manager.normalize_entities_list_for_response(complete_entities_list)


####################################################################################################
## Trigger methods specific to Dataset - DO NOT RENAME
####################################################################################################

"""
Trigger event method of setting the default status for this new Dataset

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: Initial status of "New"
"""
def set_dataset_status(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    # Always 'New' on dataset creation
    return property_key, 'New'


"""
Trigger event method of updating the dataset's data_access_level and 
its ancestors' data_access_level on status change of this dataset

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def update_dataset_and_ancestors_data_access_level(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'update_dataset_ancestors_data_access_level()' trigger method.")

    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'update_dataset_ancestors_data_access_level()' trigger method.")

    # Caculate the new data_access_level of this dataset's ancestors (except another dataset is the ancestor)
    # public if any dataset below the Donor/Sample in the provenance hierarchy is published
    ACCESS_LEVEL_PUBLIC = 'public'

    if existing_data_dict['status'].lower() == "published":
        try:
            schema_neo4j_queries.update_dataset_and_ancestors_data_access_level(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], ACCESS_LEVEL_PUBLIC)
        except TransactionError:
            # No need to log
            raise
        

"""
Trigger event method of getting a list of collections for this new Dtaset

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
list: A list of associated collections with all the normalized information
"""
def get_dataset_collections(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_collections()' trigger method.")

    # No property key needs to filter the result
    # Get back the list of collection dicts
    collections_list = schema_neo4j_queries.get_dataset_collections(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Exclude datasets from each resulting collection
    # We don't want to show too much nested information
    properties_to_skip = ['datasets']
    complete_entities_list = schema_manager.get_complete_entities_list(user_token, collections_list, properties_to_skip)

    return property_key, schema_manager.normalize_entities_list_for_response(complete_entities_list)

"""
Trigger event method of creating or recreating linkages between this new Dataset and its direct ancestors

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The uuid string of source entity
"""
def link_dataset_to_direct_ancestors(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    if 'direct_ancestor_uuids' not in existing_data_dict:
        raise KeyError("Missing 'direct_ancestor_uuids' key in 'existing_data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    # Important: existing_data_dict['direct_ancestor_uuids'] is stored as a string literal in neo4j, 
    # not an array. We need to convert it into a Python list.
    direct_ancestor_uuids = []
    property_value = existing_data_dict['direct_ancestor_uuids']
    
    if isinstance(property_value, str):
        # ast uses compile to compile the source string (which must be an expression) into an AST
        # If the source string is not a valid expression (like an empty string), a SyntaxError will be raised by compile
        # If, on the other hand, the source string would be a valid expression (e.g. a variable name like foo), 
        # compile will succeed but then literal_eval() might fail with a ValueError
        # Also this fails with a TypeError: literal_eval("{{}: 'value'}")
        try:
            direct_ancestor_uuids = ast.literal_eval(property_value)
        except (SyntaxError, ValueError, TypeError) as e:
            msg = "Invalid expression (string value) of key: direct_ancestor_uuids for ast.literal_eval()"
            logger.debug(msg)
            logger.debug(property_value)
            logger.exception(msg)

            raise ValueError(msg)
    
    # Generate property values for each Activity node
    count = len(direct_ancestor_uuids)
    activity_data_dict_list = schema_manager.generate_activity_data(normalized_type, user_token, existing_data_dict, count)

    try:
        # Create a linkage (via Activity node) between the dataset node 
        # and each direct ancestor node in neo4j
        schema_neo4j_queries.link_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], direct_ancestor_uuids, activity_data_dict_list)
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of getting source uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
list: A list of associated direct ancestors with all the normalized information
"""
def get_dataset_direct_ancestors(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_direct_ancestors()' trigger method.")

    # No property key needs to filter the result
    # Get back the list of ancestor dicts
    direct_ancestors_list = schema_neo4j_queries.get_dataset_direct_ancestors(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # We don't want to show too much nested information
    # The direct ancestor of a Dataset could be: Dataset or Sample
    # Skip running the trigger methods for 'direct_ancestors' and 'collections' if the direct ancestor is Dataset
    # Skip running the trigger methods for 'direct_ancestor' if the direct ancestor is Sample
    properties_to_skip = ['direct_ancestors', 'collections', 'direct_ancestor']
    complete_entities_list = schema_manager.get_complete_entities_list(user_token, direct_ancestors_list, properties_to_skip)

    return property_key, schema_manager.normalize_entities_list_for_response(complete_entities_list)


"""
Trigger event method of getting the relative directory path of a given dataset

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The relative directory path
"""
def get_local_directory_rel_path(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_local_directory_rel_path()' trigger method.")
    
    if 'data_access_level' not in existing_data_dict:
        raise KeyError("Missing 'data_access_level' key in 'existing_data_dict' during calling 'get_local_directory_rel_path()' trigger method.")
    
    uuid = existing_data_dict['uuid']

    if (not 'group_uuid' in existing_data_dict) or (not existing_data_dict['group_uuid']):
        raise KeyError(f"Group uuid not set for dataset with uuid: {uuid}")

    # Validate the group_uuid and make sure it's one of the valid data providers
    try:
        schema_manager.validate_entity_group_uuid(existing_data_dict['group_uuid'])
    except schema_errors.NoDataProviderGroupException as e:
        # No need to log
        raise schema_errors.NoDataProviderGroupException(e)

    group_name = schema_manager.get_entity_group_name(existing_data_dict['group_uuid'])

    dir_path = existing_data_dict['data_access_level'] + "/" + group_name + "/" + uuid + "/"

    return property_key, dir_path


"""
Trigger event method of building linkage from this new Dataset to the dataset of its previous revision

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The uuid string of source entity
"""
def link_to_previous_revision(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_to_previous_version_dataset()' trigger method.")

    if 'previous_revision_uuid' not in existing_data_dict:
        raise KeyError("Missing 'previous_revision_dataset_uuid' key in 'existing_data_dict' during calling 'link_to_previous_version_dataset()' trigger method.")

    # Create a revision reltionship from this new Dataset node and its previous revision of dataset node in neo4j
    try:
        schema_neo4j_queries.link_entity_to_previous_revision(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], existing_data_dict['previous_revision_uuid'])
    except TransactionError:
        # No need to log
        raise

"""
Trigger event method of getting the uuid of the previous revision dataset if exists

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The uuid string of previous revision entity or None if not found
"""
def get_previous_revision_uuid(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_previous_revision_uuid()' trigger method.")

    previous_revision_uuid = schema_neo4j_queries.get_previous_revision_uuid(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])
    
    # previous_revision_uuid can be None, but will be filtered out by 
    # schema_manager.normalize_entity_result_for_response()
    return property_key, previous_revision_uuid


"""
Trigger event method of getting the uuid of the next version dataset if exists

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The uuid string of next version entity or None if not found
"""
def get_next_revision_uuid(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_next_revision_uuid()' trigger method.")

    next_revision_uuid = schema_neo4j_queries.get_next_revision_uuid(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])
    
    # next_revision_uuid can be None, but will be filtered out by 
    # schema_manager.normalize_entity_result_for_response()
    return property_key, next_revision_uuid


####################################################################################################
## Trigger methods specific to Donor - DO NOT RENAME
####################################################################################################


"""
Trigger event method of building linkage between this new Donor and Lab

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_donor_to_lab(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_donor_to_lab()' trigger method.")

    if 'group_uuid' not in existing_data_dict:
        raise KeyError("Missing 'group_uuid' key in 'existing_data_dict' during calling 'link_donor_to_lab()' trigger method.")

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = [existing_data_dict['group_uuid']]

    # Generate property values for Activity
    # Only one Activity in this case, using the default count = 1
    activity_data_dict_list = schema_manager.generate_activity_data(normalized_type, user_token, existing_data_dict)

    try:
        # Create a linkage (via Activity node) 
        # between the Donor node and the parent Lab node in neo4j
        schema_neo4j_queries.link_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], direct_ancestor_uuids, activity_data_dict_list)
    except TransactionError:
        # No need to log
        raise



####################################################################################################
## Trigger methods specific to Sample - DO NOT RENAME
####################################################################################################

"""
Trigger event method to commit files saved that were previously uploaded with UploadFileHelper.save_file

The information, filename and optional description is saved in the field with name specified by `target_property_key`
in the provided data_dict.  The image files needed to be previously uploaded
using the temp file service (UploadFileHelper.save_file).  The temp file id provided
from UploadFileHelper, paired with an optional description of the file must be provided
in the field `image_files_to_add` in the data_dict for each file being committed
in a JSON array like below ("description" is optional): 

[
  {
    "temp_file_id": "eiaja823jafd",
    "description": "Metadata file 1"
  },
  {
    "temp_file_id": "pd34hu4spb3lk43usdr"
  },
  {
    "temp_file_id": "32kafoiw4fbazd",
    "description": "Metadata file 3"
  }
]


Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Sample
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def commit_metadata_files(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    return _commit_files('metadata_files', property_key, normalized_type, user_token, existing_data_dict, new_data_dict)


"""
Trigger event methods for removing files from an entity during update

Files are stored in a json encoded text field with property name 'target_property_key' in the entity dict
The files to remove are specified as file uuids in the `property_key` field

The two outer methods (delete_image_files and delete_metadata_files) pass the target property
field name to private method, _delete_files along with the other required trigger properties

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Sample
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

-----------
target_property_key: str
    The name of the property where the file information is stored

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def delete_metadata_files(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    return _delete_files('metadata_files', property_key, normalized_type, user_token, existing_data_dict, new_data_dict)
    

"""
Trigger event method of creating or recreating linkages between this new Sample and its direct ancestor

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_sample_to_direct_ancestor(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    if 'direct_ancestor_uuid' not in existing_data_dict:
        raise KeyError("Missing 'direct_ancestor_uuid' key in 'existing_data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = [existing_data_dict['direct_ancestor_uuid']]

    # Generate property values for Activity
    # Only one Activity in this case, using the default count = 1
    activity_data_dict_list = schema_manager.generate_activity_data(normalized_type, user_token, existing_data_dict)

    try:
        # Create a linkage (via Activity node) 
        # between the Sample node and the source entity node in neo4j
        schema_neo4j_queries.link_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], direct_ancestor_uuids, activity_data_dict_list)
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of getting the parent of a Sample

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
dict: The direct ancestor entity (either another Sample or a Donor) with all the normalized information
"""
def get_sample_direct_ancestor(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_sample_direct_ancestor()' trigger method.")

    direct_ancestor_dict = schema_neo4j_queries.get_sample_direct_ancestor(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    if 'entity_type' not in direct_ancestor_dict:
        raise KeyError("The 'entity_type' property in the resulting 'direct_ancestor_dict' is not set during calling 'get_sample_direct_ancestor()' trigger method.")

    # Generate trigger data for sample's direct_ancestor and skip the direct_ancestor's direct_ancestor
    properties_to_skip = ['direct_ancestor']
    complete_dict = schema_manager.get_complete_entity_result(user_token, direct_ancestor_dict, properties_to_skip)

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entity_result_for_response(complete_dict)


####################################################################################################
## Trigger methods specific to Activity - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting creation_action for Activity

Lab->Activity->Donor (Not needed for now)
Donor->Activity->Sample
Sample->Activity->Sample
Sample->Activity->Dataset
Dataset->Activity->Dataset

Register Donor Activity
Create Sample Activity

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The creation_action string
"""
def set_activity_creation_action(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    if 'normalized_entity_type' not in new_data_dict:
        raise KeyError("Missing 'normalized_entity_type' key in 'existing_data_dict' during calling 'set_activity_creation_action()' trigger method.")
    
    return property_key, f"Create {new_data_dict['normalized_entity_type']} Activity"


####################################################################################################
## Internal functions
####################################################################################################

"""
Trigger event method to commit files saved that were previously uploaded with UploadFileHelper.save_file

The information, filename and optional description is saved in the field with name specified by `target_property_key`
in the provided data_dict.  The image files needed to be previously uploaded
using the temp file service (UploadFileHelper.save_file).  The temp file id provided
from UploadFileHelper, paired with an optional description of the file must be provided
in the field `image_files_to_add` in the data_dict for each file being committed
in a JSON array like below ("description" is optional): 

[
  {
    "temp_file_id": "eiaja823jafd",
    "description": "File 1"
  },
  {
    "temp_file_id": "pd34hu4spb3lk43usdr"
  },
  {
    "temp_file_id": "32kafoiw4fbazd",
    "description": "File 3"
  }
]


Parameters
----------
target_property_key : str
    The target property key of the value to be generated
property_key : str
    The property key for which the original trigger method is defined
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def _commit_files(target_property_key, property_key, normalized_type, user_token, existing_data_dict, new_data_dict):
    
    # Do nothing if no files to add are provided (missing or empty property)
    # For image files the property name is "image_files_to_add"
    # For metadata files the property name is "metadata_files_to_add"
    # But other may be used in the future
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return property_key, None
    
    # If POST or PUT where the target doesn't exist create the file info array
    if not target_property_key in existing_data_dict:
        files_info_list = []
    # Otherwise this is a PUT where the target array exists already
    else:
        # Note: The property, name specified by `target_property_key`, is stored in Neo4j as a string representation of the Python list
        # It's not stored in Neo4j as a json string! And we can't store it as a json string 
        # due to the way that Cypher handles single/double quotes.
        files_info_list = ast.literal_eval(existing_data_dict[target_property_key])

    try:
        if 'uuid' in new_data_dict:
            entity_uuid = new_data_dict['uuid']
        else:
            entity_uuid = existing_data_dict['uuid']

        for file_info in new_data_dict[property_key]:
            file_uuid_info = schema_manager.get_file_upload_helper_instance().commit_file(file_info['temp_file_id'], entity_uuid, user_token)
            
            file_info_to_add = {
                'filename': file_uuid_info['filename'],
                'file_uuid': file_uuid_info['file_uuid']
            }
            
            # The `description` is optional
            if 'description' in file_info:
                file_info_to_add['description'] = file_info['description']
            
            # Add to list
            files_info_list.append(file_info_to_add)
        
        # Assign the target value to a different property key rather than itself
        return target_property_key, files_info_list
    except schema_errors.FileUploadException as e:
        raise
    except Exception as e:
        # No need to log
        raise


"""
Trigger event methods for removing files from an entity during update

Files are stored in a json encoded text field with property name 'target_property_key' in the entity dict
The files to remove are specified as file uuids in the `property_key` field

The two outer methods (delete_image_files and delete_metadata_files) pass the target property
field name to private method, _delete_files along with the other required trigger properties

Parameters
----------
target_property_key : str
    The target property key of the value to be generated
property_key : str
    The property key for which the original trigger method is defined
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

-----------
target_property_key: str
    The name of the property where the file information is stored

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def _delete_files(target_property_key, property_key, normalized_type, user_token, existing_data_dict, new_data_dict):

    #do nothing if no files to delete are provided in the field specified by property_key
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return property_key, None

    if 'uuid' not in existing_data_dict:
        raise KeyError(f"Missing 'uuid' key in 'existing_data_dict' during calling '_delete_files()' trigger method for property '{target_property_key}'.")
    
    if target_property_key not in existing_data_dict:
        raise KeyError(f"Missing '{target_property_key}' key in 'existing_data_dict' during calling '_delete_files()' trigger method.")

    try:
        entity_uuid = existing_data_dict['uuid']
        # `upload_dir` is already normalized with trailing slash
        entity_upload_dir = schema_manager.get_file_upload_helper_instance().upload_dir + entity_uuid + os.sep
        
        # Note: The property named by target_property_key value is stored in Neo4j as a string representation of the Python list
        # It's not stored in Neo4j as a json string! And we can't store it as a json string 
        # due to the way that Cypher handles single/double quotes.
        files_info_list = ast.literal_eval(existing_data_dict[target_property_key])
        
        # Remove physical files from the file system
        for filename in new_data_dict[property_key]:
            # Get back the updated files_info_list
            files_info_list = schema_manager.get_file_upload_helper_instance().remove_file(entity_upload_dir, filename, files_info_list)
        
        # Assign the target value to a different property key rather than itself
        return target_property_key, files_info_list
    except Exception as e:
        # No need to log
        raise
