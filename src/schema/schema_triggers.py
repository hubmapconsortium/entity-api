import os
import ast
import json
import re

import yaml
import logging
import requests
from datetime import datetime
from neo4j.exceptions import TransactionError

# Use the current_app proxy, which points to the application handling the current activity
from flask import current_app as app

# Local modules
from schema import schema_manager
from schema import schema_errors
from schema import schema_neo4j_queries
from schema.schema_constants import SchemaConstants

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
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_timestamp(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_entity_type(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    return property_key, normalized_type


"""
Trigger event method of getting user sub

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_user_sub(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_user_email(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_user_displayname(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_uuid(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_hubmap_id(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_data_access_level(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in new_data_dict:
        raise KeyError("Missing 'uuid' key in 'new_data_dict' during calling 'set_data_access_level()' trigger method.")

    if normalized_type == 'Dataset':
        # 'contains_human_genetic_sequences' is required on create
        if 'contains_human_genetic_sequences' not in new_data_dict:
            raise KeyError("Missing 'contains_human_genetic_sequences' key in 'new_data_dict' during calling 'set_data_access_level()' trigger method.")

        # Default to protected
        data_access_level = SchemaConstants.ACCESS_LEVEL_PROTECTED

        # When `contains_human_genetic_sequences` is true, even if `status` is 'Published', 
        # the `data_access_level` is still 'protected'
        if new_data_dict['contains_human_genetic_sequences']:
            data_access_level = SchemaConstants.ACCESS_LEVEL_PROTECTED
        else:
            # When creating a new dataset, status should always be "New"
            # Thus we don't use Dataset.status == "Published" to determine the data_access_level as public
            data_access_level = SchemaConstants.ACCESS_LEVEL_CONSORTIUM
    else:
        # Default to consortium for Donor/Sample
        data_access_level = SchemaConstants.ACCESS_LEVEL_CONSORTIUM
        
        # public if any dataset below it in the provenance hierarchy is published
        # (i.e. Dataset.status == "Published")
        count = schema_neo4j_queries.count_attached_published_datasets(schema_manager.get_neo4j_driver_instance(), normalized_type, new_data_dict['uuid'])

        if count > 0:
            data_access_level = SchemaConstants.ACCESS_LEVEL_PUBLIC

    return property_key, data_access_level


"""
Trigger event method of setting the group_uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample, Dataset
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_group_uuid(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
    One of the types defined in the schema yaml: Donor, Sample, Dataset
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_group_name(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
No submission_id for Dataset, Collection, and Upload

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_submission_id(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
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
    One of the types defined in the schema yaml: Donor, Sample
request: Flask request object
    The instance of Flask request passed in from application request
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
def commit_image_files(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    return _commit_files('image_files', property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict)


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
request: Flask request object
    The instance of Flask request passed in from application request
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
def delete_image_files(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    return _delete_files('image_files', property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict)


"""
Trigger event method to ONLY update descriptions of existing files

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
request: Flask request object
    The instance of Flask request passed in from application request
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
def update_file_descriptions(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    if property_key not in new_data_dict:
        raise KeyError(f"Missing '{property_key}' key in 'new_data_dict' during calling 'update_file_descriptions()' trigger method.")

    # If POST or PUT where the target doesn't exist create the file info array
    # if generated_dict doesn't contain the property yet, copy it from the existing_data_dict 
    # or if it doesn't exist in existing_data_dict create it
    if not property_key in generated_dict:
        if not property_key in existing_data_dict:
            raise KeyError(f"Missing '{property_key}' key in 'existing_data_dict' during call to 'update_file_descriptions()' trigger method.")
        # Otherwise this is a PUT where the target array exists already
        else:
            logger.info(f"Executing convert_str_literal() on {normalized_type}.{property_key} during calling 'update_file_descriptions()' trigger method.")

            # Note: The property, name specified by `target_property_key`, is stored in Neo4j as a string representation of the Python list
            # It's not stored in Neo4j as a json string! And we can't store it as a json string 
            # due to the way that Cypher handles single/double quotes.
            existing_files_list = schema_manager.convert_str_literal(existing_data_dict[property_key])
    else:
        if not property_key in generated_dict:
            raise KeyError(f"Missing '{property_key}' key in 'generated_dict' during calling 'update_file_descriptions()' trigger method.")            
        existing_files_list = generated_dict[property_key]

    file_info_by_uuid_dict = {}

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

    generated_dict[property_key] = list(file_info_by_uuid_dict.values())
    return generated_dict



####################################################################################################
## Trigger methods shared by Dataset, Upload, and Publication - DO NOT RENAME
####################################################################################################

"""
Trigger event method of tracking status change events

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def set_status_history(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    new_status_history = []
    status_entry = {}

    if 'status_history' in existing_data_dict:
        status_history_string = existing_data_dict['status_history'].replace("'", "\"")
        new_status_history += json.loads(status_history_string)

    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'set_status_history()' trigger method")
    if 'last_modified_timestamp' not in existing_data_dict:
        raise KeyError("Missing 'last_modified_timestamp' key in 'existing_dat_dict' during calling 'set_status_history()' trigger method.")
    if 'last_modified_user_email' not in existing_data_dict:
        raise KeyError("Missing 'last_modified_user_email' key in 'existing_data_dict' during calling 'set_status_hisotry()' trigger method.")

    status = existing_data_dict['status']
    last_modified_user_email = existing_data_dict['last_modified_user_email']
    last_modified_timestamp = existing_data_dict['last_modified_timestamp']
    uuid = existing_data_dict['uuid']

    status_entry['status'] = status
    status_entry['changed_by_email'] = last_modified_user_email
    status_entry['change_timestamp'] = last_modified_timestamp
    new_status_history.append(status_entry)
    entity_data_dict = {"status_history": new_status_history}

    schema_neo4j_queries.update_entity(schema_manager.get_neo4j_driver_instance(), normalized_type, entity_data_dict, uuid)



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
    One of the types defined in the schema yaml: Collection
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_collection_datasets(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_collection_datasets()' trigger method.")

    logger.info(f"Executing 'get_collection_datasets()' trigger method on uuid: {existing_data_dict['uuid']}")

    datasets_list = schema_neo4j_queries.get_collection_datasets(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entities_list_for_response(datasets_list)


####################################################################################################
## Trigger methods specific to Dataset - DO NOT RENAME
####################################################################################################

"""
Trigger event method of setting the default "New" status for this new Dataset

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_dataset_status_new(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    # Always 'New' on dataset creation
    return property_key, 'New'


"""
Trigger event method of getting a list of collections for this new Dataset

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_dataset_collections(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_collections()' trigger method.")

    logger.info(f"Executing 'get_dataset_collections()' trigger method on uuid: {existing_data_dict['uuid']}")

    collections_list = schema_neo4j_queries.get_dataset_collections(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entities_list_for_response(collections_list)


"""
Trigger event method of getting the associated collection for this publication

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
dict: A dictionary representation of the associated collection with all the normalized information
"""
def get_publication_associated_collection(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_publication_associated_collection()' trigger method.")

    logger.info(f"Executing 'get_publication_associated_collection()' trigger method on uuid: {existing_data_dict['uuid']}")

    collection_dict = schema_neo4j_queries.get_publication_associated_collection(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entity_result_for_response(collection_dict)


"""
Trigger event method of getting the associated Upload for this Dataset

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
dict: A dict of associated Upload detail with all the normalized information
"""
def get_dataset_upload(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    return_dict = None
    
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_upload()' trigger method.")

    logger.info(f"Executing 'get_dataset_upload()' trigger method on uuid: {existing_data_dict['uuid']}")

    upload_dict = schema_neo4j_queries.get_dataset_upload(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])
    
    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entity_result_for_response(upload_dict)


"""
Trigger event method of creating or recreating linkages between this new Dataset and its direct ancestors

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_dataset_to_direct_ancestors(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    if 'direct_ancestor_uuids' not in new_data_dict:
        raise KeyError("Missing 'direct_ancestor_uuids' key in 'new_data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    dataset_uuid = existing_data_dict['uuid']
    direct_ancestor_uuids = new_data_dict['direct_ancestor_uuids']

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(normalized_type, request, user_token, existing_data_dict)

    try:
        # Create a linkage (via one Activity node) between the dataset node and its direct ancestors in neo4j
        schema_neo4j_queries.link_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), dataset_uuid, direct_ancestor_uuids, activity_data_dict)
    
        # Delete the cache of this dataset if any cache exists
        # Because the `Dataset.direct_ancestors` field
        schema_manager.delete_memcached_cache([dataset_uuid])
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method for creating or recreating linkages between this new Collection and the Datasets it contains

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_collection_to_datasets(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_collection_to_datasets()' trigger method.")

    if 'dataset_uuids' not in new_data_dict:
        raise KeyError("Missing 'dataset_uuids' key in 'new_data_dict' during calling 'link_collection_to_datasets()' trigger method.")

    collection_uuid = existing_data_dict['uuid']
    dataset_uuids = new_data_dict['dataset_uuids']

    try:
        # Create a linkage (without an Activity node) between the Collection node and each Dataset it contains.
        schema_neo4j_queries.link_collection_to_datasets(neo4j_driver=schema_manager.get_neo4j_driver_instance()
                                                         ,collection_uuid=existing_data_dict['uuid']
                                                         ,dataset_uuid_list=dataset_uuids)

        # Delete the cache of each associated dataset and the collection itself if any cache exists
        # Because the `Dataset.collecctions` field and `Collection.datasets` field
        uuids_list = [collection_uuid] + dataset_uuids
        schema_manager.delete_memcached_cache(uuids_list)
    except TransactionError as te:
        # No need to log
        raise


"""
Trigger event method of getting a list of direct ancestors for a given dataset or publication

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Dataset/Publication
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_dataset_direct_ancestors(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_direct_ancestors()' trigger method.")

    logger.info(f"Executing 'get_dataset_direct_ancestors()' trigger method on uuid: {existing_data_dict['uuid']}")

    direct_ancestors_list = schema_neo4j_queries.get_dataset_direct_ancestors(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entities_list_for_response(direct_ancestors_list)


"""
Trigger event method of getting the relative directory path of a given dataset

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_local_directory_rel_path(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_local_directory_rel_path()' trigger method.")
    
    logger.info(f"Executing 'get_local_directory_rel_path()' trigger method on uuid: {existing_data_dict['uuid']}")

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
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_to_previous_revision(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    try:
        if 'uuid' not in existing_data_dict:
            raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_to_previous_revision()' trigger method.")

        if 'previous_revision_uuid' not in new_data_dict:
            raise KeyError("Missing 'previous_revision_uuid' key in 'new_data_dict' during calling 'link_to_previous_revision()' trigger method.")

        entity_uuid = existing_data_dict['uuid']
        if isinstance(new_data_dict['previous_revision_uuid'], list):
            previous_uuid = new_data_dict['previous_revision_uuid']
        else:
            previous_uuid = [new_data_dict['previous_revision_uuid']]

        # Create a revision reltionship from this new Dataset node and its previous revision of dataset node in neo4j
        try:
            schema_neo4j_queries.link_entity_to_previous_revision(schema_manager.get_neo4j_driver_instance(), entity_uuid, previous_uuid)

            # Delete the cache of each associated dataset if any cache exists
            # Because the `Dataset.previous_revision_uuid` and `Dataset.next_revision_uuid` fields
            uuids_list = [entity_uuid]
            if isinstance(previous_uuid, list):
                uuids_list.extend(previous_uuid)
            else:
                uuids_list.append(previous_uuid)
            schema_manager.delete_memcached_cache(uuids_list)
        except TransactionError:
            # No need to log
            raise
    except Exception as e:
        raise KeyError(e)


"""
Given a string which contains multiple items, each separated by the substring specified by
the 'separator' argument, and possibly also ending with 'separator',
- remove the last instance of 'separator'
- replaced the remaining last instance of 'separator' with ", and"
- replace all remaining instances of 'separator' with the substring specified in the 'new_separator' argument

Parameters
----------
separated_phrase : str
    A string which contains multiple items, each separated by the substring specified by
    the 'separator' argument, and possibly also ending with 'separator'
separator : str
    A string which is used to separate items during computation.  This should be something which
    is statistically improbable to occur within items, such as a comma or a common word.
new_separator: str
    The replacement for occurrences of 'separator', such as a comma or a comma followed by a space.

Returns
-------
str: A version of the 'separated_phase' argument revised per the method description
"""
def _make_phrase_from_separator_delineated_str(separated_phrase:str, separator:str, new_separator=', ')->str:
    # Remove the last separator
    if re.search(rf"{separator}$", separated_phrase):
        separated_phrase = re.sub(  pattern=rf"(.*)({separator})$"
                                    , repl=r"\1"
                                    , string=separated_phrase)
    # Replace the last separator with the word 'and' for inclusion in the Dataset title
    separated_phrase = re.sub(  pattern=rf"(.*)({separator})(.*?)$"
                                , repl=r"\1, and \3"
                                , string=separated_phrase)
    # Replace all remaining separator with commas
    descriptions = separated_phrase.rsplit(separator)
    return new_separator.join(descriptions)


"""
Given a string of metadata for a Donor which was returned from Neo4j, and a list of desired attribute names to
extract from that metadata, return a dictionary containing lower-case version of each attribute found.

Parameters
----------
neo4j_donor_metadata : str
    A string representation of a Python dict returned from Neo4j, containing metadata for a Donor.
attribute_key_list : list[str]
    A list of strings, each of which may be the name of a key found in the Donor metadata.

Returns
-------
dict: A dict keyed using elements of attribute_key_list which were found in the Donor metadata, containing
      a lower-case version of the value stored in Neo4j
"""
def _get_attributes_from_donor_metadata(neo4j_donor_metadata: str, attribute_key_list: list[str]) -> dict:
    # Note: The donor_metadata is stored in Neo4j as a string representation of the Python dict
    # It's not stored in Neo4j as a json string! And we can't store it as a json string
    # due to the way that Cypher handles single/double quotes.
    donor_metadata_dict = schema_manager.convert_str_literal(neo4j_donor_metadata)

    # Since either 'organ_donor_data' or 'living_donor_data' can be present in donor_metadata_dict, but not
    # both, just grab the first element.  If neither are present, use the empty list
    data_list = []
    if donor_metadata_dict:
        data_list = list(donor_metadata_dict.values())[0]

    donor_grouping_concepts_dict = dict()
    for data in data_list:
        if 'grouping_concept_preferred_term' in data:
            if data['grouping_concept_preferred_term'].lower() == 'age':
                # The actual value of age stored in 'data_value' instead of 'preferred_term'
                donor_grouping_concepts_dict['age'] = data['data_value']
                donor_grouping_concepts_dict['age_units'] = data['units'][0:-1].lower()
            elif data['grouping_concept_preferred_term'].lower() == 'race':
                donor_grouping_concepts_dict['race'] = data['preferred_term'].lower()
            elif data['grouping_concept_preferred_term'].lower() == 'sex':
                donor_grouping_concepts_dict['sex'] = data['preferred_term'].lower()
            else:
                pass
    return donor_grouping_concepts_dict


"""
Given a age, race, and sex metadata for a Donor which was returned from Neo4j, generate an appropriate and
consistent string phrase. 

Parameters
----------
age : str
    A age value found in the metadata for the Donor returned from Neo4j.
race : str
    A race value found in the metadata for the Donor returned from Neo4j.
sex : str
    A sex value found in the metadata for the Donor returned from Neo4j.

Returns
-------
str: A consistent string phrase appropriate for the Donor's metadata
"""
def _get_age_age_units_race_sex_phrase(age:str=None, age_units:str='units', race:str=None, sex:str=None)->str:
    if age is None and race is not None and sex is not None:
        return f"{race} {sex} of unknown age"
    elif race is None and age is not None and sex is not None:
        return f"{age}-{age_units}-old {sex} of unknown race"
    elif sex is None and age is not None and race is not None:
        return f"{age}-{age_units}-old {race} donor of unknown sex"
    elif age is None and race is None and sex is not None:
        return f"{sex} donor of unknown age and race"
    elif age is None and sex is None and race is not None:
        return f"{race} donor of unknown age and sex"
    elif race is None and sex is None and age is not None:
        return f"{age}-{age_units}-old donor of unknown race and sex"
    elif age is None and race is None and sex is None:
        return "donor of unknown age, race and sex"
    else:
        return f"{age}-{age_units}-old {race} {sex}"


"""
Trigger event method of auto generating the dataset title

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The generated dataset title 
"""
def get_dataset_title(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):

    MAX_ENTITY_LIST_LENGTH = 5

    # Statistically improbable phrase to separate items while building a phrase, which can be
    # replaced by a grammatically correct separator like the word 'and' or a comma later
    ITEM_SEPARATOR_SIP = '_-_-_-ENTITY_SEPARATOR-_-_-_'

    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_dataset_title()' trigger method.")

    logger.info(f"Executing 'get_dataset_title()' trigger method on uuid: {existing_data_dict['uuid']}")

    # Assume organ_desc is always available, otherwise will throw parsing error
    organ_desc = '<organ_desc>'

    dataset_type = existing_data_dict['dataset_type']

    # Get the sample organ name and donor metadata information of this dataset
    donor_organs_list = \
        schema_neo4j_queries.get_dataset_donor_organs_info( neo4j_driver=schema_manager.get_neo4j_driver_instance()
                                                            , dataset_uuid=existing_data_dict['uuid'])

    # Determine the number of unique organ types and the number of unique donors in
    # donor_organs_list so the format of the title to be created can be determined.
    organ_abbrev_set = set()
    donor_metadata_list = list()
    donor_uuid_set = set()
    for donor_organ_data in donor_organs_list:
        organ_abbrev_set.add(donor_organ_data['organ_type'])
        donor_metadata_list.append(donor_organ_data['donor_metadata'])
        donor_uuid_set.add(donor_organ_data['donor_uuid'])

    # If the number of unique organ types is no more than MAX_ENTITY_LIST_LENGTH, we need to come up
    # with a phrase to be used to create the title which describes them.  If there are more than
    # the threshold, we will just use the number in the title.
    organs_description_phrase = f"{len(organ_abbrev_set)} organs"
    organ_types_dict = schema_manager.get_organ_types()
    if len(organ_abbrev_set) <= MAX_ENTITY_LIST_LENGTH:
        organ_description_set = set()
        if organ_abbrev_set:
            for organ_abbrev in organ_abbrev_set:
                try:
                    # The organ_abbrev is the two-letter code only set for 'organ'
                    # Convert the two-letter code to a description
                    organ_desc = organ_types_dict[organ_abbrev]
                    organ_description_set.add(organ_desc.lower())
                except (yaml.YAMLError, requests.exceptions.RequestException) as e:
                    raise Exception(e)
        # Turn the set of organ descriptions into a phrase which can be used to compose the Dataset title
        organs_description_phrase = ITEM_SEPARATOR_SIP.join(organ_description_set)
        organs_description_phrase = _make_phrase_from_separator_delineated_str(organs_description_phrase
                                                                               , ITEM_SEPARATOR_SIP)

    # If the number of unique organ donors is no more than MAX_ENTITY_LIST_LENGTH, we need to come up
    # with a phrase to be used to create the title which describes them.  If there are more than
    # the threshold, we will just use the number in the title.
    # Parse age, race, and sex from the donor metadata, but determine the number of donors using donor_uuid_set.
    donors_description_phrase = f"{len(donor_uuid_set)} donors"
    if len(donor_uuid_set) <= MAX_ENTITY_LIST_LENGTH:
        donors_grouping_concepts_dict = dict()
        if donor_metadata_list:
            for donor_metadata in donor_metadata_list:
                logger.info(f"Executing _get_attributes_from_donor_metadata() to"
                            f" convert_str_literal() on donor_metadata"
                            f" to get 'age','race','sex' attributes for uuid:"
                            f" {existing_data_dict['uuid']}"
                            f" during calling 'get_dataset_title()' trigger method.")
                donor_data = _get_attributes_from_donor_metadata(neo4j_donor_metadata=donor_metadata
                                                                 , attribute_key_list=['age', 'race', 'sex'])
                age = donor_data['age'] if donor_data and 'age' in donor_data else None
                age_units = donor_data['age_units'] if donor_data and 'age_units' in donor_data else None
                race = donor_data['race'] if donor_data and 'race' in donor_data else None
                sex = donor_data['sex'] if donor_data and 'sex' in donor_data else None
                age_race_sex_info = _get_age_age_units_race_sex_phrase( age=age
                                                                        , age_units=age_units
                                                                        , race=race
                                                                        , sex=sex)
                if age_race_sex_info in donors_grouping_concepts_dict:
                    donors_grouping_concepts_dict[age_race_sex_info] += 1
                else:
                    donors_grouping_concepts_dict[age_race_sex_info] = 1

        donors_description_phrase = ''
        for age_race_sex_info in donors_grouping_concepts_dict.keys():
            if len(donors_grouping_concepts_dict) > 1:
                donors_description_phrase += f"({donors_grouping_concepts_dict[age_race_sex_info]}) "
            donors_description_phrase += f"{age_race_sex_info}{ITEM_SEPARATOR_SIP}"

        donors_description_phrase = _make_phrase_from_separator_delineated_str(donors_description_phrase
                                                                               , ITEM_SEPARATOR_SIP)

    # When both the number of unique organ codes is between 2 and MAX_ENTITY_LIST_LENGTH and
    # the number of unique organ donors is between 2 and MAX_ENTITY_LIST_LENGTH, we will
    # use a phrase which associates each organ type and donor metadata rather than the
    # phrases previously built.
    donor_organ_association_phrase = ''
    if len(organ_abbrev_set) <= MAX_ENTITY_LIST_LENGTH: # and len(donor_uuid_set) <= MAX_ENTITY_LIST_LENGTH:
        for donor_organ_data in donor_organs_list:
            # The organ_abbrev is the two-letter code only set for 'organ'
            # Convert the two-letter code to a description
            organ_desc = organ_types_dict[donor_organ_data['organ_type']]

            logger.info(f"Executing _get_attributes_from_donor_metadata() to"
                        f" convert_str_literal() on donor_organ_data['donor_metadata']"
                        f" to get 'age','race','sex' attributes for uuid:"
                        f" {existing_data_dict['uuid']}"
                        f" during calling 'get_dataset_title()' trigger method.")
            donor_data = _get_attributes_from_donor_metadata(   neo4j_donor_metadata=donor_organ_data['donor_metadata']
                                                                , attribute_key_list=['age','race','sex'])

            age = donor_data['age'] if donor_data and 'age' in donor_data else None
            age_units = donor_data['age_units'] if donor_data and 'age_units' in donor_data else None
            race = donor_data['race'] if donor_data and 'race' in donor_data  else None
            sex = donor_data['sex'] if donor_data and 'sex' in donor_data  else None
            age_race_sex_info = _get_age_age_units_race_sex_phrase( age=age
                                                                    , age_units=age_units
                                                                    , race=race
                                                                    , sex=sex)
            donor_organ_association_phrase += f"{organ_desc.lower()} of {age_race_sex_info}{ITEM_SEPARATOR_SIP}"

        donor_organ_association_phrase = _make_phrase_from_separator_delineated_str(donor_organ_association_phrase
                                                                                    , ITEM_SEPARATOR_SIP)

    if dataset_type in ['Publication [ancillary]']:
        generated_title =   f"Support data used by a publication's display and vignette visualization(s)."
    elif len(organ_abbrev_set) == 1 and len(donor_uuid_set) == 1:
        # One donor, one organ type
        generated_title =   f"{dataset_type} data from the {organs_description_phrase} of a {donors_description_phrase}"
    elif len(organ_abbrev_set) > 1 and len(organ_abbrev_set) <= MAX_ENTITY_LIST_LENGTH and len(donor_uuid_set) == 1:
        # One donor, and more than 1 and less than MAX_ENTITY_LIST_LENGTH organ types
        generated_title = f"{dataset_type} data from {organs_description_phrase} of" \
                          f" a {donors_description_phrase}"
    elif len(organ_abbrev_set) > MAX_ENTITY_LIST_LENGTH and len(donor_uuid_set) == 1:
        # One donor, more than MAX_ENTITY_LIST_LENGTH organ types
        generated_title = f"{dataset_type} data from {len(organ_abbrev_set)} organs of" \
                          f" a {donors_description_phrase}"
    elif len(organ_abbrev_set) == 1 and len(donor_uuid_set) > 1 and len(donor_uuid_set) <= MAX_ENTITY_LIST_LENGTH:
        # More than 1 and less than MAX_ENTITY_LIST_LENGTH donors, and one organ type
        generated_title =   f"{dataset_type} data from the {organs_description_phrase} of" \
                            f" {len(donor_uuid_set)} different donors: {donors_description_phrase}"
    elif len(organ_abbrev_set) == 1 and len(donor_uuid_set) > MAX_ENTITY_LIST_LENGTH:
        # More than MAX_ENTITY_LIST_LENGTH donors, one organ type
        generated_title =   f"{dataset_type} data from the {organs_description_phrase}" \
                            f" of {len(donor_uuid_set)} different donors"
    elif    len(organ_abbrev_set) > 1 and len(organ_abbrev_set) <= MAX_ENTITY_LIST_LENGTH and \
            len(donor_uuid_set) > 1 and len(donor_uuid_set) <= MAX_ENTITY_LIST_LENGTH:
        # More than 1 and less than MAX_ENTITY_LIST_LENGTH donors, and
        # more than 1 and less than MAX_ENTITY_LIST_LENGTH organ types
        generated_title =   f"{dataset_type} data from {len(organ_abbrev_set)} organs of" \
                            f" {len(donor_uuid_set)} different donors:" \
                            f" {donor_organ_association_phrase}"
    elif len(organ_abbrev_set) > MAX_ENTITY_LIST_LENGTH and len(donor_uuid_set) > 1 and len(donor_uuid_set) <= MAX_ENTITY_LIST_LENGTH:
        #  More than 1 and less than MAX_ENTITY_LIST_LENGTH donors, and more than MAX_ENTITY_LIST_LENGTH organ type
        generated_title = f"{dataset_type} data from {len(organ_abbrev_set)} organs of" \
                          f" {len(donor_uuid_set)} different donors:" \
                          f" {donors_description_phrase}"
    elif len(organ_abbrev_set) > 1 and len(organ_abbrev_set) <= MAX_ENTITY_LIST_LENGTH and len(donor_uuid_set) > MAX_ENTITY_LIST_LENGTH:
        #  More than MAX_ENTITY_LIST_LENGTH donors, and more than 1 and less than MAX_ENTITY_LIST_LENGTH organ type
        generated_title = f"{dataset_type} data from the {organs_description_phrase}" \
                          f" of {len(donor_uuid_set)} different donors"
    else:
        # Default, including more than MAX_ENTITY_LIST_LENGTH donors, and more than MAX_ENTITY_LIST_LENGTH organ types
        generated_title =   f"{dataset_type} data from {len(organ_abbrev_set)} organs of" \
                            f" {len(donor_uuid_set)} different donors"
    return property_key, generated_title


"""
Trigger event method of getting the uuid of the previous revision dataset if exists

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_previous_revision_uuid(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_previous_revision_uuid()' trigger method.")

    logger.info(f"Executing 'get_previous_revision_uuid()' trigger method on uuid: {existing_data_dict['uuid']}")

    previous_revision_uuid = schema_neo4j_queries.get_previous_revision_uuid(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])
    
    return property_key, previous_revision_uuid


"""
Trigger event method of getting the uuids of the previous revision datasets if they exist

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: A list of the uuid strings of previous revision entity or an empty list if not found
"""
def get_previous_revision_uuids(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_previous_revision_uuid()' trigger method.")

    logger.info(f"Executing 'get_previous_revision_uuids()' trigger method on uuid: {existing_data_dict['uuid']}")

    previous_revision_uuids = schema_neo4j_queries.get_previous_revision_uuids(schema_manager.get_neo4j_driver_instance(),
                                                                             existing_data_dict['uuid'])

    return property_key, previous_revision_uuids


"""
Trigger event method of getting the uuid of the next version dataset if exists

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_next_revision_uuid(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_next_revision_uuid()' trigger method.")

    logger.info(f"Executing 'get_next_revision_uuid()' trigger method on uuid: {existing_data_dict['uuid']}")
    
    next_revision_uuid = schema_neo4j_queries.get_next_revision_uuid(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])
    
    return property_key, next_revision_uuid


"""
Trigger event method of generating `creation_action`

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The `creation_action` as string
"""
def get_creation_action_activity(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_creation_action_activity()' trigger method.")

    uuid: str = existing_data_dict['uuid']
    logger.info(f"Executing 'get_creation_action_activity()' trigger method on uuid: {uuid}")

    neo4j_driver_instance = schema_manager.get_neo4j_driver_instance()
    creation_action_activity =\
        schema_neo4j_queries.get_entity_creation_action_activity(neo4j_driver_instance, uuid)

    return property_key, creation_action_activity


"""
Trigger event method of getting the uuids of the next version dataset if they exist

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The list of uuid strings of next version entity or empty string if not found
"""
def get_next_revision_uuids(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError(
            "Missing 'uuid' key in 'existing_data_dict' during calling 'get_next_revision_uuid()' trigger method.")

    logger.info(f"Executing 'get_next_revision_uuid()' trigger method on uuid: {existing_data_dict['uuid']}")

    next_revision_uuids = schema_neo4j_queries.get_next_revision_uuids(schema_manager.get_neo4j_driver_instance(),
                                                                     existing_data_dict['uuid'])

    return property_key, next_revision_uuids


"""
Trigger event method to commit thumbnail file saved that were previously uploaded via ingest-api

The information, filename is saved in the field with name specified by `target_property_key`
in the provided data_dict.  The thumbnail file needed to be previously uploaded
using the temp file service.  The temp file id provided must be provided
in the field `thumbnail_file_to_add` in the data_dict for file being committed
in a JSON object like below: 

{"temp_file_id": "eiaja823jafd"}

Parameters
----------
property_key : str
    The property key for which the original trigger method is defined
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
generated_dict : dict 
    A dictionary that contains all final data

Returns
-------
dict: The updated generated dict
"""
def commit_thumbnail_file(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    # The name of the property where the file information is stored
    target_property_key = 'thumbnail_file'

    # Do nothing if no thumbnail file to add (missing or empty property)
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    try:
        if 'uuid' in new_data_dict:
            entity_uuid = new_data_dict['uuid']
        else:
            entity_uuid = existing_data_dict['uuid']

        # Commit the thumbnail file via ingest-api call
        ingest_api_target_url = schema_manager.get_ingest_api_url() + SchemaConstants.INGEST_API_FILE_COMMIT_ENDPOINT
        
        # Example: {"temp_file_id":"dzevgd6xjs4d5grmcp4n"}
        thumbnail_file_dict = new_data_dict[property_key]

        tmp_file_id = thumbnail_file_dict['temp_file_id']

        json_to_post = {
            'temp_file_id': tmp_file_id,
            'entity_uuid': entity_uuid,
            'user_token': user_token
        }

        logger.info(f"Commit the uploaded thumbnail file of tmp_file_id {tmp_file_id} for entity {entity_uuid} via ingest-api call...")

        request_headers = {
            'Authorization': f'Bearer {user_token}'
        }
        
        # Disable ssl certificate verification
        response = requests.post(url = ingest_api_target_url, headers = request_headers, json = json_to_post, verify = False) 

        if response.status_code != 200:
            msg = f"Failed to commit the thumbnail file of tmp_file_id {tmp_file_id} via ingest-api for entity uuid: {entity_uuid}"
            logger.error(msg)
            raise schema_errors.FileUploadException(msg)

        # Get back the file uuid dict
        file_uuid_info = response.json()

        # Update the target_property_key (`thumbnail_file`) to be saved in Neo4j
        generated_dict[target_property_key] = {
            'filename': file_uuid_info['filename'],
            'file_uuid': file_uuid_info['file_uuid']
        }
  
        return generated_dict
    except schema_errors.FileUploadException as e:
        raise
    except Exception as e:
        # No need to log
        raise


"""
Trigger event method for removing the thumbnail file from a dataset during update

File is stored in a json encoded text field with property name 'target_property_key' in the entity dict
The file to remove is specified as file uuid in the `property_key` field

Parameters
----------
property_key : str
    The property key for which the original trigger method is defined
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
generated_dict : dict 
    A dictionary that contains all final data

Returns
-------
dict: The updated generated dict
"""
def delete_thumbnail_file(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    # The name of the property where the file information is stored
    target_property_key = 'thumbnail_file'
    
    # Do nothing if no thumbnail file to delete 
    # is provided in the field specified by property_key
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    if 'uuid' not in existing_data_dict:
        raise KeyError(f"Missing 'uuid' key in 'existing_data_dict' during calling 'delete_thumbnail_file()' trigger method for property '{target_property_key}'.")

    entity_uuid = existing_data_dict['uuid']

    # The property_key (`thumbnail_file_to_remove`) is just a file uuid string
    file_uuid = new_data_dict[property_key]
    
    #If POST or PUT where the target doesn't exist create the file info dict
    #if generated_dict doesn't contain the property yet, copy it from the existing_data_dict
    #if it isn't in the existing_dictionary throw and error 
    #or if it doesn't exist in existing_data_dict create it
    if not target_property_key in generated_dict:
        if not target_property_key in existing_data_dict:
            raise KeyError(f"Missing '{target_property_key}' key missing during calling 'delete_thumbnail_file()' trigger method on entity {entity_uuid}.")
        # Otherwise this is a PUT where the target thumbnail file exists already
        else:
            logger.info(f"Executing convert_str_literal() on {normalized_type}.{target_property_key} of uuid: {entity_uuid} during calling 'delete_thumbnail_file()' trigger method.")

            # Note: The property, name specified by `target_property_key`, 
            # is stored in Neo4j as a string representation of the Python dict
            # It's not stored in Neo4j as a json string! And we can't store it as a json string 
            # due to the way that Cypher handles single/double quotes.
            file_info_dict = schema_manager.convert_str_literal(existing_data_dict[target_property_key])
    else:
        file_info_dict = generated_dict[target_property_key]
    
    # Remove the thumbnail file via ingest-api call
    ingest_api_target_url = schema_manager.get_ingest_api_url() + SchemaConstants.INGEST_API_FILE_REMOVE_ENDPOINT

    # ingest-api's /file-remove takes a list of files to remove
    # In this case, we only need to remove the single thumbnail file
    json_to_post = {
        'entity_uuid': entity_uuid,
        'file_uuids': [file_uuid],
        'files_info_list': [file_info_dict]
    }

    logger.debug(f"Remove the uploaded thumbnail file {file_uuid} for entity {entity_uuid} via ingest-api call...")

    request_headers = {
        'Authorization': f'Bearer {user_token}'
    }

    # Disable ssl certificate verification
    response = requests.post(url = ingest_api_target_url, headers = request_headers, json = json_to_post, verify = False) 

    # response.json() returns an empty array because
    # there's no thumbnail file left once the only one gets removed
    if response.status_code != 200:
        msg = f"Failed to remove the thumbnail file {file_uuid} via ingest-api for dataset uuid: {entity_uuid}"
        logger.error(msg)
        raise schema_errors.FileUploadException(msg)

    # Update the value of target_property_key `thumbnail_file` to empty json string 
    generated_dict[target_property_key] = {}

    return generated_dict


"""
Trigger event method that updates the status value of the target dataset
If the dataset is a parent Multi-Assay Split dataset, will also sync the status update
to all the child component datasets

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def update_status(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'update_status()' trigger method.")
    uuid = existing_data_dict['uuid']

    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'update_status()' trigger method.")
    status = existing_data_dict['status']

    set_status_history(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict)

    # Only apply to non-published parent datasets
    if status.lower() != 'published':
        # Only sync the child component datasets status for Multi-Assay Split
        component_dataset_uuids = schema_neo4j_queries.get_component_dataset_uuids(schema_manager.get_neo4j_driver_instance(), uuid)
        
        for comp_uuid in component_dataset_uuids:
            url = schema_manager.get_entity_api_url() + SchemaConstants.ENTITY_API_UPDATE_ENDPOINT + '/' + comp_uuid

            # When the parent dataset status update disables reindex via query string '?reindex=false'
            # We'll also disable the reindex call to search-api upon each subsequent child component dataset update
            reindex = 'followed'
            if schema_manager.suppress_reindex(request): 
                url += '?reindex=false'
                reindex = 'suppressed'

            logger.info(f"Update parent Multi-Assay Split dataset {uuid} status to {status}, with re-indexing {reindex}.")
            logger.info(f'Update child component dataset {comp_uuid} status to {status}, with re-indexing {reindex}.')

            request_headers = {
                'Authorization': f'Bearer {user_token}',
                SchemaConstants.HUBMAP_APP_HEADER: SchemaConstants.INGEST_API_APP,
                SchemaConstants.INTERNAL_TRIGGER: SchemaConstants.COMPONENT_DATASET
            }

            status_body = {"status": status}

            response = requests.put(url=url, headers=request_headers, json=status_body)

            if response.status_code != 200:
                logger.error(f'Failed to update child component dataset {comp_uuid} status: {response.text}')


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
    One of the types defined in the schema yaml: Donor
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_donor_to_lab(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_donor_to_lab()' trigger method.")

    if 'group_uuid' not in existing_data_dict:
        raise KeyError("Missing 'group_uuid' key in 'existing_data_dict' during calling 'link_donor_to_lab()' trigger method.")

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = [existing_data_dict['group_uuid']]

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(normalized_type, request, user_token, existing_data_dict)

    try:
        # Create a linkage (via Activity node) 
        # between the Donor node and the parent Lab node in neo4j
        schema_neo4j_queries.link_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], direct_ancestor_uuids, activity_data_dict)
    
        # No need to delete any cache here since this is one-time donor creation
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
request: Flask request object
    The instance of Flask request passed in from application request
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
def commit_metadata_files(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    return _commit_files('metadata_files', property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict)


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
request: Flask request object
    The instance of Flask request passed in from application request
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
def delete_metadata_files(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    return _delete_files('metadata_files', property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict)
    

"""
Trigger event method of creating or recreating linkages between this new Sample and its direct ancestor

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Sample
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_sample_to_direct_ancestor(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    if 'direct_ancestor_uuid' not in new_data_dict:
        raise KeyError("Missing 'direct_ancestor_uuid' key in 'new_data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    sample_uuid = existing_data_dict['uuid']

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = [new_data_dict['direct_ancestor_uuid']]

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(normalized_type, request, user_token, existing_data_dict)

    try:
        # Create a linkage (via Activity node) 
        # between the Sample node and the source entity node in neo4j
        schema_neo4j_queries.link_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), sample_uuid, direct_ancestor_uuids, activity_data_dict)
    
        # Delete the cache of sample if any cache exists
        # Because the `Sample.direct_ancestor` field can be updated
        schema_manager.delete_memcached_cache([sample_uuid])
    except TransactionError:
        # No need to log
        raise

"""
Trigger event method of creating or recreating linkages between this new publication and its associated_collection

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Publication
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_publication_to_associated_collection(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_publication_to_associated_collection()' trigger method.")

    if 'associated_collection_uuid' not in new_data_dict:
        raise KeyError("Missing 'associated_collection_uuid' key in 'new_data_dict' during calling 'link_publication_to_associated_collection()' trigger method.")

    associated_collection_uuid = new_data_dict['associated_collection_uuid']

    # No activity node. We are creating a direct link to the associated collection

    try:
        # Create a linkage
        # between the Publication node and the Collection node in neo4j
        schema_neo4j_queries.link_publication_to_associated_collection(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], associated_collection_uuid)
    
        # Will need to delete the collection cache if later we add `Collection.associated_publications` field - 7/16/2023 Zhou
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of getting the parent of a Sample, which is a Donor

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Sample
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_sample_direct_ancestor(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_sample_direct_ancestor()' trigger method.")
    
    logger.info(f"Executing 'get_sample_direct_ancestor()' trigger method on uuid: {existing_data_dict['uuid']}")

    direct_ancestor_dict = schema_neo4j_queries.get_sample_direct_ancestor(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entity_result_for_response(direct_ancestor_dict)



####################################################################################################
## Trigger methods specific to Publication - DO NOT RENAME
####################################################################################################

"""
Trigger event method of truncating the time part of publication_date if provided by users

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Publication
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The date part YYYY-MM-DD of ISO 8601
"""
def set_publication_date(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    # We only store the date part 'YYYY-MM-DD', base on the ISO 8601 format, it's fine if the user entered the time part
    date_obj = datetime.fromisoformat(new_data_dict[property_key])

    return property_key, date_obj.date().isoformat()


"""
Trigger event method setting the dataset_type immutable property for a Publication.

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Publication
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: Immutable dataset_type of "Publication"
"""
def set_publication_dataset_type(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    # Count upon the dataset_type generated: true property in provenance_schema.yaml to assure the
    # request does not contain a value which will be overwritten.
    return property_key, 'Publication'

####################################################################################################
## Trigger methods specific to Upload - DO NOT RENAME
####################################################################################################

"""
Trigger event method of setting the Upload initial status - "New"

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Upload
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The "New" status
"""
def set_upload_status_new(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    return property_key, 'New'


"""
Trigger event method of building linkage between this new Upload and Lab
Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Upload
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_upload_to_lab(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_upload_to_lab()' trigger method.")

    if 'group_uuid' not in existing_data_dict:
        raise KeyError("Missing 'group_uuid' key in 'existing_data_dict' during calling 'link_upload_to_lab()' trigger method.")

    # Build a list of direct ancestor uuids
    # Only one uuid in the list in this case
    direct_ancestor_uuids = [existing_data_dict['group_uuid']]

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(normalized_type, request, user_token, existing_data_dict)

    try:
        # Create a linkage (via Activity node) 
        # between the Submission node and the parent Lab node in neo4j
        schema_neo4j_queries.link_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'], direct_ancestor_uuids, activity_data_dict)
    
        # No need to delete any cache here since this is one-time upload creation
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of building linkages between this Submission and the given datasets

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Upload
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def link_datasets_to_upload(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'link_datasets_to_upload()' trigger method.")

    if 'dataset_uuids_to_link' not in new_data_dict:
        raise KeyError("Missing 'dataset_uuids_to_link' key in 'new_data_dict' during calling 'link_datasets_to_upload()' trigger method.")

    upload_uuid = existing_data_dict['uuid']
    dataset_uuids = new_data_dict['dataset_uuids_to_link']

    try:
        # Create a direct linkage (Dataset) - [:IN_UPLOAD] -> (Submission) for each dataset
        schema_neo4j_queries.link_datasets_to_upload(schema_manager.get_neo4j_driver_instance(), upload_uuid, dataset_uuids)
    
        # Delete the cache of each associated dataset and the target upload if any cache exists
        # Because the `Dataset.upload` and `Upload.datasets` fields, and 
        uuids_list = [upload_uuid] + dataset_uuids
        schema_manager.delete_memcached_cache(uuids_list)
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of deleting linkages between this target Submission and the given datasets

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Upload
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
"""
def unlink_datasets_from_upload(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'unlink_datasets_from_upload()' trigger method.")

    if 'dataset_uuids_to_unlink' not in new_data_dict:
        raise KeyError("Missing 'dataset_uuids_to_unlink' key in 'new_data_dict' during calling 'unlink_datasets_from_upload()' trigger method.")

    upload_uuid = existing_data_dict['uuid']
    dataset_uuids = new_data_dict['dataset_uuids_to_unlink']

    try:
        # Delete the linkage (Dataset) - [:IN_UPLOAD] -> (Upload) for each dataset
        schema_neo4j_queries.unlink_datasets_from_upload(schema_manager.get_neo4j_driver_instance(), upload_uuid, dataset_uuids)
    
        # Delete the cache of each associated dataset and the upload itself if any cache exists
        # Because the associated datasets have this `Dataset.upload` field and Upload has `Upload.datasets` field
        uuids_list = dataset_uuids + [upload_uuid]
        schema_manager.delete_memcached_cache(uuids_list)
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of getting a list of associated datasets for a given Upload

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Upload
request: Flask request object
    The instance of Flask request passed in from application request
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
def get_upload_datasets(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'uuid' not in existing_data_dict:
        raise KeyError("Missing 'uuid' key in 'existing_data_dict' during calling 'get_upload_datasets()' trigger method.")

    logger.info(f"Executing 'get_upload_datasets()' trigger method on uuid: {existing_data_dict['uuid']}")

    datasets_list = schema_neo4j_queries.get_upload_datasets(schema_manager.get_neo4j_driver_instance(), existing_data_dict['uuid'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return property_key, schema_manager.normalize_entities_list_for_response(datasets_list)


####################################################################################################
## Trigger methods specific to Activity - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting creation_action for Activity

Lab->Activity->Donor (Not needed for now)
Lab->Activity->Submission
Donor->Activity->Sample
Sample->Activity->Sample
Sample->Activity->Dataset
Dataset->Activity->Dataset

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity
request: Flask request object
    The instance of Flask request passed in from application request
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
def set_activity_creation_action(property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict):
    if 'normalized_entity_type' not in new_data_dict:
        raise KeyError("Missing 'normalized_entity_type' key in 'existing_data_dict' during calling 'set_activity_creation_action()' trigger method.")
    if new_data_dict and new_data_dict.get('creation_action'):
        return property_key, new_data_dict['creation_action'].title()
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
    The name of the property where the file information is stored
property_key : str
    The property key for which the original trigger method is defined
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
generated_dict : dict 
    A dictionary that contains all final data

Returns
-------
dict: The updated generated dict
"""
def _commit_files(target_property_key, property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    # Do nothing if no files to add are provided (missing or empty property)
    # For image files the property name is "image_files_to_add"
    # For metadata files the property name is "metadata_files_to_add"
    # But other may be used in the future
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    #If POST or PUT where the target doesn't exist create the file info array
    #if generated_dict doesn't contain the property yet, copy it from the existing_data_dict 
    #or if it doesn't exist in existing_data_dict create it
    if not target_property_key in generated_dict:
        if not target_property_key in existing_data_dict:
            files_info_list = []
        # Otherwise this is a PUT where the target array exists already
        else:
            logger.info(f"Executing convert_str_literal() during calling internal trigger method: '_commit_files()'")

            # Note: The property, name specified by `target_property_key`, is stored in Neo4j as a string representation of the Python list
            # It's not stored in Neo4j as a json string! And we can't store it as a json string 
            # due to the way that Cypher handles single/double quotes.
            files_info_list = schema_manager.convert_str_literal(existing_data_dict[target_property_key])
    else:
        files_info_list = generated_dict[target_property_key]

    try:
        if 'uuid' in new_data_dict:
            entity_uuid = new_data_dict['uuid']
        else:
            entity_uuid = existing_data_dict['uuid']

        # Commit the files via ingest-api call
        ingest_api_target_url = schema_manager.get_ingest_api_url() + SchemaConstants.INGEST_API_FILE_COMMIT_ENDPOINT

        for file_info in new_data_dict[property_key]:
            temp_file_id = file_info['temp_file_id']

            json_to_post = {
                'temp_file_id': temp_file_id,
                'entity_uuid': entity_uuid,
                'user_token': user_token
            }

            logger.debug(f"Commit the uploaded file of temp_file_id {temp_file_id} for entity {entity_uuid} via ingest-api call...")

            request_headers = {
                'Authorization': f'Bearer {user_token}'
            }

            # Disable ssl certificate verification
            response = requests.post(url = ingest_api_target_url, headers = request_headers, json = json_to_post, verify = False) 
    
            if response.status_code != 200:
                msg = f"Failed to commit the file of temp_file_id {temp_file_id} via ingest-api for entity uuid: {entity_uuid}"
                logger.error(msg)
                raise schema_errors.FileUploadException(msg)

            # Get back the file uuid dict
            file_uuid_info = response.json()

            file_info_to_add = {
                'filename': file_uuid_info['filename'],
                'file_uuid': file_uuid_info['file_uuid']
            }
            
            # The `description` is optional
            if 'description' in file_info:
                file_info_to_add['description'] = file_info['description']    
            
            # Add to list
            files_info_list.append(file_info_to_add)

            # Update the target_property_key value
            generated_dict[target_property_key] = files_info_list
            
        return generated_dict
    except schema_errors.FileUploadException as e:
        raise
    except Exception as e:
        # No need to log
        raise


"""
Trigger event method for removing files from an entity during update

Files are stored in a json encoded text field with property name 'target_property_key' in the entity dict
The files to remove are specified as file uuids in the `property_key` field

The two outer methods (delete_image_files and delete_metadata_files) pass the target property
field name to private method, _delete_files along with the other required trigger properties

Parameters
----------
target_property_key : str
    The name of the property where the file information is stored
property_key : str
    The property key for which the original trigger method is defined
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample
request: Flask request object
    The instance of Flask request passed in from application request
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used
generated_dict : dict 
    A dictionary that contains all final data

Returns
-------
dict: The updated generated dict
"""
def _delete_files(target_property_key, property_key, normalized_type, request, user_token, existing_data_dict, new_data_dict, generated_dict):
    #do nothing if no files to delete are provided in the field specified by property_key
    if (not property_key in new_data_dict) or (not new_data_dict[property_key]):
        return generated_dict

    if 'uuid' not in existing_data_dict:
        raise KeyError(f"Missing 'uuid' key in 'existing_data_dict' during calling '_delete_files()' trigger method for property '{target_property_key}'.")

    entity_uuid = existing_data_dict['uuid']
    
    #If POST or PUT where the target doesn't exist create the file info array
    #if generated_dict doesn't contain the property yet, copy it from the existing_data_dict
    #if it isn't in the existing_dictionary throw and error 
    #or if it doesn't exist in existing_data_dict create it
    if not target_property_key in generated_dict:
        if not target_property_key in existing_data_dict:
            raise KeyError(f"Missing '{target_property_key}' key missing during calling '_delete_files()' trigger method on entity {entity_uuid}.")
        # Otherwise this is a PUT where the target array exists already
        else:
            logger.info(f"Executing convert_str_literal() on {normalized_type}.{target_property_key} of uuid: {entity_uuid} during calling internal  trigger method: '_delete_files()'")

            # Note: The property, name specified by `target_property_key`, is stored in Neo4j as a string representation of the Python list
            # It's not stored in Neo4j as a json string! And we can't store it as a json string 
            # due to the way that Cypher handles single/double quotes.
            files_info_list = schema_manager.convert_str_literal(existing_data_dict[target_property_key])
    else:
        files_info_list = generated_dict[target_property_key]
    
    file_uuids = []
    for file_uuid in new_data_dict[property_key]:
        file_uuids.append(file_uuid)

    # Remove the files via ingest-api call
    ingest_api_target_url = schema_manager.get_ingest_api_url() + SchemaConstants.INGEST_API_FILE_REMOVE_ENDPOINT

    json_to_post = {
        'entity_uuid': entity_uuid,
        'file_uuids': file_uuids,
        'files_info_list': files_info_list
    }

    logger.debug(f"Remove the uploaded files for entity {entity_uuid} via ingest-api call...")
    
    request_headers = {
        'Authorization': f'Bearer {user_token}'
    }

    # Disable ssl certificate verification
    response = requests.post(url = ingest_api_target_url, headers = request_headers, json = json_to_post, verify = False) 

    if response.status_code != 200:
        msg = f"Failed to remove the files via ingest-api for entity uuid: {entity_uuid}"
        logger.error(msg)
        raise schema_errors.FileUploadException(msg)

    files_info_list = response.json()

    # Update the target_property_key value to be saved in Neo4j
    generated_dict[target_property_key] = files_info_list

    return generated_dict

