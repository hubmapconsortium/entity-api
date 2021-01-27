import json
import logging
import datetime
from neo4j.exceptions import TransactionError

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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
int: A timestamp integer of seconds
"""
def set_timestamp(property_key, normalized_type, data_dict):
    current_time = datetime.datetime.now() 
    seconds = int(current_time.timestamp())
    return property_key, seconds

"""
Trigger event method of setting the entity type of a given entity

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The string of normalized entity type
"""
def set_entity_type(property_key, normalized_type, data_dict):
    return property_key, normalized_type


"""
Trigger event method of getting user sub

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The 'sub' string
"""
def set_user_sub(property_key, normalized_type, data_dict):
    if 'sub' not in data_dict:
        raise KeyError("Missing 'sub' key in 'data_dict' during calling 'set_user_sub()' trigger method.")
    return property_key, data_dict['sub']

"""
Trigger event method of getting user email

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The 'email' string
"""
def set_user_email(property_key, normalized_type, data_dict):
    if 'email' not in data_dict:
        raise KeyError("Missing 'email' key in 'data_dict' during calling 'set_user_email()' trigger method.")
    return property_key, data_dict['email']

"""
Trigger event method of getting user name

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The 'name' string
"""
def set_user_displayname(property_key, normalized_type, data_dict):
    if 'name' not in data_dict:
        raise KeyError("Missing 'name' key in 'data_dict' during calling 'set_user_displayname()' trigger method.")
    
    return property_key, data_dict['name']

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The uuid created via uuid-api
"""
def set_uuid(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'set_uuid()' trigger method.")
    
    return property_key, data_dict['uuid']

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The hubmap_id created via uuid-api
"""
def set_hubmap_id(property_key, normalized_type, data_dict):
    if 'hubmap_id' not in data_dict:
        raise KeyError("Missing 'hubmap_id' key in 'data_dict' during calling 'set_hubmap_id()' trigger method.")
    
    return property_key, data_dict['hubmap_id']


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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The data access level string
"""
def set_data_access_level(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'set_data_access_level()' trigger method.")

    # For now, don't use the constants from commons
    # All lowercase for easy comparision
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'
    
    if normalized_type == 'Dataset':
        # Default to protected
        data_access_level = ACCESS_LEVEL_PROTECTED

        # When `contains_human_genetic_sequences` is true, even if `status` is 'Published', 
        # the `data_access_level` is still 'protected'
        if data_dict['contains_human_genetic_sequences']:
            data_access_level = ACCESS_LEVEL_PROTECTED
        else:
            if data_dict['status'].lower() == 'published':
                data_access_level = ACCESS_LEVEL_PUBLIC
            else:
                data_access_level = ACCESS_LEVEL_CONSORTIUM
    else:
        # Default to consortium for Donor/Sample
        data_access_level = ACCESS_LEVEL_CONSORTIUM
        
        # public if any dataset below it in the provenance hierarchy is published
        # (i.e. Dataset.status == "Published")
        count = schema_neo4j_queries.count_attached_published_datasets(schema_manager.get_neo4j_driver_instance(), normalized_type, data_dict['uuid'])

        if count > 0:
            data_access_level = ACCESS_LEVEL_PUBLIC

    return property_key, data_access_level


"""
Trigger event method of getting the group_uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The group uuid
"""
def set_group_uuid(property_key, normalized_type, data_dict):
    # Use the user input if `group_uuid` is set
    if 'group_uuid' in data_dict:
        # Validate the group_uuid and make sure it's one of the valid data providers
        try:
            schema_manager.validate_entity_group_uuid(data_dict['group_uuid'])
        except schema_errors.NoDataProviderGroupException as e:
            # No need to log
            raise schema_errors.NoDataProviderGroupException(e)

        return data_dict['group_uuid']

    # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that. 
    # Otherwise if not set and no single "provider group" membership throws error.  
    # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
    if 'hmgroupids' not in data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'data_dict' during calling 'set_group_uuid()' trigger method.")

    try:
        group_info = schema_manager.get_entity_group_info(data_dict['hmgroupids'])
        return property_key, group_info['uuid']
    except schema_errors.NoDataProviderGroupException as e:
        # No need to log
        raise schema_errors.NoDataProviderGroupException(e)
    except schema_errors.MultipleDataProviderGroupException as e:
        # No need to log
        raise schema_errors.MultipleDataProviderGroupException(e)

"""
Trigger event method of getting the group_name

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The group name
"""
def set_group_name(property_key, normalized_type, data_dict):
    # Use the user input if `group_name` is set
    if 'group_name' in data_dict:
        return data_dict['group_name']

    # Get the `group_name` based on the provided `group_uuid`
    # when `group_name` is not provided but `group_uuid` is provided
    if ('group_name' not in data_dict) and ('group_uuid' in data_dict):
        # Validate the group_uuid and make sure it's one of the valid data providers
        try:
            schema_manager.validate_entity_group_uuid(data_dict['group_uuid'])
        except schema_errors.NoDataProviderGroupException as e:
            # No need to log
            raise schema_errors.NoDataProviderGroupException(e)

        return schema_manager.get_entity_group_name(data_dict['group_uuid'])

    # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that. 
    # Otherwise if not set and no single "provider group" membership throws error.  
    # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
    if 'hmgroupids' not in data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'data_dict' during calling 'set_group_name()' trigger method.")
    
    try:
        group_info = schema_manager.get_entity_group_info(data_dict['hmgroupids'])
        return property_key, group_info['name']
    except schema_errors.NoDataProviderGroupException as e:
        # No need to log
        raise schema_errors.NoDataProviderGroupException(e)
    except schema_errors.MultipleDataProviderGroupException as e:
        # No need to log
        raise schema_errors.MultipleDataProviderGroupException(e)
    

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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The submission_id
"""
def set_submission_id(property_key, normalized_type, data_dict):
    if 'submission_id' not in data_dict:
        raise KeyError("Missing 'submission_id' key in 'data_dict' during calling 'set_submission_id()' trigger method.")
    
    return property_key, data_dict['submission_id']


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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
list: A list of associated dataset dicts with all the normalized information
"""
def get_collection_datasets(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_collection_datasets()' trigger method.")

    datasets_list = schema_neo4j_queries.get_collection_datasets(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'])

    # Additional properties of the datasets to exclude 
    # We don't want to show too much nested information
    properties_to_skip = ['direct_ancestors', 'collections']
    complete_entities_list = schema_manager.get_complete_entities_list(datasets_list, properties_to_skip)

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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: Initial status of "New"
"""
def set_dataset_status(property_key, normalized_type, data_dict):
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data
"""
def update_dataset_and_ancestors_data_access_level(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'update_dataset_ancestors_data_access_level()' trigger method.")

    if 'status' not in data_dict:
        raise KeyError("Missing 'status' key in 'data_dict' during calling 'update_dataset_ancestors_data_access_level()' trigger method.")

    # Caculate the new data_access_level of this dataset's ancestors (except another dataset is the ancestor)
    # public if any dataset below the Donor/Sample in the provenance hierarchy is published
    ACCESS_LEVEL_PUBLIC = 'public'

    if data_dict['status'].lower() == "published":
        try:
            schema_neo4j_queries.update_dataset_and_ancestors_data_access_level(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'], ACCESS_LEVEL_PUBLIC)
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
list: A list of associated collections with all the normalized information
"""
def get_dataset_collections(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_dataset_collections()' trigger method.")

    # No property key needs to filter the result
    # Get back the list of collection dicts
    collections_list = schema_neo4j_queries.get_dataset_collections(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'])

    # Exclude datasets from each resulting collection
    # We don't want to show too much nested information
    properties_to_skip = ['datasets']
    complete_entities_list = schema_manager.get_complete_entities_list(collections_list, properties_to_skip)

    return property_key, schema_manager.normalize_entities_list_for_response(complete_entities_list)

"""
Trigger event method of building linkages between this new Dataset and its direct ancestors

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The uuid string of source entity
"""
def link_dataset_to_direct_ancestors(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    if 'direct_ancestor_uuids' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuids' key in 'data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    # For each source entity, create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    for direct_ancestor_uuid in data_dict['direct_ancestor_uuids']:
        # Activity is not an Entity
        normalized_activity_type = 'Activity'

        # Target entity type dict
        # Will be used when calling set_activity_creation_action() trigger method
        normalized_entity_type_dict = {'normalized_entity_type': normalized_type}

        # Create new ids for the new Activity
        new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_type, json_data_dict = None, user_info_dict = None)

        # The `data_dict` should already have user_info
        data_dict_for_activity = {**data_dict, **normalized_entity_type_dict, **new_ids_dict_for_activity}

        # Generate property values for Activity
        generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_type, data_dict_for_activity)

        # `UNWIND` in Cypher expects List<T>
        activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

        # Convert the list (only contains one entity) to json list string
        activity_json_list_str = json.dumps(activity_data_list)

        logger.debug("======link_dataset_to_direct_ancestors() create activity with activity_json_list_str======")
        logger.debug(activity_json_list_str)

        try:
            schema_neo4j_queries.link_entity_to_direct_ancestor(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
        except TransactionError:
            # No need to log
            raise


"""
Trigger event method of rebuilding linkages between Dataset and its direct ancestors

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The uuid string of source entity
"""
def relink_dataset_to_direct_ancestors(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'relink_dataset_to_direct_ancestors()' trigger method.")

    if 'direct_ancestor_uuids' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuids' key in 'data_dict' during calling 'relink_dataset_to_direct_ancestors()' trigger method.")

    # Delete old linkages before recreating new ones
    try:
        schema_neo4j_queries.unlink_entity_to_direct_ancestors(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'])
    except TransactionError:
        # No need to log
        raise

    # For each source entity, create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    for direct_ancestor_uuid in data_dict['direct_ancestor_uuids']:
        # Activity is not an Entity
        normalized_activity_type = 'Activity'

        # Target entity type dict
        # Will be used when calling set_activity_creation_action() trigger method
        normalized_entity_type_dict = {'normalized_entity_type': normalized_type}

        # Create new ids for the new Activity
        new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_type, json_data_dict = None, user_info_dict = None)

        # The `data_dict` should already have user_info
        data_dict_for_activity = {**data_dict, **normalized_entity_type_dict, **new_ids_dict_for_activity}

        # Generate property values for Activity
        generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_type, data_dict_for_activity)

        # `UNWIND` in Cypher expects List<T>
        activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

        # Convert the list (only contains one entity) to json list string
        activity_json_list_str = json.dumps(activity_data_list)

        logger.debug("======relink_dataset_to_direct_ancestors() create activity with activity_json_list_str======")
        logger.debug(activity_json_list_str)

        try:
            schema_neo4j_queries.link_entity_to_direct_ancestor(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
list: A list of associated direct ancestors with all the normalized information
"""
def get_dataset_direct_ancestors(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_dataset_direct_ancestors()' trigger method.")

    # No property key needs to filter the result
    # Get back the list of ancestor dicts
    direct_ancestors_list = schema_neo4j_queries.get_dataset_direct_ancestors(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'])

    # We don't want to show too much nested information
    # The direct ancestor of a Dataset could be: Dataset or Sample
    # Skip running the trigger methods for 'direct_ancestors' and 'collections' if the direct ancestor is Dataset
    # Skip running the trigger methods for 'direct_ancestor' if the direct ancestor is Sample
    properties_to_skip = ['direct_ancestors', 'collections', 'direct_ancestor']
    complete_entities_list = schema_manager.get_complete_entities_list(direct_ancestors_list, properties_to_skip)

    return property_key, schema_manager.normalize_entities_list_for_response(complete_entities_list)


"""
Trigger event method of getting the relative directory path of a given dataset

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The relative directory path
"""
def get_local_directory_rel_path(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_local_directory_rel_path()' trigger method.")
    
    if 'data_access_level' not in data_dict:
        raise KeyError("Missing 'data_access_level' key in 'data_dict' during calling 'get_local_directory_rel_path()' trigger method.")
    
    uuid = data_dict['uuid']

    if (not 'group_uuid' in data_dict) or (not data_dict['group_uuid']):
        raise KeyError(f"Group uuid not set for dataset with uuid: {uuid}")

    # Validate the group_uuid and make sure it's one of the valid data providers
    try:
        schema_manager.validate_entity_group_uuid(data_dict['group_uuid'])
    except schema_errors.NoDataProviderGroupException as e:
        # No need to log
        raise schema_errors.NoDataProviderGroupException(e)

    group_name = schema_manager.get_entity_group_name(data_dict['group_uuid'])

    dir_path = data_dict['data_access_level'] + "/" + group_name + "/" + uuid + "/"

    return property_key, dir_path


####################################################################################################
## Trigger methods specific to Donor - DO NOT RENAME
####################################################################################################


"""
Trigger event method to ONLY update descriptions of existing image files

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
list: The file info dicts (with updated descriptions) in a list
"""
def update_image_files_descriptions(property_key, normalized_type, data_dict):
    if property_key not in data_dict:
        raise KeyError(f"Missing '{property_key}' key in 'data_dict' during calling 'delete_image_files()' trigger method.")

    # TODO 

    return property_key, data_dict[property_key]


"""
Trigger event method to commit image files save that were previously uploaded with UploadFileHelper.save_file

The information, filename and optional description is saved in the image_files field 
in the provided data_dict.  The image files needed to be previously uploaded
using the temp file service (UploadFileHelper.save_file).  The temp file id provided
from UploadFileHelper, paired with an optional description of the file must be provided
in the field `image_files_to_add` in the data_dict for each file being committed
in a JSON array like below (image "description" is optional): 

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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def commit_image_files(property_key, normalized_type, data_dict):
    target_property_key = 'image_files'

    if property_key not in data_dict:
        raise KeyError(f"Missing '{property_key}' key in 'data_dict' during calling 'delete_image_files()' trigger method.")

    files_info_list = []

    try: 
        for file_info in data_dict[property_key]:
            filename = schema_manager.get_file_upload_helper_instance().commit_file(file_info['temp_file_id'], data_dict['uuid'])
            
            file_info_to_add = {
                'filename': filename
            }
            
            # The `description` is optional
            if 'description' in file_info:
                # Note: it'll break the neo4j query if description contains single quotes
                file_info_to_add['description'] = file_info['description']
            
            # Add to list
            files_info_list.append(file_info_to_add)
        
        # Assign the target value to a different property key rather than itself
        return target_property_key, files_info_list
    except Exception as e:
        # No need to log
        raise


"""
Trigger event method of removing image files from an entity during update

Image files are stored in a json encoded text field named `image_files` in the entity dict
The images to remove are specified as filenames in the `image_files_to_remove` field

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data
    In this case, the following properties are required:
        - uuid
        - image_files
        - image_files_to_remove

Returns
-------
str: The target property key
list: The file info dicts in a list
"""
def delete_image_files(property_key, normalized_type, data_dict):
    target_property_key = 'image_files'
    image_files_to_delete_property = 'image_files_to_delete'

    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'delete_image_files()' trigger method.")
    
    if target_property_key not in data_dict:
        raise KeyError(f"Missing '{target_property_key}' key in 'data_dict' during calling 'delete_image_files()' trigger method.")
    
    if image_files_to_delete_property not in data_dict:
        raise KeyError(f"Missing '{image_files_to_delete_property}' key in 'data_dict' during calling 'delete_image_files()' trigger method.")
    
    try:
        entity_uuid = data_dict['uuid']
        # `upload_dir` is already normalized with trailing slash
        entity_upload_dir = schema_manager.get_file_upload_helper_instance().upload_dir + entity_uuid + os.sep
        files_info_list = json.loads(data_dict[target_property_key])
        
        # Remove physical files from the file system
        for filename in data_dict[image_files_to_delete_property]:
            files_info_list = schema_manager.get_file_upload_helper_instance().remove_file(entity_upload_dir, filename, files_info_list)
        
        # Assign the target value to a different property key rather than itself
        return target_property_key, files_info_list
    except Exception as e:
        # No need to log
        raise


####################################################################################################
## Trigger methods specific to Sample - DO NOT RENAME
####################################################################################################

"""
Trigger event method of building linkages between this new Sample and its ancestor

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data
"""
def link_sample_to_direct_ancestor(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    if 'direct_ancestor_uuid' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuid' key in 'data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    # Create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    # Activity is not an Entity
    normalized_activity_type = 'Activity'

    # Target entity type dict
    # Will be used when calling set_activity_creation_action() trigger method
    normalized_entity_type_dict = {'normalized_entity_type': normalized_type}

    # Create new ids for the new Activity
    new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_type, json_data_dict = None, user_info_dict = None)

    # The `data_dict` should already have user_info
    data_dict_for_activity = {**data_dict, **normalized_entity_type_dict, **new_ids_dict_for_activity}
    
    # Generate property values for Activity
    generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_type, data_dict_for_activity)

    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    logger.debug("======link_sample_to_direct_ancestor() create activity with activity_json_list_str======")
    logger.debug(activity_json_list_str)

    try:
        schema_neo4j_queries.link_entity_to_direct_ancestor(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of rebuilding linkages between this Sample and its direct ancestors 

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data
"""
def relink_sample_to_direct_ancestor(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'relink_sample_to_direct_ancestor()' trigger method.")

    if 'direct_ancestor_uuid' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuid' key in 'data_dict' during calling 'relink_sample_to_direct_ancestor()' trigger method.")

    # Create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    # Activity is not an Entity
    normalized_activity_type = 'Activity'

    # Target entity type dict
    # Will be used when calling set_activity_creation_action() trigger method
    normalized_entity_type_dict = {'normalized_entity_type': normalized_type}

    # Create new ids for the new Activity
    new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_type, json_data_dict = None, user_info_dict = None)

    # The `data_dict` should already have user_info
    data_dict_for_activity = {**data_dict, **normalized_entity_type_dict, **new_ids_dict_for_activity}
    
    # Generate property values for Activity
    generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_type, data_dict_for_activity)

    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    logger.debug("======relink_sample_to_direct_ancestor() create activity with activity_json_list_str======")
    logger.debug(activity_json_list_str)
 
    try:
        schema_neo4j_queries.link_entity_to_direct_ancestor(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
dict: The direct ancestor entity (either another Sample or a Donor) with all the normalized information
"""
def get_sample_direct_ancestor(property_key, normalized_type, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_sample_direct_ancestor()' trigger method.")

    direct_ancestor_dict = schema_neo4j_queries.get_sample_direct_ancestor(schema_manager.get_neo4j_driver_instance(), data_dict['uuid'])

    if 'entity_type' not in direct_ancestor_dict:
        raise KeyError("The 'entity_type' property in the resulting 'direct_ancestor_dict' is not set during calling 'get_sample_direct_ancestor()' trigger method.")

    # Generate trigger data for sample's direct_ancestor and skip the direct_ancestor's direct_ancestor
    properties_to_skip = ['direct_ancestor']
    complete_dict = schema_manager.get_complete_entity_result(direct_ancestor_dict, properties_to_skip)

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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str: The target property key
str: The creation_action string
"""
def set_activity_creation_action(property_key, normalized_type, data_dict):
    if 'normalized_entity_type' not in data_dict:
        raise KeyError("Missing 'normalized_entity_type' key in 'data_dict' during calling 'set_activity_creation_action()' trigger method.")
    
    return property_key, f"Create {normalized_entity_type} Activity"

