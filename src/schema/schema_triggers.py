import json
import logging
import datetime
from neo4j.exceptions import TransactionError

# Local modules
from schema import schema_manager
from schema import schema_neo4j_queries

# HuBMAP commons
from hubmap_commons import globus_groups

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
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
int
    A timestamp integer of seconds
"""
def set_timestamp(property_key, normalized_class, neo4j_driver, data_dict):
    current_time = datetime.datetime.now() 
    seconds = int(current_time.timestamp())
    return seconds

"""
Trigger event method of setting the entity class of a given entity

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The string of normalized entity class
"""
def set_entity_class(property_key, normalized_class, neo4j_driver, data_dict):
    return normalized_class

"""
Trigger event method of getting data access level

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the entity classes defined in the schema yaml: Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The data access level string
"""
def get_data_access_level(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_data_access_level()' trigger method.")

    # For now, don't use the constants from commons
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'
    
    if normalized_class == 'Dataset':
        # Default to protected
        data_access_level = ACCESS_LEVEL_PROTECTED

        if data_dict['contains_human_genetic_sequences']:
            data_access_level = ACCESS_LEVEL_PROTECTED
        else:
            if data_dict['status'] == 'Published':
                data_access_level = ACCESS_LEVEL_PUBLIC
            else:
                data_access_level = ACCESS_LEVEL_CONSORTIUM
    else:
        # Default to consortium for Collection/Donor/Sample
        data_access_level = ACCESS_LEVEL_CONSORTIUM
        
        # public if any dataset below it in the provenance hierarchy is published
        # (i.e. Dataset.status == "Published")
        count = schema_neo4j_queries.count_attached_published_datasets(neo4j_driver, normalized_class, data_dict['uuid'])

        if count > 0:
            data_access_level = ACCESS_LEVEL_PUBLIC

    return data_access_level

"""
Trigger event method of getting user sub

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'sub' string
"""
def set_user_sub(property_key, normalized_class, neo4j_driver, data_dict):
    if 'sub' not in data_dict:
        raise KeyError("Missing 'sub' key in 'data_dict' during calling 'set_user_sub()' trigger method.")
    return data_dict['sub']

"""
Trigger event method of getting user email

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'email' string
"""
def set_user_email(property_key, normalized_class, neo4j_driver, data_dict):
    if 'email' not in data_dict:
        raise KeyError("Missing 'email' key in 'data_dict' during calling 'set_user_email()' trigger method.")
    return data_dict['email']

"""
Trigger event method of getting user name

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'name' string
"""
def set_user_displayname(property_key, normalized_class, neo4j_driver, data_dict):
    if 'name' not in data_dict:
        raise KeyError("Missing 'name' key in 'data_dict' during calling 'set_user_displayname()' trigger method.")
    return data_dict['name']

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid created via uuid-api
"""
def set_uuid(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'set_uuid()' trigger method.")
    return data_dict['uuid']

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The hubmap_id created via uuid-api
"""
def set_hubmap_id(property_key, normalized_class, neo4j_driver, data_dict):
    if 'hubmap_id' not in data_dict:
        raise KeyError("Missing 'hubmap_id' key in 'data_dict' during calling 'set_hubmap_id()' trigger method.")
    return data_dict['hubmap_id']


####################################################################################################
## Trigger methods shared by Sample, Donor, Dataset - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting the group_uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The group uuid
"""
def set_group_uuid(property_key, normalized_class, neo4j_driver, data_dict):
    if 'hmgroupids' not in data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'data_dict' during calling 'set_group_uuid()' trigger method.")

    try:
        group_info = schema_manager.get_entity_group_info(data_dict['hmgroupids'])
        return group_info['uuid']
    except ValueError as e:
        # No need to log
        raise ValueError(e)

"""
Trigger event method of getting the group_name

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The group name
"""
def set_group_name(property_key, normalized_class, neo4j_driver, data_dict):
    if 'hmgroupids' not in data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'data_dict' during calling 'set_group_name()' trigger method.")
    
    try:
        group_info = schema_manager.get_entity_group_info(data_dict['hmgroupids'])
        return group_info['name']
    except ValueError as e:
        raise ValueError(e)
    

####################################################################################################
## Trigger methods shared by Donor and Sample - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting the submission_id

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The submission_id
"""
def set_submission_id(property_key, normalized_class, neo4j_driver, data_dict):
    if 'submission_id' not in data_dict:
        raise KeyError("Missing 'submission_id' key in 'data_dict' during calling 'set_submission_id()' trigger method.")
    return data_dict['submission_id']


####################################################################################################
## Trigger methods specific to Collection - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting a list of associated datasets for a given collection

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
list
    A list of associated dataset dicts with all the normalized information
"""
def get_collection_datasets(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_collection_datasets()' trigger method.")

    datasets_list = schema_neo4j_queries.get_collection_datasets(neo4j_driver, data_dict['uuid'])

    # Additional properties of the datasets to exclude 
    # We don't want to show too much nested information
    properties_to_skip = ['direct_ancestors', 'collections']
    complete_entities_list = schema_manager.get_complete_entities_list(datasets_list, properties_to_skip)

    return schema_manager.normalize_entities_list_for_response(complete_entities_list)


####################################################################################################
## Trigger methods specific to Dataset - DO NOT RENAME
####################################################################################################

"""
Trigger event method of getting a list of collections for this new Dtaset

Parameters
----------
property_key : str
    The target property key
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
list 
    A list of associated collections with all the normalized information
"""
def get_dataset_collections(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_dataset_collections()' trigger method.")

    # No property key needs to filter the result
    # Get back the list of collection dicts
    collections_list = schema_neo4j_queries.get_dataset_collections(neo4j_driver, data_dict['uuid'])

    # Exclude datasets from each resulting collection
    # We don't want to show too much nested information
    properties_to_skip = ['datasets']
    complete_entities_list = schema_manager.get_complete_entities_list(collections_list, properties_to_skip)

    return schema_manager.normalize_entities_list_for_response(complete_entities_list)

"""
Trigger event method of building linkages between this new Dataset and its direct ancestors

Parameters
----------
property_key : str
    The target property key
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def link_dataset_to_direct_ancestors(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    if 'direct_ancestor_uuids' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuids' key in 'data_dict' during calling 'link_dataset_to_direct_ancestors()' trigger method.")

    # For each source entity, create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    for direct_ancestor_uuid in data_dict['direct_ancestor_uuids']:
        # Activity is not an Entity, thus we use "class" for reference
        normalized_activity_class = 'Activity'

        # Target entity class dict
        # Will be used when calling set_activity_creation_action() trigger method
        normalized_entity_class_dict = {'normalized_entity_class': normalized_class}

        # Create new ids for the new Activity
        new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_class)

        # The `data_dict` should already have user_info
        data_dict_for_activity = {**data_dict, **normalized_entity_class_dict, **new_ids_dict_for_activity}

        # Generate property values for Activity
        generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_class, data_dict_for_activity)

        # `UNWIND` in Cypher expects List<T>
        activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

        # Convert the list (only contains one entity) to json list string
        activity_json_list_str = json.dumps(activity_data_list)

        logger.debug("======link_dataset_to_direct_ancestors() create activity with activity_json_list_str======")
        logger.debug(activity_json_list_str)

        try:
            schema_neo4j_queries.link_entity_to_direct_ancestor(neo4j_driver, data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
        except TransactionError:
            # No need to log
            raise


"""
Trigger event method of rebuilding linkages between Dataset and its direct ancestors

Parameters
----------
property_key : str
    The target property key
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def relink_dataset_to_direct_ancestors(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'relink_dataset_to_direct_ancestors()' trigger method.")

    if 'direct_ancestor_uuids' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuids' key in 'data_dict' during calling 'relink_dataset_to_direct_ancestors()' trigger method.")

    # Delete old linkages before recreating new ones
    try:
        schema_neo4j_queries.unlink_entity_to_direct_ancestors(neo4j_driver, data_dict['uuid'])
    except TransactionError:
        # No need to log
        raise

    # For each source entity, create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    for direct_ancestor_uuid in data_dict['direct_ancestor_uuids']:
        # Activity is not an Entity, thus we use "class" for reference
        normalized_activity_class = 'Activity'

        # Target entity class dict
        # Will be used when calling set_activity_creation_action() trigger method
        normalized_entity_class_dict = {'normalized_entity_class': normalized_class}

        # Create new ids for the new Activity
        new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_class)

        # The `data_dict` should already have user_info
        data_dict_for_activity = {**data_dict, **normalized_entity_class_dict, **new_ids_dict_for_activity}

        # Generate property values for Activity
        generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_class, data_dict_for_activity)

        # `UNWIND` in Cypher expects List<T>
        activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

        # Convert the list (only contains one entity) to json list string
        activity_json_list_str = json.dumps(activity_data_list)

        logger.debug("======relink_dataset_to_direct_ancestors() create activity with activity_json_list_str======")
        logger.debug(activity_json_list_str)

        try:
            schema_neo4j_queries.link_entity_to_direct_ancestor(neo4j_driver, data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
        except TransactionError:
            # No need to log
            raise


"""
Trigger event method of getting source uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
list
    A list of associated direct ancestors with all the normalized information
"""
def get_dataset_direct_ancestors(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_dataset_direct_ancestors()' trigger method.")

    # No property key needs to filter the result
    # Get back the list of ancestor dicts
    direct_ancestors_list = schema_neo4j_queries.get_dataset_direct_ancestors(neo4j_driver, data_dict['uuid'])

    # We don't want to show too much nested information
    # The direct ancestor of a Dataset could be: Dataset or Sample
    # Skip running the trigger methods for 'direct_ancestors' and 'collections' if the direct ancestor is Dataset
    # Skip running the trigger methods for 'direct_ancestor' if the direct ancestor is Sample
    properties_to_skip = ['direct_ancestors', 'collections', 'direct_ancestor']
    complete_entities_list = schema_manager.get_complete_entities_list(direct_ancestors_list, properties_to_skip)

    return schema_manager.normalize_entities_list_for_response(complete_entities_list)

"""
Trigger event method of getting source uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def get_dataset_direct_ancestor_uuids(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_dataset_direct_ancestors()' trigger method.")

    # Pass in the property key 'uuid' to filter the result
    # Only get back a list of collection uuids instead of the whole dict
    return schema_neo4j_queries.get_dataset_direct_ancestors(neo4j_driver, data_dict['uuid'], 'uuid')


"""
Trigger event method of getting the relative directory path of a given dataset

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The relative directory path
"""
def get_local_directory_rel_path(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_local_directory_rel_path()' trigger method.")
    
    uuid = data_dict['uuid']

    if (not 'group_uuid' in data_dict) or (not data_dict['group_uuid']):
        raise KeyError("Group uuid not set for dataset with uuid: " + uuid)

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
    
    # Get the data_acess_level by calling another trigger method
    data_access_level = get_data_access_level(property_key, normalized_class, neo4j_driver, data_dict)

    #look up the Component's group ID, return an error if not found
    data_group_id = data_dict['group_uuid']
    if not data_group_id in groups_by_id_dict:
        raise KeyError("Can not find dataset group: " + data_group_id + " for uuid: " + uuid)

    dir_path = data_access_level + "/" + groups_by_id_dict[data_group_id]['displayname'] + "/" + uuid + "/"

    return dir_path


####################################################################################################
## Trigger methods specific to Donor - DO NOT RENAME
####################################################################################################


####################################################################################################
## Trigger methods specific to Sample - DO NOT RENAME
####################################################################################################

"""
Trigger event method of building linkages between this new Sample and its ancestor

Parameters
----------
property_key : str
    The target property key
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
bool
    True if everything goes well, otherwise False
"""
def link_sample_to_direct_ancestor(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    if 'direct_ancestor_uuid' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuid' key in 'data_dict' during calling 'link_sample_to_direct_ancestor()' trigger method.")

    # Create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    # Activity is not an Entity, thus we use "class" for reference
    normalized_activity_class = 'Activity'

    # Target entity class dict
    # Will be used when calling set_activity_creation_action() trigger method
    normalized_entity_class_dict = {'normalized_entity_class': normalized_class}

    # Create new ids for the new Activity
    new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_class)

    # The `data_dict` should already have user_info
    data_dict_for_activity = {**data_dict, **normalized_entity_class_dict, **new_ids_dict_for_activity}
    
    # Generate property values for Activity
    generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_class, data_dict_for_activity)

    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    logger.debug("======link_sample_to_direct_ancestor() create activity with activity_json_list_str======")
    logger.debug(activity_json_list_str)

    try:
        schema_neo4j_queries.link_entity_to_direct_ancestor(neo4j_driver, data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
    except TransactionError:
        # No need to log
        raise



"""
Trigger event method of rebuilding linkages between this Sample and its direct ancestors 

Parameters
----------
property_key : str
    The target property key
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
bool
    True if everything goes well, otherwise False
"""
def relink_sample_to_direct_ancestor(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'relink_sample_to_direct_ancestor()' trigger method.")

    if 'direct_ancestor_uuid' not in data_dict:
        raise KeyError("Missing 'direct_ancestor_uuid' key in 'data_dict' during calling 'relink_sample_to_direct_ancestor()' trigger method.")

    # Create a linkage (via Activity node) 
    # between the dataset node and the source entity node in neo4j
    # Activity is not an Entity, thus we use "class" for reference
    normalized_activity_class = 'Activity'

    # Target entity class dict
    # Will be used when calling set_activity_creation_action() trigger method
    normalized_entity_class_dict = {'normalized_entity_class': normalized_class}

    # Create new ids for the new Activity
    new_ids_dict_for_activity = schema_manager.create_hubmap_ids(normalized_activity_class)

    # The `data_dict` should already have user_info
    data_dict_for_activity = {**data_dict, **normalized_entity_class_dict, **new_ids_dict_for_activity}
    
    # Generate property values for Activity
    generated_before_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('before_create_trigger', normalized_activity_class, data_dict_for_activity)

    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [generated_before_create_trigger_data_dict_for_activity]

    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    logger.debug("======relink_sample_to_direct_ancestor() create activity with activity_json_list_str======")
    logger.debug(activity_json_list_str)
 
    try:
        schema_neo4j_queries.link_entity_to_direct_ancestor(neo4j_driver, data_dict['uuid'], direct_ancestor_uuid, activity_json_list_str)
    except TransactionError:
        # No need to log
        raise


"""
Trigger event method of getting the parent of a Sample

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
dict
    The direct ancestor entity (either another Sample or a Donor) with all the normalized information
"""
def get_sample_direct_ancestor(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_sample_direct_ancestor()' trigger method.")

    direct_ancestor_dict = schema_neo4j_queries.get_sample_direct_ancestor(neo4j_driver, data_dict['uuid'])

    if 'entity_class' not in direct_ancestor_dict:
        raise KeyError("The 'entity_class' property in the resulting 'direct_ancestor_dict' is not set during calling 'get_sample_direct_ancestor()' trigger method.")

    # Generate trigger data for sample's direct_ancestor and skip the direct_ancestor's direct_ancestor
    properties_to_skip = ['direct_ancestor']
    complete_dict = schema_manager.get_complete_entity_result(direct_ancestor_dict, properties_to_skip)

    # Get rid of the entity node properties that are not defined in the yaml schema
    # as well as the ones defined as `exposed: false` in the yaml schema
    return normalize_entity_result_for_response(complete_dict)

"""
Trigger event method of getting source uuid

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def get_sample_direct_ancestor_uuid(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_sample_direct_ancestor_uuid()' trigger method.")

    return schema_neo4j_queries.get_sample_direct_ancestor(neo4j_driver, data_dict['uuid'], 'uuid')


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
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
neo4j_driver : neo4j.Driver object
    The neo4j database connection pool
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The creation_action string
"""
def set_activity_creation_action(property_key, normalized_class, neo4j_driver, data_dict):
    if 'normalized_entity_class' not in data_dict:
        raise KeyError("Missing 'normalized_entity_class' key in 'data_dict' during calling 'set_activity_creation_action()' trigger method.")
    
    return "Create {normalized_entity_class} Activity".format(normalized_entity_class = data_dict['normalized_entity_class'])

