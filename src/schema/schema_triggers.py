import datetime

# Local modules
from schema import schema_neo4j_queries


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

def set_group_uuid(property_key, normalized_class, neo4j_driver, data_dict):
    if 'hmgroupids' not in data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'data_dict' during calling 'set_group_uuid()' trigger method.")
    
    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']

    # A list of data provider uuids
    data_provider_uuids = []
    for uuid_key in groups_by_id_dict:
        if 'data_provider' in groups_by_id_dict[uuid_key] and groups_by_id_dict[uuid_key]['data_provider']:
            data_provider_uuids.append(uuid_key)

    data_provider_groups = []
    for group_uuid in data_dict['hmgroupids']:
        if group_uuid in data_provider_uuids:
            data_provider_groups.append(group_uuid)

    if len(data_provider_groups) == 0:
        raise ValueError("No data_provider groups found for this user. Can't continue.")

    if len(data_provider_groups) > 1:
        raise ValueError("More than one data_provider groups found for this user. Can't continue.")

    # By now only one data provider group found, this is what we want
    uuid = data_provider_groups[0]

    return uuid


def set_group_name(property_key, normalized_class, neo4j_driver, data_dict):
    if 'hmgroupids' not in data_dict:
        raise KeyError("Missing 'hmgroupids' key in 'data_dict' during calling 'set_group_name()' trigger method.")
    
    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']

    # A list of data provider uuids
    data_provider_uuids = []
    for uuid_key in groups_by_id_dict:
        if 'data_provider' in groups_by_id_dict[uuid_key] and groups_by_id_dict[uuid_key]['data_provider']:
            data_provider_uuids.append(uuid_key)

    data_provider_groups = []
    for group_uuid in data_dict['hmgroupids']:
        if group_uuid in data_provider_uuids:
            data_provider_groups.append(group_uuid)

    if len(data_provider_groups) == 0:
        raise ValueError("No data_provider groups found for this user. Can't continue.")

    if len(data_provider_groups) > 1:
        raise ValueError("More than one data_provider groups found for this user. Can't continue.")

    # By now only one data provider group found, this is what we want
    uuid = data_provider_groups[0]
    group_name = groups_by_id_dict[uuid]['displayname']

    return group_name


####################################################################################################
## Trigger methods shared by Donor and Sample - DO NOT RENAME
####################################################################################################

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
    A list a associated dataset dicts
"""
def get_collection_datasets(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_collection_datasets()' trigger method.")

    return schema_neo4j_queries.get_collection_datasets(neo4j_driver, data_dict['uuid'])

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
str
    The name of relationship
"""
def link_collection_to_datasets(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'link_collection_to_datasets()' trigger method.")

    if 'uuids_list' not in data_dict:
        raise KeyError("Missing 'uuids_list' key in 'data_dict' during calling 'link_collection_to_datasets()' trigger method.")

    return schema_neo4j_queries.link_collection_to_datasets(neo4j_driver, data_dict['uuid'], data_dict['uuids_list'])


####################################################################################################
## Trigger methods specific to Dataset - DO NOT RENAME
####################################################################################################

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
def get_dataset_source_uuids(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_dataset_source_uuids()' trigger method.")

    return schema_neo4j_queries.get_dataset_source_uuids(neo4j_driver, data_dict['uuid'])

"""
Trigger event method of getting the relative directory path of a given uuid

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
def get_local_file_path(property_key, normalized_class, neo4j_driver, data_dict):
    return "dummy"

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
str
    The name of relationship
"""
def link_dataset_to_collectionss(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'link_collection_to_datasets()' trigger method.")

    if 'uuids_list' not in data_dict:
        raise KeyError("Missing 'uuids_list' key in 'data_dict' during calling 'link_collection_to_datasets()' trigger method.")

    return schema_neo4j_queries.link_dataset_to_collectionss(neo4j_driver, data_dict['uuid'], data_dict['uuids_list'])


####################################################################################################
## Trigger methods specific to Donor - DO NOT RENAME
####################################################################################################

def set_submission_id(property_key, normalized_class, neo4j_driver, data_dict):
    if 'submission_id' not in data_dict:
        raise KeyError("Missing 'submission_id' key in 'data_dict' during calling 'set_submission_id()' trigger method.")
    return data_dict['submission_id']

####################################################################################################
## Trigger methods specific to Sample - DO NOT RENAME
####################################################################################################

def set_sample_to_parent_relationship(property_key, normalized_class, neo4j_driver, data_dict):
    return "dummy"

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
    The parent entity, either another Sample or a Donor
"""
def get_sample_direct_ancestor(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_sample_direct_ancestor()' trigger method.")

    return schema_neo4j_queries.get_sample_direct_ancestor(neo4j_driver, data_dict['uuid'])

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
def get_sample_source_uuid(property_key, normalized_class, neo4j_driver, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_sample_source_uuid()' trigger method.")

    return schema_neo4j_queries.get_sample_source_uuid(neo4j_driver, data_dict['uuid'])


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

