import datetime

# Local modules
import schema_neo4j_queries


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
def set_timestamp(property_key, normalized_class, data_dict):
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
def set_entity_class(property_key, normalized_class, data_dict):
    return normalized_class

"""
Trigger event method of getting data access level

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_class : str
    One of the entity classes defined in the schema yaml: Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The data access level string
"""
def get_data_access_level(property_key, normalized_class, data_dict):
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
        if 'neo4j_db' not in data_dict:
            raise KeyError("Missing 'neo4j_db' key in 'data_dict' during calling 'get_data_access_level()' trigger method.")
    
        count = schema_neo4j_queries.count_attached_published_datasets(data_dict['neo4j_db'], normalized_class, data_dict['uuid'])

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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'sub' string
"""
def set_user_sub(property_key, normalized_class, data_dict):
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'email' string
"""
def set_user_email(property_key, normalized_class, data_dict):
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'name' string
"""
def set_user_displayname(property_key, normalized_class, data_dict):
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid created via uuid-api
"""
def set_uuid(property_key, normalized_class, data_dict):
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The hubmap_id created via uuid-api
"""
def set_hubmap_id(property_key, normalized_class, data_dict):
    if 'hubmap_id' not in data_dict:
        raise KeyError("Missing 'hubmap_id' key in 'data_dict' during calling 'set_hubmap_id()' trigger method.")
    return data_dict['hubmap_id']



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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
list
    A list a associated dataset dicts
"""
def get_collection_datasets(property_key, normalized_class, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_collection_datasets()' trigger method.")
    
    if 'neo4j_db' not in data_dict:
        raise KeyError("Missing 'neo4j_db' key in 'data_dict' during calling 'get_collection_datasets()' trigger method.")
    
    return schema_neo4j_queries.get_collection_datasets(data_dict['neo4j_db'], data_dict['uuid'])

def connect_datasets_to_collection():
    return "dummy"

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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def get_dataset_source_uuids(property_key, normalized_class, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_dataset_source_uuids()' trigger method.")
    
    if 'neo4j_db' not in data_dict:
        raise KeyError("Missing 'neo4j_db' key in 'data_dict' during calling 'get_dataset_source_uuids()' trigger method.")
    
    return schema_neo4j_queries.get_dataset_source_uuids(data_dict['neo4j_db'], data_dict['uuid'])

def get_local_file_path():
    return "dummy"

def set_group_uuid():
    return "dummy"

def set_group_name():
    return "dummy"

####################################################################################################
## Trigger methods specific to Donor - DO NOT RENAME
####################################################################################################

def set_donor_submission_id():
    return "dummy"

def set_donor_group_uuid():
    return "dummy"

####################################################################################################
## Trigger methods specific to Sample - DO NOT RENAME
####################################################################################################

def set_sample_submission_id():
    return "dummy"

def set_sample_to_parent_relationship():
    return "dummy"

def get_sample_ancestor():
    return "dummy"

"""
Trigger event method of getting source uuid

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
    The uuid string of source entity
"""
def get_sample_source_uuid(property_key, normalized_class, data_dict):
    if 'uuid' not in data_dict:
        raise KeyError("Missing 'uuid' key in 'data_dict' during calling 'get_sample_source_uuid()' trigger method.")
    
    if 'neo4j_db' not in data_dict:
        raise KeyError("Missing 'neo4j_db' key in 'data_dict' during calling 'get_sample_source_uuid()' trigger method.")
    
    return schema_neo4j_queries.get_sample_source_uuid(data_dict['neo4j_db'], data_dict['uuid'])


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
    Activity
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The creation_action string
"""
def set_activity_creation_action(property_key, normalized_class, data_dict):
    if 'normalized_entity_class' not in data_dict:
        raise KeyError("Missing 'normalized_entity_class' key in 'data_dict' during calling 'set_activity_creation_action()' trigger method.")
    return "Create {normalized_entity_class} Activity".format(normalized_entity_class = data_dict['normalized_entity_class'])

