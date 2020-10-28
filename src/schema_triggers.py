####################################################################################################
## Schema trigger methods based on the yaml file - DO NOT RENAME
####################################################################################################

import logging
import datetime

# Local modules
import neo4j_queries

####################################################################################################
## Trigger methods shared among Collection, Dataset, Donor, Sample
####################################################################################################

"""
Trigger event method of generating current timestamp

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    A timestamp string
"""
def get_current_timestamp(data_dict):
    current_time = datetime.datetime.now() 
    return current_time.timestamp() 

"""
Trigger event method of generating current timestamp

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The string of normalized entity type
"""
def get_entity_type(data_dict):
    return data_dict['normalized_entity_type']

"""
Trigger event method of getting user sub

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The 'sub' string
"""
def get_user_sub(data_dict):
    return data_dict['sub']

"""
Trigger event method of getting user email

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The 'email' string
"""
def get_user_email(data_dict):
    return data_dict['email']

"""
Trigger event method of getting user name

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The 'name' string
"""
def get_user_name(data_dict):
    return data_dict['name']

"""
Trigger event method of getting source uuid

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The uuid string
"""
def get_source_uuid(data_dict):
    return neo4j_queries.get_source_uuid(data_dict['neo4j_driver'], data_dict['uuid'])

"""
Trigger event method of getting uuid

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The uuid string
"""
def create_uuid(data_dict):
    return data_dict['uuid']

"""
Trigger event method of getting uuid

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The doi_suffix_id string
"""
def create_doi_suffix_id(data_dict):
    return data_dict['doi_suffix_id']

"""
Trigger event method of getting uuid

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The hubmap_id string
"""
def create_hubmap_id(data_dict):
    return data_dict['hubmap_id']

"""
Trigger event method of getting data access level

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The data access level string
"""
def get_data_access_level(data_dict):
    data_access_level = "consortium"

    normalized_entity_type = data_dict['normalized_entity_type']
    if normalized_entity_type == "Dataset":
        if data_dict['contains_human_genetic_sequences']:
            data_access_level = "protected" 

    return data_access_level

# TO-DO
def get_creators_info(data_dict):
    return "dummy"

# TO-DO
def get_contacts_info(data_dict):
    return "dummy"

####################################################################################################
## Trigger methods specific to Collection
####################################################################################################

"""
Trigger event method of getting a list of associated dataset uuids for a given collection

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
list
    A list a associated dataset uuids
"""
def get_dataset_uuids_by_collection(data_dict):
    return neo4j_queries.get_dataset_uuids_by_collection(data_dict['neo4j_driver'], data_dict['uuid'])


####################################################################################################
## Trigger methods specific to Dataset
####################################################################################################


####################################################################################################
## Trigger methods specific to Donor
####################################################################################################


####################################################################################################
## Trigger methods specific to Sample
####################################################################################################

####################################################################################################
## Trigger methods specific to Activity
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
string
    The creation_action string
"""
def get_activity_creation_action(data_dict):
    return "Create {entity_type} Activity".format(entity_type = data_dict['normalized_entity_type'])
