####################################################################################################
## Schema trigger methods based on the yaml file - DO NOT RENAME
####################################################################################################

import logging
import datetime

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
    return data_dict['source_uuid']

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

# TO-DO
def get_data_access_level(data_dict):
    return "public"

# TO-DO
def get_creators_info(data_dict):
    return "dummy"

# TO-DO
def get_contacts_info(data_dict):
    return "dummy"

####################################################################################################
## Trigger methods specific to Collection
####################################################################################################

# TO-DO
def get_dataset_uuids(data_dict):
    return "dummy"


####################################################################################################
## Trigger methods specific to Dataset
####################################################################################################


####################################################################################################
## Trigger methods specific to Donor
####################################################################################################


####################################################################################################
## Trigger methods specific to Sample
####################################################################################################