####################################################################################################
## Schema trigger methods based on the yaml file - DO NOT RENAME
####################################################################################################

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
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
int
    A timestamp integer of seconds
"""
def get_current_timestamp(combined_data_dict):
    current_time = datetime.datetime.now() 
    seconds = int(current_time.timestamp())
    return seconds

"""
Trigger event method of generating current timestamp

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The string of normalized entity type
"""
def get_entity_type(combined_data_dict):
    return combined_data_dict['normalized_entity_class']

"""
Trigger event method of getting data access level

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The data access level string
"""
def get_data_access_level(combined_data_dict):
    normalized_entity_class = combined_data_dict['normalized_entity_class']

    if normalized_entity_class == "Dataset":
        # Default to "protected"
        data_access_level = "protected"

        if combined_data_dict['contains_human_genetic_sequences']:
            data_access_level = "protected" 
        else:
            if combined_data_dict['status'] == "Published":
                data_access_level = "public"
            else:
                data_access_level = "consortium" 
    else:
        # Default to "consortium" for Collection/Donor/Sample
        data_access_level = "consortium"
        
        # public if any dataset below it in the provenance hierarchy is published
        # (i.e. Dataset.status == "Published")
        count = neo4j_queries.count_attached_published_datasets(combined_data_dict['neo4j_driver'], normalized_entity_class, combined_data_dict['uuid'])

        if count > 0:
            data_access_level = "public"

    return data_access_level

"""
Trigger event method of getting user sub

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'sub' string
"""
def get_user_sub(combined_data_dict):
    return combined_data_dict['sub']

"""
Trigger event method of getting user email

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'email' string
"""
def get_user_email(combined_data_dict):
    return combined_data_dict['email']

"""
Trigger event method of getting user name

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'name' string
"""
def get_user_name(combined_data_dict):
    return combined_data_dict['name']

"""
Trigger event method of getting source uuid

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def get_source_uuids(combined_data_dict):
    return neo4j_queries.get_source_uuids(combined_data_dict['neo4j_driver'], combined_data_dict['uuid'])

"""
Trigger event method of getting uuid

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string
"""
def create_uuid(combined_data_dict):
    return combined_data_dict['uuid']

"""
Trigger event method of getting uuid

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The doi_suffix_id string
"""
def create_doi_suffix_id(combined_data_dict):
    return combined_data_dict['doi_suffix_id']

"""
Trigger event method of getting uuid

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The hubmap_id string
"""
def create_hubmap_id(combined_data_dict):
    return combined_data_dict['hubmap_id']

####################################################################################################
## Trigger methods specific to Collection
####################################################################################################

"""
Trigger event method of getting a list of associated dataset uuids for a given collection

Parameters
----------
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
list
    A list a associated dataset uuids
"""
def get_dataset_uuids_by_collection(combined_data_dict):
    return neo4j_queries.get_dataset_uuids_by_collection(combined_data_dict['neo4j_driver'], combined_data_dict['uuid'])


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
combined_data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The creation_action string
"""
def get_activity_creation_action(combined_data_dict):
    return "Create {entity_type} Activity".format(entity_type = combined_data_dict['normalized_activity_class'])
