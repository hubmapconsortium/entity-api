####################################################################################################
## Schema trigger methods based on the yaml file - DO NOT RENAME
####################################################################################################

import datetime
import requests
from urllib3.exceptions import InsecureRequestWarning

# Use the current_app proxy, which points to the application handling the current activity
from flask import current_app as app

# Local modules
import neo4j_queries

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)


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
int
    A timestamp integer of seconds
"""
def set_timestamp(property_key, entity_dict, data_dict):
    current_time = datetime.datetime.now() 
    seconds = int(current_time.timestamp())
    entity_dict[property_key] = seconds
    return entity_dict

"""
Trigger event method of setting the entity class of a given entity

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The string of normalized entity class
"""
def set_entity_class(property_key, entity_dict, data_dict):
    entity_dict['entity_class'] = data_dict['normalized_entity_class']
    return entity_dict

"""
Trigger event method of getting data access level

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The data access level string
"""
def get_data_access_level(property_key, entity_dict, data_dict):
    # For now, don't use the constants from commons
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'
    
    normalized_entity_class = data_dict['normalized_entity_class']

    if normalized_entity_class == 'Dataset':
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
        count = neo4j_queries.count_attached_published_datasets(app.get_neo4j_db(), normalized_entity_class, data_dict['uuid'])

        if count > 0:
            data_access_level = ACCESS_LEVEL_PUBLIC

    return data_access_level

"""
Trigger event method of getting user sub

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'sub' string
"""
def set_user_sub(property_key, entity_dict, data_dict):
    user_info = get_user_info(data_dict['request'])
    entity_dict[property_key] = user_info['sub']
    return entity_dict

"""
Trigger event method of getting user email

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'email' string
"""
def set_user_email(property_key, entity_dict, data_dict):
    user_info = get_user_info(data_dict['request'])
    entity_dict[property_key] = user_info['email']
    return entity_dict

"""
Trigger event method of getting user name

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The 'name' string
"""
def set_user_displayname(property_key, entity_dict, data_dict):
    user_info = get_user_info(data_dict['request'])
    entity_dict[property_key] = user_info['name']
    return entity_dict

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
dict
    The dict that contains uuid and hubmap_id created via uuid-api
"""
def set_uuid(property_key, entity_dict, data_dict):
    if entity_dict['hubmap_id'] != None:
        hubmap_ids = get_hubmap_ids(entity_dict['hubmap_id'])
        
    else:
        hubmap_ids = create_hubmap_ids(data_dict['normalized_entity_class'])

    entity_dict[property_key] = hubmap_ids['hmuuid']
    return entity_dict

"""
Trigger event method of getting uuid, hubmap_id for a new entity to be created

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
dict
    The dict that contains uuid and hubmap_id created via uuid-api
"""
def set_hubmap_id(property_key, entity_dict, data_dict):
    if entity_dict['uuid'] != None:
        hubmap_ids = get_hubmap_ids(entity_dict['uuid'])
        
    else:
        hubmap_ids = create_hubmap_ids(data_dict['normalized_entity_class'])

    entity_dict[property_key] = hubmap_ids['hubmapId']
    return entity_dict



####################################################################################################
## Trigger methods specific to Collection
####################################################################################################

"""
Trigger event method of getting a list of associated datasets for a given collection

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
list
    A list a associated dataset dicts
"""
def get_collection_datasets(property_key, entity_dict, data_dict):
    return neo4j_queries.get_collection_datasets(app.get_neo4j_db(), data_dict['uuid'])

def connect_datasets_to_collection():
    return "dummy"

####################################################################################################
## Trigger methods specific to Dataset
####################################################################################################

"""
Trigger event method of getting source uuid

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def get_dataset_source_uuids(property_key, entity_dict, data_dict):
    return neo4j_queries.get_source_uuids(app.get_neo4j_db(), data_dict['uuid'])

def set_local_file_path():
    return "dummy"

def set_group_uuid():
    return "dummy"

def set_group_name():
    return "dummy"

####################################################################################################
## Trigger methods specific to Donor
####################################################################################################

def set_donor_submission_id():
    return "dummy"

def set_donor_group_uuid():
    return "dummy"

####################################################################################################
## Trigger methods specific to Sample
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
data_dict : dict
    A merged dictionary that contains all possible input data to be used
    It's fine if a trigger method doesn't use any input data

Returns
-------
str
    The uuid string of source entity
"""
def get_sample_source_uuid(property_key, entity_dict, data_dict):
    return neo4j_queries.get_source_uuids(app.get_neo4j_db(), data_dict['uuid'])


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
str
    The creation_action string
"""
def get_activity_creation_action(property_key, entity_dict, data_dict):
    return "Create {entity_class} Activity".format(entity_class = data_dict['normalized_activity_class'])

####################################################################################################
## Internal Functions
####################################################################################################

"""
Initialize AuthHelper (AuthHelper from HuBMAP commons package)
HuBMAP commons AuthHelper handles "MAuthorization" or "Authorization"

Returns
-------
AuthHelper
    An instnce of AuthHelper
"""
def init_auth_helper():
    if AuthHelper.isInitialized() == False:
        auth_helper = AuthHelper.create(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'])
    else:
        auth_helper = AuthHelper.instance()
    
    return auth_helper

"""
Get user infomation dict based on the http request(headers)

Parameters
----------
request : Flask request object
    The Flask request passed from the API endpoint 

Returns
-------
dict
    A dict containing all the user info

    {
        "scope": "urn:globus:auth:scope:nexus.api.globus.org:groups",
        "name": "First Last",
        "iss": "https://auth.globus.org",
        "client_id": "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114",
        "active": True,
        "nbf": 1603761442,
        "token_type": "Bearer",
        "aud": ["nexus.api.globus.org", "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114"],
        "iat": 1603761442,
        "dependent_tokens_cache_id": "af2d5979090a97536619e8fbad1ebd0afa875c880a0d8058cddf510fc288555c",
        "exp": 1603934242,
        "sub": "c0f8907a-ec78-48a7-9c85-7da995b05446",
        "email": "email@pitt.edu",
        "username": "username@pitt.edu",
        "hmscopes": ["urn:globus:auth:scope:nexus.api.globus.org:groups"],
    }
"""
def get_user_info(request):
    auth_helper = init_auth_helper()
    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    user_info = auth_helper.getUserInfoUsingRequest(request, False)

    logger.debug("======get_user_info()======")
    logger.debug(user_info)

    # If returns error response, invalid header or token
    if isinstance(user_info, Response):
        msg = "Failed to query the user info with the given globus token"

        logger.error(msg)

        app.bad_request_error(msg)

    return user_info

"""
Create a dict of HTTP Authorization header with Bearer token for making calls to uuid-api

Returns
-------
dict
    The headers dict to be used by requests
"""
def create_request_headers():
    # Will need this to call getProcessSecret()
    auth_helper = init_auth_helper()

    auth_header_name = 'Authorization'
    auth_scheme = 'Bearer'

    headers_dict = {
        # Don't forget the space between scheme and the token value
        auth_header_name: auth_scheme + ' ' + auth_helper.getProcessSecret()
    }

    return headers_dict

"""
Retrive target uuid based on the given id

Parameters
----------
id : str
    Either the uuid or hubmap_id of target entity 

Returns
-------
dict
    The dict returned by uuid-api that contains all the associated ids, e.g.:
    {
        "doiSuffix": "456FDTP455",
        "email": "xxx@pitt.edu",
        "hmuuid": "461bbfdc353a2673e381f632510b0f17",
        "hubmapId": "VAN0002",
        "parentId": null,
        "timeStamp": "2019-11-01 18:34:24",
        "type": "{UUID_DATATYPE}",
        "userId": "83ae233d-6d1d-40eb-baa7-b6f636ab579a"
    }
"""
def get_hubmap_ids(id):
    target_url = app.config['UUID_API_URL'] + '/' + id

    # Use modified version of globus app secrect from configuration as the internal token
    # All API endpoints specified in gateway regardless of auth is required or not, 
    # will consider this internal token as valid and has the access to HuBMAP-Read group
    request_headers = create_request_headers()

    # Disable ssl certificate verification
    response = requests.get(url = target_url, headers = request_headers, verify = False) 
    
    if response.status_code == 200:
        ids_list = response.json()

        if len(ids_list) == 0:
            app.not_found_error("Unable to find information via uuid-api on id: " + id)
        if len(ids_list) > 1:
            app.internal_server_error("Found multiple records via uuid-api for id: " + id)
        
        return ids_list[0]
    else:
        app.not_found_error("Could not find the target uuid via uuid-api service associatted with the provided id of " + id)


"""
Create a set of new ids for the new entity to be created
Make a POST call to uuid-api with the following json:
{
    "entityType":"Dataset",
    "generateDOI": "true"
}

The list returned by uuid-api that contains all the associated ids, e.g.:
{
    "uuid": "c754a4f878628f3c072d4e8024f707cd",
    "doi": "479NDDG476",
    "displayDoi": "HBM479.NDDG.476"
}

Then map them to the target ids:
uuid -> uuid
doi -> doi_suffix_id
displayDoi -> hubmap_id

Returns
-------
dict
    The dictionary of new ids

    {
        "uuid": "c754a4f878628f3c072d4e8024f707cd",
        "doi_suffix_id": "479NDDG476",
        "hubmap_id": "HBM479.NDDG.476"
    }

"""
def create_hubmap_ids(normalized_entity_class):
    target_url = app.config['UUID_API_URL']

    # Must use "generateDOI": "true" to generate the doi (doi_suffix_id) and displayDoi (hubmap_id)
    json_to_post = {
        'entityType': normalized_entity_class, 
        'generateDOI': "true"
    }

    # Use modified version of globus app secrect from configuration as the internal token
    # All API endpoints specified in gateway regardless of auth is required or not, 
    # will consider this internal token as valid and has the access to HuBMAP-Read group
    request_headers = create_request_headers()

    # Disable ssl certificate verification
    response = requests.post(url = target_url, headers = request_headers, json = json_to_post, verify = False) 
    
    if response.status_code == 200:
        ids_list = response.json()
        ids_dict = ids_list[0]

        # Create a new dict with desired keys
        new_ids_dict = {
            'uuid': ids_dict['uuid'],
            'hubmap_id': ids_dict['displayDoi']
        }

        return new_ids_dict
    else:
        msg = "Failed to create new ids via the uuid-api service during the creation of this new " + normalized_entity_class
        
        logger.error(msg)

        logger.debug("======create_new_ids() status code======")
        logger.debug(response.status_code)

        logger.debug("======create_new_ids() response text======")
        logger.debug(response.text)

        app.internal_server_error(msg)
