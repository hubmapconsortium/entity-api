import yaml
import traceback
import logging
import requests
from cachetools import cached, TTLCache
import functools
from urllib3.exceptions import InsecureRequestWarning

# Use the current_app proxy, which points to the application handling the current activity
from flask import current_app as app

# Local modules
from schema import schema_triggers

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons import neo4j_driver
from hubmap_commons import globus_groups

logger = logging.getLogger(__name__)

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

# LRU Cache implementation with per-item time-to-live (TTL) value
# with a memoizing callable that saves up to maxsize results based on a Least Frequently Used (LFU) algorithm
# with a per-item time-to-live (TTL) value
# The maximum integer number of entries in the cache queue: 128
# Expire the cache after the time-to-live (seconds): two hours, 7200 seconds
cache = TTLCache(128, ttl=7200)

# In Python, "privacy" depends on "consenting adults'" levels of agreement, we can't force it.
# A single leading underscore means you're not supposed to access it "from the outside"
_schema = None
_neo4j_driver = None
_uuid_api_url = None
_auth_helper = None

####################################################################################################
## Provenance yaml schema initialization
####################################################################################################

"""
Initialize the schema_manager module with loading the schema yaml file 
and create an neo4j driver instance (some trigger methods query neo4j)

Parameters
----------
valid_yaml_file : file
    A valid yaml file
neo4j_session_context : neo4j.Session object
    The neo4j database session
"""
def initialize(valid_yaml_file, neo4j_uri, neo4j_username, neo4j_password, uuid_api_url, globus_app_client_id, globus_app_client_secret):
    # Specify as module-scope variables
    global _schema
    global _neo4j_driver
    global _uuid_api_url
    global _auth_helper

    _schema = load_provenance_schema(valid_yaml_file)
    _neo4j_driver = neo4j_driver.instance(neo4j_uri, neo4j_username, neo4j_password)
    _uuid_api_url = uuid_api_url

    # Initialize AuthHelper (AuthHelper from HuBMAP commons package)
    # auth_helper will be used to get the globus user info and 
    # the secret token for making calls to other APIs
    _auth_helper = AuthHelper.create(globus_app_client_id, globus_app_client_secret)
    

####################################################################################################
## Provenance yaml schema loading
####################################################################################################

"""
Load the schema yaml file

Parameters
----------
valid_yaml_file : file
    A valid yaml file

Returns
-------
dict
    A dict containing the schema details
"""
@cached(cache)
def load_provenance_schema(valid_yaml_file):
    with open(valid_yaml_file) as file:
        schema_dict = yaml.safe_load(file)

        logger.info("Schema yaml file loaded successfully")

        logger.debug("======schema_dict======")
        logger.debug(schema_dict)

        return schema_dict


"""
Clear or invalidate the schema cache even before it expires
"""
def clear_schema_cache():
    logger.info("Schema yaml cache cleared")
    cache.clear()


####################################################################################################
## Helper functions
####################################################################################################

"""
Get a list of all the supported entity classes in the schmea yaml

Returns
-------
list
    A list of entity classes
"""
def get_all_entity_classes():
    global _schema

    dict_keys = _schema['ENTITIES'].keys()
    # Need convert the dict_keys object to a list
    return list(dict_keys)

"""
Generating triggered data based on the target events and methods

Parameters
----------
trigger_type : str
    One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
data_dict : dict
    A dictionary that contains data to be used by the trigger methods

Returns
-------
dict
    A dictionary of trigger event methods generated data
"""
def generate_triggered_data(trigger_type, normalized_class, data_dict):
    global _schema
    global _neo4j_driver

    schema_section = None

    # A bit validation
    validate_trigger_type(trigger_type)
    validate_normalized_entity_class(normalized_class)

    # Determine the schema section based on class
    if normalized_class == 'Activity':
        schema_section = _schema['ACTIVITIES']
    else:
        schema_section = _schema['ENTITIES']

    properties = schema_section[normalized_class]['properties']
    class_property_keys = properties.keys() 

    # Put all resulting data into a dictionary too
    trigger_generated_data_dict = {}
    for key in class_property_keys:
        if trigger_type in properties[key]:

            if trigger_type in ['after_create_trigger', 'after_update_trigger']:
                # Only call the triggers if the propery key presents from the incoming data
                # E.g., 'source_uuid' for Sample, 'dataset_uuids' for Collection
                if key in data_dict:
                    trigger_method_name = properties[key][trigger_type]

                    logger.debug("Calling schema " + trigger_type + ": " + trigger_method_name + " defined for " + normalized_class)

                    # Call the target trigger method of schema_triggers.py module
                    trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                    try:
                        # No return values for 'after_create_trigger' and 'after_update_trigger'
                        # because the property value is already set in `data_dict`
                        # normally it's building linkages between entity nodes
                        trigger_method_to_call(key, normalized_class, _neo4j_driver, data_dict)
                    except KeyError as ke:
                        logger.error(ke)
                    except Exception as e:
                        logger.error(traceback.format_exc())

                    # True to indicate success
                    return True
            else:
                # Handling of all other trigger types:
                # before_create_trigger|before_update_trigger|on_read_trigger
                trigger_method_name = properties[key][trigger_type]

                logger.debug("Calling schema " + trigger_type + ": " + trigger_method_name + " defined for " + normalized_class)

                # Call the target trigger method of schema_triggers.py module
                trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                try:
                    # Will set the trigger return value as the property value
                    trigger_generated_data_dict[key] = trigger_method_to_call(key, normalized_class, _neo4j_driver, data_dict)
                except KeyError as ke:
                    logger.error(ke)
                except Exception as e:
                    logger.error(traceback.format_exc())

                return trigger_generated_data_dict


"""
Remove entity node properties that are not defined in the yaml schema prior to response

Parameters
----------
normalized_entity_class : str
    One of the entity classes defined in the schema yaml: Collection, Donor, Sample, Dataset
data_dict : dict
    A merged dictionary that contains all possible data to be used by the trigger methods

Returns
-------
dict
    A entity dictionary with keys that are all defined in schema yaml
"""
def remove_undefined_entity_properties(normalized_entity_class, entity_dict):
    global _schema

    properties = _schema['ENTITIES'][normalized_entity_class]['properties']
    class_property_keys = properties.keys() 
    # In Python 3, entity_dict.keys() returns an iterable, which causes error if deleting keys during the loop
    # We can use list to force a copy of the keys to be made
    entity_keys = list(entity_dict)

    for key in entity_keys:
        if key not in class_property_keys:
            del entity_dict[key]
        else:
            # Also remove the properties that are marked as `exposed: false`
            if ('exposed' in properties[key]) and (not properties[key]['exposed']):
                del entity_dict[key]

    return entity_dict

"""
Validate json data from user request against the schema

Parameters
----------
json_data_dict : dict
    The json data dict from user request
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
existing_entity_dict : dict
    Entity dict for creating new entity, otherwise pass in the existing entity dict for update validation
"""
def validate_json_data_against_schema(json_data_dict, normalized_entity_class, existing_entity_dict = {}):
    global _schema

    properties = _schema['ENTITIES'][normalized_entity_class]['properties']
    schema_keys = properties.keys() 
    json_data_keys = json_data_dict.keys()
    separator = ', '

    # Check if keys in request json are supported
    unsupported_keys = []
    for key in json_data_keys:
        if key not in schema_keys:
            unsupported_keys.append(key)

    if len(unsupported_keys) > 0:
        raise KeyError("Unsupported keys in request json: " + separator.join(unsupported_keys))

    # Check if keys in request json are the ones to be auto generated
    generated_keys = []
    for key in json_data_keys:
        if ('generated' in properties[key]) and properties[key]['generated']:
            if properties[key]:
                generated_keys.append(key)

    if len(generated_keys) > 0:
        raise KeyError("Auto generated keys are not allowed in request json: " + separator.join(generated_keys))

    # Only check if keys in request json are immutable during entity update
    if not bool(existing_entity_dict):
        immutable_keys = []
        for key in json_data_keys:
            if ('immutable' in properties[key]) and properties[key]['immutable']:
                if properties[key]:
                    immutable_keys.append(key)

        if len(immutable_keys) > 0:
            raise KeyError("Immutable keys are not allowed in request json: " + separator.join(immutable_keys))
        
    # Check if keys in request json are generated transient keys
    transient_keys = []
    for key in json_data_keys:
        if ('transient' in properties[key]) and properties[key]['transient']:
            if properties[key]:
                transient_keys.append(key)

    if len(transient_keys) > 0:
        rise KeyError("Transient keys are not allowed in request json: " + separator.join(transient_keys))

    # Check if any schema keys that are user_input_required but missing from request
    missing_required_keys = []
    for key in schema_keys:
        # Schema rules: 
        # - By default, the schema treats all entity properties as optional. Use `user_input_required: true` to mark a property as required
        # - If aproperty is marked as `user_input_required: true`, it can't have `trigger` at the same time
        # It's reenforced here because we can't guarantee this rule is being followed correctly in the schema yaml
        if 'user_input_required' in properties[key]:
            if properties[key]['user_input_required'] and ('trigger' not in properties[key]) and (key not in json_data_keys):
                # When existing_entity_dict is empty, it means creating new entity
                # When existing_entity_dict is not empty, it means updating an existing entity
                if not bool(existing_entity_dict):
                    missing_required_keys.append(key)
                else:
                    # It is a missing key when the existing entity data doesn't have it
                    if key not in existing_entity_dict:
                        missing_required_keys.append(key)

    if len(missing_required_keys) > 0:
        raise KeyError("Missing required keys in request json: " + separator.join(missing_required_keys))

    # By now all the keys in request json have passed the above two checks: existence cehck in schema and required check in schema
    # Verify data types of keys
    invalid_data_type_keys = []
    for key in json_data_keys:
        # boolean starts with bool, string starts with str, integer starts with int, list is list
        if not properties[key]['type'].startswith(type(json_data_dict[key]).__name__):
            invalid_data_type_keys.append(key)

        # Handling json_string as dict
        if (properties[key]['type'] == 'json_string') and (not isinstance(json_data_dict[key], dict)): 
            invalid_data_type_keys.append(key)
    
    if len(invalid_data_type_keys) > 0:
        raise TypeError("Keys in request json with invalid data types: " + separator.join(invalid_data_type_keys))


"""
Get a list of entity classes that can be used as derivation source in the schmea yaml

Returns
-------
list
    A list of entity classes
"""
def get_derivation_source_entity_classes():
    global _schema

    derivation_source_entity_classes = []
    entity_classes = get_all_entity_classes()
    for entity_class in entity_classes:
        if _schema['ENTITIES'][entity_class]['derivation']['source']:
            derivation_source_entity_classes.append(entity_class)

    return derivation_source_entity_classes

"""
Get a list of entity classes that can be used as derivation target in the schmea yaml

Returns
-------
list
    A list of entity classes
"""
def get_derivation_target_entity_classes():
    global _schema

    derivation_target_entity_classes = []
    entity_classes = get_all_entity_classes()
    for entity_class in entity_classes:
        if _schema['ENTITIES'][entity_class]['derivation']['target']:
            derivation_target_entity_classes.append(entity_class)

    return derivation_target_entity_classes

"""
Lowercase and captalize the entity type string

Parameters
----------
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
id : str
    The uuid of target entity 

Returns
-------
string
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
"""
def normalize_entity_class(entity_class):
    normalized_entity_class = entity_class.lower().capitalize()
    return normalized_entity_class

"""
Validate the provided trigger type

Parameters
----------
trigger_type : str
    One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger
"""
def validate_trigger_type(trigger_type):
    accepted_trigger_types = ['on_read_trigger', 'on_create_trigger', 'on_update_trigger']
    separator = ', '

    if trigger_type.lower() not in accepted_trigger_types:
        msg = "Invalid trigger type: " + trigger_type + ". The trigger type must be one of the following: " + separator.join(accepted_trigger_types)
        logger.error(msg)
        raise ValueError(msg)

"""
Validate the normalized entity class

Parameters
----------
normalized_entity_class : str
    The normalized entity class
"""
def validate_normalized_entity_class(normalized_entity_class):
    separator = ', '
    accepted_entity_classes = get_all_entity_classes()

    # Validate provided entity_class
    if normalized_entity_class not in accepted_entity_classes:
        msg = "Invalida entity class " + normalized_entity_class + ". The entity class must be one of the following: " + separator.join(accepted_entity_classes)
        logger.error(msg)
        raise ValueError(msg)

"""
Validate the source and target entity classes for creating derived entity

Parameters
----------
normalized_target_entity_class : str
    The normalized target entity class
"""
def validate_target_entity_class_for_derivation(normalized_target_entity_class):
    separator = ', '
    accepted_target_entity_classes = get_derivation_target_entity_classes()

    if normalized_target_entity_class not in accepted_target_entity_classes:
        bad_request_error("Invalid target entity class specified for creating the derived entity. Accepted classes: " + separator.join(accepted_target_entity_classes))

"""
Validate the source and target entity classes for creating derived entity

Parameters
----------
normalized_source_entity_class : str
    The normalized source entity class
"""
def validate_source_entity_class_for_derivation(normalized_source_entity_class):
    separator = ', '
    accepted_source_entity_classes = get_derivation_source_entity_classes()

    if normalized_source_entity_class not in accepted_source_entity_classes:
        bad_request_error("Invalid source entity class specified for creating the derived entity. Accepted classes: " + separator.join(accepted_source_entity_classes))


####################################################################################################
## Other functions used in conjuction with the trigger methods
####################################################################################################

"""
Get user infomation dict based on the http request(headers)
The result will be used by the trigger methods

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
    global _auth_helper

    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    user_info = _auth_helper.getUserInfoUsingRequest(request, True)

    logger.debug("======get_user_info()======")
    logger.debug(user_info)

    # If returns error response, invalid header or token
    if isinstance(user_info, Response):
        msg = "Failed to query the user info with the given globus token from the http request"
        logger.error(msg)
        raise Exception(msg)

    return user_info


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
    global _uuid_api_url

    target_url = _uuid_api_url + '/' + id
    request_headers = _create_request_headers()

    # Disable ssl certificate verification
    response = requests.get(url = target_url, headers = request_headers, verify = False) 
    
    if response.status_code == 200:
        ids_list = response.json()

        if len(ids_list) == 0:
            raise Exception("Could not find the target uuid via uuid-api: " + id)
        if len(ids_list) > 1:
            raise Exception("Found multiple records via uuid-api for id: " + id)
        
        return ids_list[0]
    else:
        raise requests.exceptions.RequestException("Failed to make a request to the target uuid via uuid-api: " + id)


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
displayDoi -> hubmap_id

Parameters
----------
normalized_class : str
    One of the classes defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset

Returns
-------
dict
    The dictionary of new ids

    {
        "uuid": "c754a4f878628f3c072d4e8024f707cd",
        "hubmap_id": "HBM479.NDDG.476"
    }

"""
def create_hubmap_ids(normalized_class):
    global _uuid_api_url

    # Must use "generateDOI": "true" to generate the doi (doi_suffix_id) and displayDoi (hubmap_id)
    ##############################
    # For sample and dataset, pass in the source_uuid
    # If sample and sample_type == organ, pass in the organ 
    ##############################
    
    json_to_post = {
        'entityType': normalized_class, 
        'generateDOI': "true"
    }

    request_headers = _create_request_headers()

    # Disable ssl certificate verification
    response = requests.post(url = _uuid_api_url, headers = request_headers, json = json_to_post, verify = False) 
    
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
        msg = "Failed to create new ids via the uuid-api service during the creation of this new " + normalized_class
        
        logger.error(msg)

        logger.debug("======create_new_ids() status code======")
        logger.debug(response.status_code)

        logger.debug("======create_new_ids() response text======")
        logger.debug(response.text)

        raise requests.exceptions.RequestException(msg)


####################################################################################################
## Internal functions
####################################################################################################

"""
Create a dict of HTTP Authorization header with Bearer token for making calls to uuid-api

Returns
-------
dict
    The headers dict to be used by requests
"""
def _create_request_headers():
    global _auth_helper

    auth_header_name = 'Authorization'
    auth_scheme = 'Bearer'
    token = _auth_helper.getProcessSecret()

    headers_dict = {
        # Don't forget the space between scheme and the token value
        auth_header_name: auth_scheme + ' ' + token
    }

    return headers_dict
