import yaml
import logging
import requests
from cachetools import cached, TTLCache
import functools
from urllib3.exceptions import InsecureRequestWarning
from flask import Response

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
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
dict
    A dictionary of trigger event methods generated data
"""
def generate_triggered_data(trigger_type, normalized_class, data_dict, properties_to_skip = []):
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
    # A list of property keys
    class_property_keys = list(properties) 
    
    # Set each property value and put all resulting data into a dictionary for:
    # before_create_trigger|before_update_trigger|on_read_trigger
    # No property value to be set for: after_create_trigger|after_update_trigger
    trigger_generated_data_dict = {}
    for key in class_property_keys:
        # Among those properties that have the target trigger type,
        # we can skip the ones specified in the `properties_to_skip` by not running their triggers
        if (trigger_type in list(properties[key])) and (key not in properties_to_skip):
            # 'after_create_trigger' and 'after_update_trigger' don't generate property values
            # E.g., create relationships between nodes in neo4j
            if trigger_type in ['after_create_trigger', 'after_update_trigger']:
                # Only call the triggers if the propery key presents from the incoming data
                # E.g., 'direct_ancestor_uuid' for Sample, 'dataset_uuids' for Collection
                if key in data_dict:
                    trigger_method_name = properties[key][trigger_type]

                    logger.debug("Calling schema " + trigger_type + ": " + trigger_method_name + " defined for " + normalized_class)

                    # Call the target trigger method of schema_triggers.py module
                    trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                    # Assume the trigger method failed by default
                    # Overwrite if everything goes well
                    trigger_generated_data_dict[trigger_method_name] = False

                    try:
                        # No return values for 'after_create_trigger' and 'after_update_trigger'
                        # because the property value is already set in `data_dict`
                        # normally it's building linkages between entity nodes
                        trigger_method_to_call(key, normalized_class, _neo4j_driver, data_dict)
                        
                        # Overwrite if the trigger method gets executed successfully
                        trigger_generated_data_dict[trigger_method_name] = True
                    except Exception:
                        # Log the full stack trace, prepend a line with our message
                        logger.exception("Failed to call the trigger method " + trigger_method_name)
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
                except Exception as e:
                    # Log the full stack trace, prepend a line with our message
                    logger.exception("Failed to call the trigger method " + trigger_method_name)
    
    # Return after for loop
    return trigger_generated_data_dict
                

"""
Generate the complete entity record as well as result filtering for response

Parameters
----------
normalized_class : str
    One of the classes defined in the schema yaml: Collection, Donor, Sample, Dataset
entity_dict : dict
    The entity dict based on neo4j record
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
dict
    A dictionary of complete entity with all all generated 'on_read_trigger' data
"""
def get_complete_entity_result(entity_dict, properties_to_skip = []):
    generated_on_read_trigger_data_dict = generate_triggered_data('on_read_trigger', entity_dict['entity_class'], entity_dict, properties_to_skip)

    # Merge the entity info and the generated on read data into one dictionary
    complete_entity_dict = {**entity_dict, **generated_on_read_trigger_data_dict}

    return complete_entity_dict


"""
Generate the complete entity records as well as result filtering for response

Parameters
----------
entities_list : list
    A list of entity dictionaries 
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
list
    A list a complete entity dictionaries with all the normalized information
"""
def get_complete_entities_list(entities_list, properties_to_skip = []):
    complete_entities_list = []

    for entity_dict in entities_list:
        complete_entity_dict = get_complete_entity_result(entity_dict, properties_to_skip)
        complete_entities_list.append(complete_entity_dict)

    return complete_entities_list


"""
Normalize the entity result by removing properties that are not defined in the yaml schema
and filter out the ones that are marked as `exposed: false` prior to sending the response

Parameters
----------
data_dict : dict
    A merged dictionary that contains all possible data to be used by the trigger methods
properties_to_exclude : list
    Any additional properties to exclude from the response

Returns
-------
dict
    A entity dictionary with keys that are all normalized
"""
def normalize_entity_result_for_response(entity_dict, properties_to_exclude = []):
    global _schema

    normalized_entity_class = entity_dict['entity_class']
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

            # Exclude additional properties if specified
            if key in properties_to_exclude:
                del entity_dict[key]

    return entity_dict


"""
Normalize the given list of complete entity results by removing properties that are not defined in the yaml schema
and filter out the ones that are marked as `exposed: false` prior to sending the response

Parameters
----------
entities_list : dict
    A merged dictionary that contains all possible data to be used by the trigger methods
properties_to_exclude : list
    Any additional properties to exclude from the response

Returns
-------
list
    A list of normalzied entity dictionaries
"""
def normalize_entities_list_for_response(entities_list, properties_to_exclude = []):
    normalized_entities_list = []

    for entity_dict in entities_list:
        normalized_entity_dict = normalize_entity_result_for_response(entity_dict, properties_to_exclude)
        normalized_entities_list.append(normalized_entity_dict)

    return normalized_entities_list


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
        # No need to log the validation errors
        raise KeyError("Unsupported keys in request json: " + separator.join(unsupported_keys))

    # Check if keys in request json are the ones to be auto generated
    generated_keys = []
    for key in json_data_keys:
        if ('generated' in properties[key]) and properties[key]['generated']:
            if properties[key]:
                generated_keys.append(key)

    if len(generated_keys) > 0:
        # No need to log the validation errors
        raise KeyError("Auto generated keys are not allowed in request json: " + separator.join(generated_keys))

    # Only check if keys in request json are immutable during entity update
    if not bool(existing_entity_dict):
        immutable_keys = []
        for key in json_data_keys:
            if ('immutable' in properties[key]) and properties[key]['immutable']:
                if properties[key]:
                    immutable_keys.append(key)

        if len(immutable_keys) > 0:
            # No need to log the validation errors
            raise KeyError("Immutable keys are not allowed in request json: " + separator.join(immutable_keys))
        
    # Check if any schema keys that are required_on_create but missing from POST request on creating new entity
    # No need to check on entity update
    if not bool(existing_entity_dict):    
        missing_required_keys_on_create = []
        for key in schema_keys:
            # By default, the schema treats all entity properties as optional no creation. 
            # Use `required_on_create: true` to mark a property as required for creating a new entity
            if 'required_on_create' in properties[key]:
                if properties[key]['required_on_create'] and ('trigger' not in properties[key]) and (key not in json_data_keys):
                    missing_required_keys_on_create.append(key)

        if len(missing_required_keys_on_create) > 0:
            # No need to log the validation errors
            raise KeyError("Missing required keys in request json: " + separator.join(missing_required_keys_on_create))

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
        # No need to log the validation errors
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
    accepted_trigger_types = ['on_read_trigger', 
                              'before_create_trigger', 
                              'before_update_trigger', 
                              'after_create_trigger', 
                              'after_update_trigger']
    separator = ', '

    if trigger_type.lower() not in accepted_trigger_types:
        msg = "Invalid trigger type: " + trigger_type + ". The trigger type must be one of the following: " + separator.join(accepted_trigger_types)
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
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
        msg = "Invalid entity class: " + normalized_entity_class + ". The entity class must be one of the following: " + separator.join(accepted_entity_classes)
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
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
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
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
        return ids_list[0]
    elif response.status_code == 404:
        msg = "Could not find the target id via uuid-api: " + id
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise requests.exceptions.HTTPError(msg)
    else:
        # uuid-api will also return 400 if the gien id is invalid
        # We'll just hanle that and all other cases all together here
        msg = "Failed to make a request to query the id via uuid-api: " + id
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise requests.exceptions.RequestException(msg)


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
        
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        logger.debug("======create_new_ids() status code======")
        logger.debug(response.status_code)

        logger.debug("======create_new_ids() response text======")
        logger.debug(response.text)

        raise requests.exceptions.RequestException(msg)

"""
Get the group info (group_uuid and group_name) based on user's hmgroupids list

Parameters
----------
user_hmgroupids_list : list
    A list of globus group uuids that the user has access to

Returns
-------
dict
    The group info (group_uuid and group_name)
"""
def get_entity_group_info(user_hmgroupids_list):
    # Default
    group_info = {
        'uuid': '',
        'name': ''
    }

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']

    # A list of data provider uuids
    data_provider_uuids = []
    for uuid_key in groups_by_id_dict:
        if 'data_provider' in groups_by_id_dict[uuid_key] and groups_by_id_dict[uuid_key]['data_provider']:
            data_provider_uuids.append(uuid_key)

    data_provider_groups = []
    for group_uuid in user_hmgroupids_list:
        if group_uuid in data_provider_uuids:
            data_provider_groups.append(group_uuid)

    if len(data_provider_groups) == 0:
        msg = "No data_provider groups found for this user. Can't continue."
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)

    if len(data_provider_groups) > 1:
        msg = "More than one data_provider groups found for this user. Can't continue."
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)

    # By now only one data provider group found, this is what we want
    group_info['uuid'] = data_provider_groups[0]
    group_info['name'] = groups_by_id_dict[uuid]['displayname']
    
    return group_info

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
