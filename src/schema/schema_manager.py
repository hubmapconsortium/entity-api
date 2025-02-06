import ast
import yaml
import logging
import requests
import unicodedata
import concurrent.futures
from flask import Response
from datetime import datetime

# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# Local modules
from schema import schema_errors
from schema import schema_triggers
from schema import schema_validators
from schema import schema_neo4j_queries
from schema.schema_constants import SchemaConstants
from schema.schema_constants import MetadataScopeEnum
from schema.schema_constants import TriggerTypeEnum

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper



logger = logging.getLogger(__name__)

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

# In Python, "privacy" depends on "consenting adults'" levels of agreement, we can't force it.
# A single leading underscore means you're not supposed to access it "from the outside"
_schema = None
_uuid_api_url = None
_ingest_api_url = None
_entity_api_url = None
_ontology_api_url = None
_auth_helper = None
_neo4j_driver = None
_memcached_client = None
_memcached_prefix = None
_organ_types = None


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
uuid_api_url : str
    The uuid-api base URL
ingest_api_url : str
    The ingest-api base URL
ontology_api_url : str
    The ontology-api base URL
auth_helper_instance : AuthHelper
    The auth helper instance
neo4j_driver_instance : neo4j_driver
    The Neo4j driver instance
memcached_client_instance : PooledClient
    The pooled client from Memcached connection
memcached_prefix : str
    The application-specifc prefix for Memcached data store
"""
def initialize(valid_yaml_file, 
               uuid_api_url,
               ingest_api_url,
               ontology_api_url,
               entity_api_url,
               auth_helper_instance,
               neo4j_driver_instance,
               memcached_client_instance,
               memcached_prefix):
    # Specify as module-scope variables
    global _schema
    global _uuid_api_url
    global _ingest_api_url
    global _ontology_api_url
    global _entity_api_url
    global _auth_helper
    global _neo4j_driver
    global _memcached_client
    global _memcached_prefix

    _schema = load_provenance_schema(valid_yaml_file)
    if uuid_api_url is not None:
        _uuid_api_url = uuid_api_url
    else:
        msg = f"Unable to initialize schema manager with uuid_api_url={uuid_api_url}."
        logger.critical(msg=msg)
        raise Exception(msg)

    if ingest_api_url is not None:
        _ingest_api_url = ingest_api_url
    else:
        msg = f"Unable to initialize schema manager with ingest_api_url={ingest_api_url}."
        logger.critical(msg=msg)
        raise Exception(msg)

    if ontology_api_url is not None:
        _ontology_api_url = ontology_api_url
    else:
        msg = f"Unable to initialize schema manager with ontology_api_url={ontology_api_url}."
        logger.critical(msg=msg)
        raise Exception(msg)

    if entity_api_url is not None:
        _entity_api_url = entity_api_url
    else:
        msg = f"Unable to initialize schema manager with entity_api_url={entity_api_url}."
        logger.critical(msg=msg)
        raise Exception(msg)
    # Get the helper instances
    _auth_helper = auth_helper_instance
    _neo4j_driver = neo4j_driver_instance
    _memcached_client = memcached_client_instance
    _memcached_prefix = memcached_prefix


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
def load_provenance_schema(valid_yaml_file):
    with open(valid_yaml_file) as file:
        schema_dict = yaml.safe_load(file)

        logger.info(f"Provenance Schema yaml file loaded successfully from {valid_yaml_file} :)")
        # For entities with properties set to None/Null, remove them as these represent private values not inherited by subclass
        for entity in schema_dict['ENTITIES']:
            schema_dict['ENTITIES'][entity]['properties'] = remove_none_values(schema_dict['ENTITIES'][entity]['properties'])
        return schema_dict


####################################################################################################
## Helper functions
####################################################################################################

"""
Get a list of all the supported types in the schema yaml

Returns
-------
list
    A list of types
"""
def get_all_types():
    global _schema

    entity_types = _schema['ENTITIES'].keys()
    activity_types = _schema['ACTIVITIES'].keys()

    # Need convert the dict_keys object to a list
    return list(entity_types) + list(activity_types)


"""
Get a list of all the supported entity types in the schema yaml

Returns
-------
list
    A list of entity types
"""
def get_all_entity_types():
    global _schema

    dict_keys = _schema['ENTITIES'].keys()
    # Need convert the dict_keys object to a list
    return list(dict_keys)


"""
Get the superclass (if defined) of the given entity class

Parameters
----------
normalized_entity_class : str
    The normalized target entity class

Returns
-------
string or None
    One of the normalized entity classes if defined (currently only Publication has Dataset as superclass). None otherwise
"""
def get_entity_superclass(normalized_entity_class):
    normalized_superclass = None

    all_entity_types = get_all_entity_types()

    if normalized_entity_class in all_entity_types:
        if 'superclass' in _schema['ENTITIES'][normalized_entity_class]:
            normalized_superclass = normalize_entity_type(_schema['ENTITIES'][normalized_entity_class]['superclass'])

            if normalized_superclass not in all_entity_types:
                msg = f"Invalid 'superclass' value defined for {normalized_entity_class}: {normalized_superclass}"
                logger.error(msg)
                raise ValueError(msg)
        else:
            # Since the 'superclass' property is optional, we just log the warning message, no need to bubble up
            msg = f"The 'superclass' property is not defined for entity class: {normalized_entity_class}"
            logger.warning(msg)

    return normalized_superclass


def entity_type_instanceof(entity_type: str, entity_class: str) -> bool:
    """
    Determine if the Entity type with 'entity_type' is an instance of 'entity_class'.
    Use this function if you already have the Entity type. Use entity_instanceof(uuid, class)
    if you just have the Entity uuid.

    :param entity_type: from Entity
    :param entity_class: found in .yaml file
    :return:  True or False
    """
    if entity_type is None:
        return False

    normalized_entry_class: str = normalize_entity_type(entity_class)
    super_entity_type: str = normalize_entity_type(entity_type)
    while super_entity_type is not None:
        if normalized_entry_class == super_entity_type:
            return True
        super_entity_type = get_entity_superclass(super_entity_type)
    return False


def entity_instanceof(entity_uuid: str, entity_class: str) -> bool:
    """
    Determine if the Entity with 'entity_uuid' is an instance of 'entity_class'.

    :param entity_uuid: from Entity
    :param entity_class: found in .yaml file
    :return: True or False
    """
    entity_type: str =\
        schema_neo4j_queries.get_entity_type(get_neo4j_driver_instance(), entity_uuid.strip())
    return entity_type_instanceof(entity_type, entity_class)


"""
Retrieves fields designated in the provenance schema yaml under 
excluded_properties_from_public_response and returns the fields in a list

Parameters
----------
normalized_class : str
    the normalized entity type of the entity who's fields are to be removed

Returns
-------
excluded_fields
    A list of strings where each entry is a field to be excluded
"""
def get_fields_to_exclude(normalized_class=None):
    # Determine the schema section based on class
    excluded_fields = []
    schema_section = _schema['ENTITIES']
    exclude_list = schema_section[normalized_class].get('excluded_properties_from_public_response')
    if exclude_list:
        excluded_fields.extend(exclude_list)
    return excluded_fields


"""
Removes specified fields from an existing dictionary

Parameters
----------
excluded_fields : list
    A list of the fields to be excluded
output_dict : dictionary
    A dictionary representing the data to be modified

Returns
-------
dict
    The modified data with removed fields
"""
def exclude_properties_from_response(excluded_fields, output_dict):
    def delete_nested_field(data, nested_path):
        if isinstance(nested_path, dict):
            for key, value in nested_path.items():
                if key in data:
                    if isinstance(value, list):
                        for nested_field in value:
                            if isinstance(nested_field, dict):
                                if isinstance(data[key], list):
                                    for item in data[key]:
                                        delete_nested_field(item, nested_field)
                                else:
                                    delete_nested_field(data[key], nested_field)
                            elif isinstance(data[key], list):
                                for item in data[key]:
                                    if nested_field in item:
                                        del item[nested_field]
                            elif nested_field in data[key]:
                                del data[key][nested_field]
                    elif isinstance(value, dict):
                        delete_nested_field(data[key], value)
        elif nested_path in data:
            if isinstance(data[nested_path], list):
                for item in data[nested_path]:
                    if nested_path in item:
                        del item[nested_path]
            else:
                del data[nested_path]

    for field in excluded_fields:
        delete_nested_field(output_dict, field)
    
    return output_dict


"""
Generating triggered data based on the target events and methods

Parameters
----------
trigger_type : str
    One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger
normalized_class : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token, 'on_read_trigger' doesn't really need this
existing_data_dict : dict
    A dictionary that contains existing entity data
new_data_dict : dict
    A dictionary that contains incoming entity data
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
dict
    A dictionary of trigger event methods generated data
"""
def generate_triggered_data(trigger_type: TriggerTypeEnum, normalized_class, user_token, existing_data_dict
                            , new_data_dict, properties_to_skip = []):
    global _schema

    schema_section = None

    # A bit validation
    validate_trigger_type(trigger_type)
    # Use validate_normalized_class instead of validate_normalized_entity_type()
    # to allow "Activity"
    validate_normalized_class(normalized_class)

    # Determine the schema section based on class
    if normalized_class == 'Activity':
        schema_section = _schema['ACTIVITIES']
    else:
        schema_section = _schema['ENTITIES']

    # The ordering of properties of this entity class defined in the yaml schema
    # decides the ordering of which trigger method gets to run first
    properties = schema_section[normalized_class]['properties']

    # Set each property value and put all resulting data into a dictionary for:
    # before_create_trigger|before_update_trigger|on_read_trigger
    # No property value to be set for: after_create_trigger|after_update_trigger
    trigger_generated_data_dict = {}
    for key in properties:
        # Among those properties that have the target trigger type,
        # we can skip the ones specified in the `properties_to_skip` by not running their triggers
        if (trigger_type.value in properties[key]) and (key not in properties_to_skip):
            # 'after_create_trigger' and 'after_update_trigger' don't generate property values
            # E.g., create relationships between nodes in neo4j
            # So just return the empty trigger_generated_data_dict
            if trigger_type in [TriggerTypeEnum.AFTER_CREATE, TriggerTypeEnum.AFTER_UPDATE]:
                # Only call the triggers if the propery key presents from the incoming data
                # E.g., 'direct_ancestor_uuid' for Sample, 'dataset_uuids' for Collection
                # This `existing_data_dict` is the newly created or updated entity dict
                if key in existing_data_dict:
                    trigger_method_name = properties[key][trigger_type.value]

                    try:
                        # Get the target trigger method defined in the schema_triggers.py module
                        trigger_method_to_call = getattr(schema_triggers, trigger_method_name)
                        
                        logger.info(f"To run {trigger_type.value}: {trigger_method_name} defined for {normalized_class}")

                        # No return values for 'after_create_trigger' and 'after_update_trigger'
                        # because the property value is already set and stored in neo4j
                        # Normally it's building linkages between entity nodes
                        # Use {} since no incoming new_data_dict 
                        trigger_method_to_call(key, normalized_class, user_token, existing_data_dict, {})
                    except Exception:
                        msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                        # Log the full stack trace, prepend a line with our message
                        logger.exception(msg)

                        if trigger_type == TriggerTypeEnum.AFTER_CREATE:
                            raise schema_errors.AfterCreateTriggerException
                        elif trigger_type.value == TriggerTypeEnum.AFTER_UPDATE:
                            raise schema_errors.AfterUpdateTriggerException
            elif trigger_type in [TriggerTypeEnum.BEFORE_UPDATE]:
                # IMPORTANT! Call the triggers for the properties:
                # Case 1: specified in request JSON to be updated explicitly
                # Case 2: defined as `auto_update: true` in the schema yaml, meaning will always be updated if the entity gets updated
                if (key in new_data_dict) or (('auto_update' in properties[key]) and properties[key]['auto_update']):
                    trigger_method_name = properties[key][trigger_type.value]
                    try:
                        trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                        logger.info(f"To run {trigger_type.value}: {trigger_method_name} defined for {normalized_class}")

                        # Will set the trigger return value as the property value by default
                        # Unless the return value is to be assigned to another property different target key
                        
                        #the updated_peripherally tag is a temporary measure to correctly handle any attributes
                        #which are potentially updated by multiple triggers
                        #we keep the state of the attribute(s) directly in the trigger_generated_data_dict
                        #dictionary, which is used to track and save all changes from triggers in general
                        #the trigger methods for the 'updated_peripherally' attributes take an extra argument,
                        #the trigger_generated_data_dict, and must initialize this dictionary with the value for
                        #the attribute from the existing_data_dict as well as make any updates to this attribute
                        #within this dictionary and return it so it can be saved in the scope of this loop and
                        #passed to other 'updated_peripherally' triggers                        
                        if 'updated_peripherally' in properties[key] and properties[key]['updated_peripherally']: 
                            trigger_generated_data_dict = trigger_method_to_call(key, normalized_class, user_token, existing_data_dict, new_data_dict, trigger_generated_data_dict)
                        else:
                            target_key, target_value = trigger_method_to_call(key, normalized_class, user_token, existing_data_dict, new_data_dict)
                            trigger_generated_data_dict[target_key] = target_value
                            
                            # Meanwhile, set the original property as None if target_key is different
                            # This is especially important when the returned target_key is different from the original key
                            # Because we'll be merging this trigger_generated_data_dict with the original user input
                            # and this will overwrite the original key so it doesn't get stored in Neo4j
                            if key != target_key:
                                trigger_generated_data_dict[key] = None
                    # If something wrong with file upload
                    except schema_errors.FileUploadException as e:
                        msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                        # Log the full stack trace, prepend a line with our message
                        logger.exception(msg)
                        raise schema_errors.FileUploadException(e)
                    except Exception as e:
                        msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                        # Log the full stack trace, prepend a line with our message
                        logger.exception(msg)

                        # We can't create/update the entity 
                        # without successfully executing this trigger method
                        raise schema_errors.BeforeUpdateTriggerException
            else:
                # Handling of all other trigger types: before_create_trigger|on_read_trigger
                trigger_method_name = properties[key][trigger_type.value]

                try:
                    trigger_method_to_call = getattr(schema_triggers, trigger_method_name)

                    logger.info(f"To run {trigger_type.value}: {trigger_method_name} defined for {normalized_class}")

                    # Will set the trigger return value as the property value by default
                    # Unless the return value is to be assigned to another property different target key
                    
                    # the updated_peripherally tag is a temporary measure to correctly handle any attributes
                    # which are potentially updated by multiple triggers
                    # we keep the state of the attribute(s) directly in the trigger_generated_data_dict
                    # dictionary, which is used to track and save all changes from triggers in general
                    # the trigger methods for the 'updated_peripherally' attributes take an extra argument,
                    # the trigger_generated_data_dict, and must initialize this dictionary with the value for
                    # the attribute from the existing_data_dict as well as make any updates to this attribute
                    # within this dictionary and return it so it can be saved in the scope of this loop and
                    # passed to other 'updated_peripherally' triggers                    
                    if 'updated_peripherally' in properties[key] and properties[key]['updated_peripherally']:
                        trigger_generated_data_dict = trigger_method_to_call(key, normalized_class, user_token, existing_data_dict, new_data_dict, trigger_generated_data_dict)
                    else:  
                        target_key, target_value = trigger_method_to_call(key, normalized_class, user_token, existing_data_dict, new_data_dict)
                        trigger_generated_data_dict[target_key] = target_value

                        # Meanwhile, set the original property as None if target_key is different
                        # This is especially important when the returned target_key is different from the original key
                        # Because we'll be merging this trigger_generated_data_dict with the original user input
                        # and this will overwrite the original key so it doesn't get stored in Neo4j
                        if key != target_key:
                            trigger_generated_data_dict[key] = None
                except schema_errors.NoDataProviderGroupException as e:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.NoDataProviderGroupException
                except schema_errors.MultipleDataProviderGroupException as e:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.MultipleDataProviderGroupException
                except schema_errors.UnmatchedDataProviderGroupException as e:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.UnmatchedDataProviderGroupException
                # If something wrong with file upload
                except schema_errors.FileUploadException as e:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    raise schema_errors.FileUploadException(e)
                except Exception as e:
                    msg = f"Failed to call the {trigger_type.value} method: {trigger_method_name}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(msg)
                    if trigger_type == TriggerTypeEnum.BEFORE_CREATE:
                        # We can't create/update the entity 
                        # without successfully executing this trigger method
                        raise schema_errors.BeforeCreateTriggerException
                    else:
                        # Assign the error message as the value of this property
                        # No need to raise exception
                        trigger_generated_data_dict[key] = msg
    
    # Return after for loop
    return trigger_generated_data_dict
    

"""
Filter out the merged dict by getting rid of properties with None values
This method is used by get_complete_entity_result() for the 'on_read_trigger'

Parameters
----------
merged_dict : dict
    A merged dict that may contain properties with None values

Returns
-------
dict
    A filtered dict that removed all properties with None values
"""
def remove_none_values(merged_dict):
    filtered_dict = {}
    for k, v in merged_dict.items():
        # Only keep the properties whose value is not None
        if v is not None:
            filtered_dict[k] = v

    return filtered_dict


"""
Filter out the merged_dict by getting rid of the transitent properties (not to be stored) 
and properties with None value
Meaning the returned target property key is different from the original key 
in the trigger method, e.g., Donor.image_files_to_add

Parameters
----------
merged_dict : dict
    A merged dict that may contain properties with None values
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor, Upload, Publication

Returns
-------
dict
    A filtered dict that removed all transient properties and the ones with None values
"""
def remove_transient_and_none_values(merged_dict, normalized_entity_type):
    global _schema

    properties = _schema['ENTITIES'][normalized_entity_type]['properties']

    filtered_dict = {}
    for k, v in merged_dict.items():
        # Only keep the properties that don't have `transitent` flag or are marked as `transitent: false`
        # and at the same time the property value is not None
        if (('transient' not in properties[k]) or ('transient' in properties[k] and not properties[k]['transient'])) and (v is not None):
            filtered_dict[k] = v 

    return filtered_dict


"""
Generate the complete entity record by running the read triggers

Parameters
----------
token: str
    Either the user's globus nexus token or the internal token
entity_dict : dict
    The entity dict based on neo4j record
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
dict
    A dictionary of complete entity with all the generated 'on_read_trigger' data
"""
def get_complete_entity_result(token, entity_dict, properties_to_skip = []):
    global _memcached_client
    global _memcached_prefix

    complete_entity = {}

    # In case entity_dict is None or 
    # an incorrectly created entity that doesn't have the `entity_type` property
    if entity_dict and ('entity_type' in entity_dict) and ('uuid' in entity_dict):
        entity_uuid = entity_dict['uuid']
        entity_type = entity_dict['entity_type']
        cache_result = None

        # Need both client and prefix when fetching the cache
        # Do NOT fetch cache if properties_to_skip is specified
        if _memcached_client and _memcached_prefix and (not properties_to_skip):
            cache_key = f'{_memcached_prefix}_complete_{entity_uuid}'
            cache_result = _memcached_client.get(cache_key)

        # Use the cached data if found and still valid
        # Otherwise, calculate and add to cache
        if cache_result is None:
            if _memcached_client and _memcached_prefix:
                logger.info(f'Cache of complete entity of {entity_type} {entity_uuid} not found or expired at time {datetime.now()}')
            
            # No error handling here since if a 'on_read_trigger' method fails, 
            # the property value will be the error message
            # Pass {} since no new_data_dict for 'on_read_trigger'
            generated_on_read_trigger_data_dict = generate_triggered_data(  trigger_type=TriggerTypeEnum.ON_READ
                                                                            , normalized_class=entity_type
                                                                            , user_token=token
                                                                            , existing_data_dict=entity_dict
                                                                            , new_data_dict={}
                                                                            , properties_to_skip=properties_to_skip)

            # Merge the entity info and the generated on read data into one dictionary
            complete_entity_dict = {**entity_dict, **generated_on_read_trigger_data_dict}

            # Remove properties of None value
            complete_entity = remove_none_values(complete_entity_dict)

            # Need both client and prefix when creating the cache
            # Do NOT cache when properties_to_skip is specified
            if _memcached_client and _memcached_prefix and (not properties_to_skip):
                logger.info(f'Creating complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}')

                cache_key = f'{_memcached_prefix}_complete_{entity_uuid}'
                _memcached_client.set(cache_key, complete_entity, expire = SchemaConstants.MEMCACHED_TTL)

                logger.debug(f"Following is the complete {entity_type} cache created at time {datetime.now()} using key {cache_key}:")
                logger.debug(complete_entity)
        else:
            logger.info(f'Using complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}')
            logger.debug(cache_result)

            complete_entity = cache_result
    else:
        # Just return the original entity_dict otherwise
        complete_entity = entity_dict

    # One final return
    return complete_entity

"""
Generate the entity metadata by reading Neo4j data and only running triggers for data which will go into an
OpenSearch document. Any data from Neo4j which will not go into the OSS document must also be removed e.g.
local_directory_rel_path.

Parameters
----------
token: str
    Either the user's globus nexus token or the internal token
entity_dict : dict
    The entity dict based on neo4j record
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
dict
    A dictionary of metadata to be included in an OpenSearch index document for the entity.
"""
def get_index_metadata(token, entity_dict, properties_to_skip=[]):
    metadata_dict = _get_metadata_result(   token=token
                                            ,entity_dict=entity_dict
                                            ,metadata_scope=MetadataScopeEnum.INDEX
                                            ,properties_to_skip=properties_to_skip)
    return metadata_dict

"""
Generate the entity metadata by reading Neo4j data and appropriate triggers based upon the scope of
metadata requested e.g. complete data for a another service, indexing data for an OpenSearch document, etc.

Parameters
----------
token: str
    Either the user's globus nexus token or the internal token
entity_dict : dict
    The entity dict based on neo4j record
metadata_scope:
    A recognized scope from the SchemaConstants, controlling the triggers which are fired and elements
    from Neo4j which are retained.
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
dict
    A dictionary of metadata appropriate for the metadata_scope argument value.
"""
def _get_metadata_result(token, entity_dict, metadata_scope:MetadataScopeEnum, properties_to_skip=[]):
    global _memcached_client
    global _memcached_prefix

    complete_entity = {}

    # In case entity_dict is None or
    # an incorrectly created entity that doesn't have the `entity_type` property
    if entity_dict and ('entity_type' in entity_dict) and ('uuid' in entity_dict):
        entity_uuid = entity_dict['uuid']
        entity_type = entity_dict['entity_type']
        cache_result = None

        # Need both client and prefix when fetching the cache
        # Do NOT fetch cache if properties_to_skip is specified
        if _memcached_client and _memcached_prefix and (not properties_to_skip):
            cache_key = f'{_memcached_prefix}_complete_index_{entity_uuid}'
            cache_result = _memcached_client.get(cache_key)

        # Use the cached data if found and still valid
        # Otherwise, calculate and add to cache
        if cache_result is None:
            if _memcached_client and _memcached_prefix:
                logger.info(
                    f'Cache of complete entity of {entity_type} {entity_uuid} not found or expired at time {datetime.now()}')

            if metadata_scope == MetadataScopeEnum.COMPLETE:
                # No error handling here since if a 'on_read_trigger' method fails,
                # the property value will be the error message
                # Pass {} since no new_data_dict for 'on_read_trigger'
                #generated_on_read_trigger_data_dict = generate_triggered_data('on_read_trigger', entity_type, token,
                #                                                              entity_dict, {}, properties_to_skip)
                generated_on_read_trigger_data_dict = generate_triggered_data(  trigger_type=TriggerTypeEnum.ON_READ
                                                                                , normalized_class=entity_type
                                                                                , user_token=token
                                                                                , existing_data_dict=entity_dict
                                                                                , new_data_dict={}
                                                                                , properties_to_skip=properties_to_skip)

                # Merge the entity info and the generated on read data into one dictionary
                complete_entity_dict = {**entity_dict, **generated_on_read_trigger_data_dict}

                # Remove properties of None value
                metadata_dict = remove_none_values(complete_entity_dict)
            elif metadata_scope == MetadataScopeEnum.INDEX:
                # No error handling here since if a 'on_index_trigger' method fails,
                # the property value will be the error message
                # Pass {} since no new_data_dict for 'on_index_trigger'
                generated_on_index_trigger_data_dict = generate_triggered_data( trigger_type=TriggerTypeEnum.ON_INDEX
                                                                                , normalized_class=entity_type
                                                                                , user_token=token
                                                                                , existing_data_dict=entity_dict
                                                                                , new_data_dict={}
                                                                                , properties_to_skip=properties_to_skip)

                # Merge the entity info and the generated on read data into one dictionary
                complete_entity_dict = {**entity_dict, **generated_on_index_trigger_data_dict}

                # Remove properties of None value
                metadata_dict = remove_none_values(complete_entity_dict)
            else:
                # Merge the entity info and the generated on read data into one dictionary
                metadata_dict = {**entity_dict}

            # Need both client and prefix when creating the cache
            # Do NOT cache when properties_to_skip is specified
            if _memcached_client and _memcached_prefix and (not properties_to_skip):
                logger.info(f'Creating complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}')

                cache_key = f'{_memcached_prefix}_complete_index_{entity_uuid}'
                _memcached_client.set(cache_key, metadata_dict, expire=SchemaConstants.MEMCACHED_TTL)

                logger.debug(
                    f"Following is the complete {entity_type} cache created at time {datetime.now()} using key {cache_key}:")
                logger.debug(metadata_dict)
        else:
            logger.info(f'Using complete entity cache of {entity_type} {entity_uuid} at time {datetime.now()}')
            logger.debug(cache_result)

            metadata_dict = cache_result
    else:
        # Just return the original entity_dict otherwise
        metadata_dict = entity_dict

    # One final return
    return metadata_dict


"""
Generate the complete entity records as well as result filtering for response

Parameters
----------
token: str
    Either the user's globus nexus token or the internal token
entities_list : list
    A list of entity dictionaries 
properties_to_skip : list
    Any properties to skip running triggers

Returns
-------
list
    A list a complete entity dictionaries with all the normalized information
"""
def get_complete_entities_list(token, entities_list, properties_to_skip = []):
    complete_entities_list = []
    
    # Use a pool of threads to execute the time-consuming iteration asynchronously to avoid timeout - Zhou 2/6/2025
    with concurrent.futures.ThreadPoolExecutor() as executor:
        helper_func = lambda args: get_complete_entity_result(args[0], args[1], args[2])
        args_list = [(token, entity_dict, properties_to_skip) for entity_dict in entities_list]

        # The order of donors/organs/samples lists are not gurenteed with using `executor.submit()`
        # `executor.map()` maintains the same order of results as the original submitted tasks
        for result in executor.map(helper_func, args_list):
            complete_entities_list.append(result)

    return complete_entities_list


"""
Normalize the activity result by filtering out properties that are not defined in the yaml schema
and the ones that are marked as `exposed: false` prior to sending the response

Parameters
----------
activity_dict : dict
    A dictionary that contains all activity details

Returns
-------
dict
    A dictionary of activity information with keys that are all normalized
"""
def normalize_activity_result_for_response(activity_dict):
    global _schema

    properties = _schema['ACTIVITIES']['Activity']['properties']

    normalized_activity = {}
    for key in activity_dict:
        # Only return the properties defined in the schema yaml
        # Exclude additional properties if specified
        if key in properties:
            # By default, all properties are exposed
            # It's possible to see `exposed: true`
            if ('exposed' not in properties[key]) or (('exposed' in properties[key]) and properties[key]['exposed']):
                # Add to the normalized_activity dict
                normalized_activity[key] = activity_dict[key]

    return normalized_activity


"""
Normalize the entity result by filtering out properties that are not defined in the yaml schema
and the ones that are marked as `exposed: false` prior to sending the response

Parameters
----------
entity_dict : dict
    Either a neo4j node converted dict or complete dict generated from get_complete_entity_result()
properties_to_exclude : list
    Any additional properties to exclude from the response

Returns
-------
dict
    A entity dictionary with keys that are all normalized
"""
def normalize_entity_result_for_response(entity_dict, properties_to_exclude = []):
    global _schema

    normalized_entity = {}

    # In case entity_dict is None or 
    # an incorrectly created entity that doesn't have the `entity_type` property
    if entity_dict and ('entity_type' in entity_dict):
        normalized_entity_type = entity_dict['entity_type']
        properties = _schema['ENTITIES'][normalized_entity_type]['properties']

        for key in entity_dict:
            # Only return the properties defined in the schema yaml
            # Exclude additional properties if specified
            if (key in properties) and (key not in properties_to_exclude):
                # Skip properties with None value and the ones that are marked as not to be exposed.
                # By default, all properties are exposed if not marked as `exposed: false`
                # It's still possible to see `exposed: true` marked explictly
                if (entity_dict[key] is not None) and ('exposed' not in properties[key]) or (('exposed' in properties[key]) and properties[key]['exposed']): 
                    # Only run convert_str_literal() on string representation of Python dict and list with removing control characters
                    # No convertion for string representation of Python string, meaning that can still contain control characters
                    if entity_dict[key] and (properties[key]['type'] in ['list', 'json_string']):
                        logger.info(f"Executing convert_str_literal() on {normalized_entity_type}.{key} of uuid: {entity_dict['uuid']}")

                        # Safely evaluate a string containing a Python dict or list literal
                        # Only convert to Python list/dict when the string literal is not empty
                        # instead of returning the json-as-string or array-as-string
                        # convert_str_literal() also removes those control chars to avoid SyntaxError
                        entity_dict[key] = convert_str_literal(entity_dict[key])
                    
                    # Add the target key with correct value of data type to the normalized_entity dict
                    normalized_entity[key] = entity_dict[key]

                    # Final step: remove properties with empty string value, empty dict {}, and empty list []
                    if (isinstance(normalized_entity[key], (str, dict, list)) and (not normalized_entity[key])):
                        normalized_entity.pop(key)

    return normalized_entity

"""
Normalize the entity result to got into the OpenSearch index by filtering out properties that are not defined in
the yaml schema, properties marked as `exposed: false` in the yaml schema, and properties lacking `indexed: true`
marking in the yaml schema.

Parameters
----------
entity_dict : dict
    Either a neo4j node converted dict or metadata dict generated from get_index_metadata()
properties_to_exclude : list
    Any additional properties to exclude from the response

Returns
-------
dict
    An entity metadata dictionary with keys that are all normalized
"""
def normalize_document_result_for_response(entity_dict, properties_to_exclude=[]):
    return _normalize_metadata( entity_dict=entity_dict
                                , metadata_scope=MetadataScopeEnum.INDEX
                                , properties_to_skip=properties_to_exclude)

"""
Normalize the entity result by filtering the properties to those appropriate for the
scope of metadata requested e.g. complete data for a another service, indexing data for an OpenSearch document, etc.

Properties that are not defined in the yaml schema and properties marked as `exposed: false` in the yaml schema are
removed. Properties are also filter based upon the metadata_scope argument e.g. properties lacking `indexed: true`
marking in the yaml schema are removed when `metadata_scope` has a value of `MetadataScopeEnum.INDEX`.

Parameters
----------
entity_dict : dict
    Either a neo4j node converted dict or metadata dict generated from get_index_metadata()
metadata_scope:
    A recognized scope from the SchemaConstants, controlling the triggers which are fired and elements
    from Neo4j which are retained.  Default is MetadataScopeEnum.INDEX.
properties_to_exclude : list
    Any additional properties to exclude from the response

Returns
-------
dict
    An entity metadata dictionary with keys that are all normalized appropriately for the metadata_scope argument value.
"""
def _normalize_metadata(entity_dict, metadata_scope:MetadataScopeEnum, properties_to_skip=[]):
    global _schema

    # When the entity_dict is unavailable or the entity was incorrectly created, do not
    # try to normalize.
    if not entity_dict or 'entity_type' not in entity_dict:
        return {}

    normalized_metadata = {}

    normalized_entity_type = entity_dict['entity_type']
    properties = _schema['ENTITIES'][normalized_entity_type]['properties']

    for key in entity_dict:
        # Only return the properties defined in the schema yaml
        # Exclude additional schema yaml properties, if specified
        if  key not in properties:
            # Skip Neo4j entity properties not found in the schema yaml
            continue
        if  key in properties_to_skip:
            # Skip properties if directed by the calling function
            continue
        if  entity_dict[key] is None:
            # Do not include properties in the metadata if they are empty
            continue
        if  'exposed' in properties[key] and \
            properties[key]['exposed'] is False:
            # Do not include properties in the metadata if they are not exposed
            continue
        if  metadata_scope is MetadataScopeEnum.INDEX and \
            'indexed' in properties[key] and \
            properties[key]['indexed'] is False:
            # Do not include properties in metadata for indexing if they are not True i.e. False or non-boolean
            continue
        # Only run convert_str_literal() on string representation of Python dict and list with removing control characters
        # No convertion for string representation of Python string, meaning that can still contain control characters
        if entity_dict[key] and (properties[key]['type'] in ['list', 'json_string']):
            logger.info(
                f"Executing convert_str_literal() on {normalized_entity_type}.{key} of uuid: {entity_dict['uuid']}")

            # Safely evaluate a string containing a Python dict or list literal
            # Only convert to Python list/dict when the string literal is not empty
            # instead of returning the json-as-string or array-as-string
            # convert_str_literal() also removes those control chars to avoid SyntaxError
            entity_dict[key] = convert_str_literal(entity_dict[key])

        # Add the target key with correct value of data type to the normalized_entity dict
        normalized_metadata[key] = entity_dict[key]

        # After possible modification to entity_dict[key] prior to assigning to normalized_metadata[key], remove
        # the normalized_metadata entry for the key if it is an empty string, dictionary, or list.
        if (isinstance(normalized_metadata[key], (str, dict, list)) and (not normalized_metadata[key])):
            normalized_metadata.pop(key)

    return normalized_metadata

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
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor, Upload, Publication
existing_entity_dict : dict
    Entity dict for creating new entity, otherwise pass in the existing entity dict for update validation
"""
def validate_json_data_against_schema(json_data_dict, normalized_entity_type, existing_entity_dict = {}):
    global _schema

    properties = _schema['ENTITIES'][normalized_entity_type]['properties']
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
        raise schema_errors.SchemaValidationException(f"Unsupported keys in request json: {separator.join(unsupported_keys)}")

    # Check if keys in request json are the ones to be auto generated
    # Disallow direct creation via POST, but allow update via PUT
    generated_keys = []
    if not existing_entity_dict:
        for key in json_data_keys:
            if ('generated' in properties[key]) and properties[key]['generated']:
                if properties[key]:
                    generated_keys.append(key)

    if len(generated_keys) > 0:
        # No need to log the validation errors
        raise schema_errors.SchemaValidationException(f"Auto generated keys are not allowed in request json: {separator.join(generated_keys)}")

    # Checks for entity update via HTTP PUT
    if existing_entity_dict:
        # Check if keys in request json are immutable 
        immutable_keys = []
        for key in json_data_keys:
            if ('immutable' in properties[key]) and properties[key]['immutable']:
                if properties[key]:
                    immutable_keys.append(key)

        if len(immutable_keys) > 0:
            # No need to log the validation errors
            raise schema_errors.SchemaValidationException(f"Immutable keys are not allowed in request json: {separator.join(immutable_keys)}")
        
    # Check if any schema keys that are `required_on_create: true` but missing from POST request on creating new entity
    # No need to check on entity update
    if not existing_entity_dict:    
        missing_required_keys_on_create = []
        empty_value_of_required_keys_on_create = []
        for key in schema_keys:
            # By default, the schema treats all entity properties as optional on creation. 
            # Use `required_on_create: true` to mark a property as required for creating a new entity
            if ('required_on_create' in properties[key]) and properties[key]['required_on_create']:
                if key not in json_data_keys:
                    missing_required_keys_on_create.append(key)
                else:
                    # Empty values or None(null in request json) of required keys are invalid too
                    # The data type check will be handled later regardless of it's required or not
                    if (json_data_dict[key] is None) or (isinstance(json_data_dict[key], (list, dict)) and (not json_data_dict[key])) or (isinstance(json_data_dict[key], str) and (not json_data_dict[key].strip())):
                        empty_value_of_required_keys_on_create.append(key)

        if len(missing_required_keys_on_create) > 0:
            # No need to log the validation errors
            raise schema_errors.SchemaValidationException(f"Missing required keys in request json: {separator.join(missing_required_keys_on_create)}")

        if len(empty_value_of_required_keys_on_create) > 0:
            # No need to log the validation errors
            raise schema_errors.SchemaValidationException(f"Required keys in request json with empty values: {separator.join(empty_value_of_required_keys_on_create)}")

    # Verify data type of each key
    invalid_data_type_keys = []
    for key in json_data_keys:
        # boolean starts with bool, string starts with str, integer starts with int, list is list
        property_type = properties[key]['type']
        if isinstance(property_type, str):
            if (property_type in ['string', 'integer', 'list', 'boolean']) and (not property_type.startswith(type(json_data_dict[key]).__name__)):
                invalid_data_type_keys.append(key)
        elif isinstance(property_type, list):
            if not any(item.startswith(type(json_data_dict[key]).__name__) for item in property_type):
                invalid_data_type_keys.append(key)
        # Handling json_string as dict
        if (properties[key]['type'] == 'json_string') and (not isinstance(json_data_dict[key], dict)):
            invalid_data_type_keys.append(key)
    
    if len(invalid_data_type_keys) > 0:
        # No need to log the validation errors
        raise schema_errors.SchemaValidationException(f"Keys in request json with invalid data types: {separator.join(invalid_data_type_keys)}")
    

"""
Execute the entity level validator of the given type defined in the schema yaml 
before entity creation via POST or entity update via PUT
Only one validator defined per given validator type, no need to support multiple validators

Parameters
----------
validator_type : str
    One of the validator types: before_entity_create_validator
normalized_entity_type : str
    One of the normalized entity types defined in the schema yaml: Donor, Sample, Dataset, Upload, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
"""
def execute_entity_level_validator(validator_type, normalized_entity_type, request):
    global _schema

    # A bit validation
    validate_entity_level_validator_type(validator_type)
    validate_normalized_entity_type(normalized_entity_type)

    entity = _schema['ENTITIES'][normalized_entity_type]

    for key in entity:
         if validator_type == key:
            validator_method_name = entity[validator_type]

            try:
                # Get the target validator method defined in the schema_validators.py module
                validator_method_to_call = getattr(schema_validators, validator_method_name)
                
                logger.info(f"To run {validator_type}: {validator_method_name} defined for entity {normalized_entity_type}")

                validator_method_to_call(normalized_entity_type, request)
            except schema_errors.MissingApplicationHeaderException as e: 
                raise schema_errors.MissingApplicationHeaderException(e) 
            except schema_errors.InvalidApplicationHeaderException as e: 
                raise schema_errors.InvalidApplicationHeaderException(e)
            except Exception as e:
                msg = f"Failed to call the {validator_type} method: {validator_method_name} defined for entity {normalized_entity_type}"
                # Log the full stack trace, prepend a line with our message
                logger.exception(msg)


"""
Execute the property level validators defined in the schema yaml 
before property update via PUT

Parameters
----------
validator_type : str
    before_property_create_validators|before_property_update_validators (support multiple validators)
normalized_entity_type : str
    One of the normalized entity types defined in the schema yaml: Donor, Sample, Dataset, Upload, Publication
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties, {} for before_property_create_validators
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def execute_property_level_validators(validator_type, normalized_entity_type, request, existing_data_dict, new_data_dict):
    global _schema

    schema_section = None

    # A bit validation
    validate_property_level_validator_type(validator_type)
    validate_normalized_entity_type(normalized_entity_type)

    properties = _schema['ENTITIES'][normalized_entity_type]['properties']

    for key in properties:
        # Only run the validators for keys present in the request json
        if (key in new_data_dict) and (validator_type in properties[key]):
            # Get a list of defined validators on this property
            validators_list = properties[key][validator_type]
            # Run each validator defined on this property
            for validator_method_name in validators_list:
                try:
                    # Get the target validator method defined in the schema_validators.py module
                    validator_method_to_call = getattr(schema_validators, validator_method_name)
                    
                    logger.info(f"To run {validator_type}: {validator_method_name} defined for entity {normalized_entity_type} on property {key}")

                    validator_method_to_call(key, normalized_entity_type, request, existing_data_dict, new_data_dict)
                except schema_errors.MissingApplicationHeaderException as e: 
                    raise schema_errors.MissingApplicationHeaderException(e) 
                except schema_errors.InvalidApplicationHeaderException as e: 
                    raise schema_errors.InvalidApplicationHeaderException(e)
                except ValueError as ve:
                    raise ValueError(ve)
                except schema_errors.UnimplementedValidatorException as uve:
                    msg = f"Failed to call the {validator_type} method: {validator_method_name} defined for entity {normalized_entity_type} on property {key}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(f"{msg}. {str(uve)}")
                    raise uve
                except Exception as e:
                    msg = f"Unexpected exception calling {validator_type} method: {validator_method_name} defined for entity {normalized_entity_type} on property {key}"
                    # Log the full stack trace, prepend a line with our message
                    logger.exception(f"{msg}. {str(e)}")
                    raise e

"""
Get a list of entity types that can be used as derivation source in the schmea yaml

Returns
-------
list
    A list of entity types
"""
def get_derivation_source_entity_types():
    global _schema

    derivation_source_entity_types = []
    entity_types = get_all_entity_types()
    for entity_type in entity_types:
        if _schema['ENTITIES'][entity_type]['derivation']['source']:
            derivation_source_entity_types.append(entity_type)

    return derivation_source_entity_types

"""
Get a list of entity types that can be used as derivation target in the schmea yaml

Returns
-------
list
    A list of entity types
"""
def get_derivation_target_entity_types():
    global _schema

    derivation_target_entity_types = []
    entity_types = get_all_entity_types()
    for entity_type in entity_types:
        if _schema['ENTITIES'][entity_type]['derivation']['target']:
            derivation_target_entity_types.append(entity_type)

    return derivation_target_entity_types

"""
Lowercase and captalize the entity type string

Parameters
----------
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor, Upload, Publication
id : str
    The uuid of target entity 

Returns
-------
string
    One of the normalized entity types: Dataset, Collection, Sample, Donor, Upload, Publication
"""
def normalize_entity_type(entity_type):
    normalized_entity_type = entity_type.lower().capitalize()
    return normalized_entity_type

"""
Lowercase and capitalize the status string unless its "qa", then make it capitalized.

Parameters
----------
status : str
    One of the status types: New|Processing|QA|Published|Error|Hold|Invalid
    
Returns
-------
string
    One of the normalized status types: New|Processing|QA|Published|Error|Hold|Invalid
"""
def normalize_status(status):
    if status.lower() == "qa":
        normalized_status = "QA"
    else:
        normalized_status = status.lower().capitalize()
    return normalized_status

"""
Validate the provided trigger type

Parameters
----------
trigger_type : str
    One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger
"""
def validate_trigger_type(trigger_type:TriggerTypeEnum):

    if trigger_type not in TriggerTypeEnum:
        msg = f"Invalid trigger type: {trigger_type}. The trigger type must be one of the following: {', '.join([t.value for t in TriggerTypeEnum])}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)

"""
Validate the provided entity level validator type

Parameters
----------
validator_type : str
    One of the validator types: before_entity_create_validator
"""
def validate_entity_level_validator_type(validator_type):
    accepted_validator_types = ['before_entity_create_validator']
    separator = ', '

    if validator_type.lower() not in accepted_validator_types:
        msg = f"Invalid validator type: {validator_type}. The validator type must be one of the following: {separator.join(accepted_validator_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)

"""
Validate the provided property level validator type

Parameters
----------
validator_type : str
    One of the validator types: before_property_create_validators|before_property_update_validators
"""
def validate_property_level_validator_type(validator_type):
    accepted_validator_types = ['before_property_create_validators', 'before_property_update_validators']
    separator = ', '

    if validator_type.lower() not in accepted_validator_types:
        msg = f"Invalid validator type: {validator_type}. The validator type must be one of the following: {separator.join(accepted_validator_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise ValueError(msg)

"""
Validate the normalized entity class

Parameters
----------
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor, Upload, Publication
"""
def validate_normalized_entity_type(normalized_entity_type):
    separator = ', '
    accepted_entity_types = get_all_entity_types()

    # Validate provided entity_type
    if normalized_entity_type not in accepted_entity_types:
        msg = f"Invalid entity class: {normalized_entity_type}. The entity class must be one of the following: {separator.join(accepted_entity_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.InvalidNormalizedEntityTypeException(msg)

"""
Validate the normalized class

Parameters
----------
normalized_class : str
    The normalized class: Activity|Collection|Donor|Sample|Dataset|Publication
"""
def validate_normalized_class(normalized_class):
    separator = ', '
    accepted_types = get_all_types()

    # Validate provided entity_type
    if normalized_class not in accepted_types:
        msg = f"Invalid class: {normalized_class}. The class must be one of the following: {separator.join(accepted_types)}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.InvalidNormalizedTypeException(msg)


"""
Validate the source and target entity types for creating derived entity

Parameters
----------
normalized_target_entity_type : str
    The normalized target entity class
"""
def validate_target_entity_type_for_derivation(normalized_target_entity_type):
    separator = ', '
    accepted_target_entity_types = get_derivation_target_entity_types()

    if normalized_target_entity_type not in accepted_target_entity_types:
        bad_request_error(f"Invalid target entity type specified for creating the derived entity. Accepted types: {separator.join(accepted_target_entity_types)}")


"""
Validate the source and target entity types for creating derived entity

Parameters
----------
normalized_source_entity_type : str
    The normalized source entity class
"""
def validate_source_entity_type_for_derivation(normalized_source_entity_type):
    separator = ', '
    accepted_source_entity_types = get_derivation_source_entity_types()

    if normalized_source_entity_type not in accepted_source_entity_types:
        bad_request_error(f"Invalid source entity class specified for creating the derived entity. Accepted types: {separator.join(accepted_source_entity_types)}")


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

    logger.info("======get_user_info()======")
    logger.info(user_info)

    # For debugging purposes
    try:
        auth_helper_instance = get_auth_helper_instance()
        token = auth_helper_instance.getAuthorizationTokens(request.headers)
        groups_list = auth_helper_instance.get_user_groups_deprecated(token)

        logger.info("======Groups using get_user_groups_deprecated()======")
        logger.info(groups_list)
    except Exception:
        msg = "For debugging purposes, failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

    # It returns error response when:
    # - invalid header or token
    # - token is valid but not nexus token, can't find group info
    if isinstance(user_info, Response):
        # Bubble up the actual error message from commons
        # The Response.data returns binary string, need to decode
        msg = user_info.get_data().decode()
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise Exception(msg)

    return user_info


"""
Retrive target uuid, hubmap_id, and submission_id based on the given id

Parameters
----------
id : str
    Either the uuid or hubmap_id of target entity

Returns
-------
dict
    The dict returned by uuid-api that contains all the associated ids, e.g.:
    Only Donor and Sample have `submission_id`
    {
        "ancestor_id": "23c0ffa90648358e06b7ac0c5673ccd2",
        "ancestor_ids":[
            "23c0ffa90648358e06b7ac0c5673ccd2"
        ],
        "email": "marda@ufl.edu",
        "hm_uuid": "1785aae4f0fb8f13a56d79957d1cbedf",
        "hubmap_id": "HBM966.VNKN.965",
        "submission_id": "UFL0007",
        "time_generated": "2020-10-19 15:52:02",
        "type": "DONOR",
        "user_id": "694c6f6a-1deb-41a6-880f-d1ad8af3705f"
    }
"""
def get_hubmap_ids(id):
    global _uuid_api_url

    target_url = _uuid_api_url + SchemaConstants.UUID_API_ID_ENDPOINT + '/' + id

    # Use Memcached to improve performance
    response = make_request_get(target_url, internal_token_used = True)

    # Invoke .raise_for_status(), an HTTPError will be raised with certain status codes
    response.raise_for_status()

    if response.status_code == 200:
        ids_dict = response.json()
        return ids_dict
    else:
        # uuid-api will also return 400 if the given id is invalid
        # We'll just hanle that and all other cases all together here
        msg = f"Unable to make a request to query the id via uuid-api: {id}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        logger.debug("======get_hubmap_ids() status code from uuid-api======")
        logger.debug(response.status_code)

        logger.debug("======get_hubmap_ids() response text from uuid-api======")
        logger.debug(response.text)

        # Also bubble up the error message from uuid-api
        raise requests.exceptions.RequestException(response.text)


"""
Helper function to use the Ontology API to retrieve a valueset from UBKG containing
allowed values for soft assays, which can be set on the beginning of (part before
square brackets containing anything) the Dataset dataset_type field.

Examples of valid dataset_type values are "RNASeq" and "CODEX [cytokit, image_pyramid]" 

Parameters
----------
N/A: This help encapsulates hard-coded strings for soft assay values from the HUBMAP
     source vocabulary of UBKG.

Returns
-------
List of String values for each element in the UBKG valueset for valid dataset_type soft assay entries.
['Histology','Molecular Cartography',...]
"""
def get_dataset_type_valueset_list():
    # Use the Ontology API to get JSON for allowed terms.
    ubkg_valueset = get_valueset(ontology_app_context='HUBMAP')
    # Extract the term elements from the JSON into a list to be returned.
    return [v['dataset_type'] for v in ubkg_valueset]

"""
Use the Ontology API valueset endpoint to retrieve the UBKG valueset for a particular
"parent" vocabulary & term.  The preferred vocabulary which each "child" element of the valueset
comes from is also specified.

Parameters
----------
parent_vocabulary_sab: The source vocabulary (SAB) recognized by UBKG to which parent_vocabulary_valueset_code belongs.

parent_vocabulary_valueset_code: A code from parent_vocabulary_sab which is the parent of all elements of the valueset.

value_preferred_vocabulary_sab: The source vocabulary (SAB) preferred for each term in the dataset.  It is common, but
not required, that parent_vocabulary_sab and value_preferred_vocabulary_sab are the same i.e. specify a parent code
from the HUBMAP vocabulary and return terms from the HUBMAP vocabulary.

Returns
-------
JSON response from the Ontology API, which is a list of dictionaries, each containing "code", "sab", and "term" elements.
[
    {"code": "C003047", "sab": "HUBMAP", "term": "Histology"},
    {"code": "C003051", "sab": "HUBMAP", "term": "Molecular Cartography"},
    ...
]
"""
def get_valueset(ontology_app_context):
    global _ontology_api_url

    target_url = f"{_ontology_api_url}/dataset-types" \
                 f"?application_context={ontology_app_context}"

    # Use Memcached to improve performance
    response = make_request_get(target_url, internal_token_used = True)

    # Invoke .raise_for_status(), an HTTPError will be raised with certain status codes
    response.raise_for_status()

    if response.status_code == 200:
        return response.json()
    else:
        msg = f"Unable to make a request to query the UBKG via ontology-api: {target_url}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        logger.debug("======get_valueset() status code from ontology-api======")
        logger.debug(response.status_code)

        logger.debug("======get_valueset() response text from ontology-api======")
        logger.debug(response.text)

        # Also bubble up the error message from uuid-api
        raise requests.exceptions.RequestException(response.text)

"""
Create a set of new ids for the new entity to be created

Parameters
----------
normalized_class : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
json_data_dict: dict
    The json request dict from user input, required when creating ids for Donor/Sample/Dataset only
user_token: str
    The user's globus nexus token
user_info_dict: dict
    A dict containing all the user info, requried when creating ids for Donor only:
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
count : int
    The optional number of ids to generate. If omitted, defaults to 1 

Returns
-------
list
    The list of new ids dicts, the number of dicts is based on the count
"""
def create_hubmap_ids(normalized_class, json_data_dict, user_token, user_info_dict, count = 1):
    global _uuid_api_url

    """
    POST arguments in json
    entity_type - required: the type of entity, DONOR, SAMPLE, DATASET
    parent_ids - required for entity types of SAMPLE, DONOR and DATASET
               an array of UUIDs for the ancestors of the new entity
               For SAMPLEs and DONORs a single uuid is required (one entry in the array)
               and multiple ids are not allowed (SAMPLEs and DONORs are required to 
               have a single ancestor, not multiple).  For DATASETs at least one ancestor
               UUID is required, but multiple can be specified. (A DATASET can be derived
               from multiple SAMPLEs or DATASETs.) 
    organ_code - required only in the case where an id is being generated for a SAMPLE that
               has a DONOR as a direct ancestor.  Must be one of the codes from:
               https://github.com/hubmapconsortium/search-api/blob/main/src/search-schema/data/definitions/enums/organ_types.yaml
    
    Query string (in url) arguments:
        entity_count - optional, the number of ids to generate. If omitted, defaults to 1 
    """
    json_to_post = {
        'entity_type': normalized_class
    }

    # Activity and Collection don't require the `parent_ids` in request json
    if normalized_class in ['Donor', 'Sample', 'Dataset', 'Upload', 'Publication']:
        # The direct ancestor of Donor and Upload must be Lab
        # The group_uuid is the Lab id in this case
        if normalized_class in ['Donor', 'Upload']:
            # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that. 
            # Otherwise if not set and no single "provider group" membership throws error.  
            # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
            if 'hmgroupids' not in user_info_dict:
                raise KeyError("Missing 'hmgroupids' key in 'user_info_dict' when calling 'create_hubmap_ids()' to create new ids for this Donor.")

            user_group_uuids = user_info_dict['hmgroupids']

            # If group_uuid is provided by the request, use it with validation
            if 'group_uuid' in json_data_dict:
                group_uuid = json_data_dict['group_uuid']
                # Validate the group_uuid and make sure it's one of the valid data providers
                # and the user also belongs to this group
                try:
                    validate_entity_group_uuid(group_uuid, user_group_uuids)
                except schema_errors.NoDataProviderGroupException as e:
                    # No need to log
                    raise schema_errors.NoDataProviderGroupException(e)
                except schema_errors.UnmatchedDataProviderGroupException as e:
                    raise schema_errors.UnmatchedDataProviderGroupException(e)

                # Use group_uuid as parent_id for Donor
                parent_id = group_uuid
            # Otherwise, parse user token to get the group_uuid
            else:
                # If `group_uuid` is not already set, looks for membership in a single "data provider" group and sets to that. 
                # Otherwise if not set and no single "provider group" membership throws error.  
                # This field is also used to link (Neo4j relationship) to the correct Lab node on creation.
                if 'hmgroupids' not in user_info_dict:
                    raise KeyError("Missing 'hmgroupids' key in 'user_info_dict' when calling 'create_hubmap_ids()' to create new ids for this Donor.")

                try:
                    group_info = get_entity_group_info(user_info_dict['hmgroupids'])
                except schema_errors.NoDataProviderGroupException as e:
                    # No need to log
                    raise schema_errors.NoDataProviderGroupException(e)
                except schema_errors.MultipleDataProviderGroupException as e:
                    # No need to log
                    raise schema_errors.MultipleDataProviderGroupException(e)
                
                parent_id = group_info['uuid']
            
            # Add the parent_id to the request json
            json_to_post['parent_ids'] = [parent_id]
        elif normalized_class == 'Sample':
            # 'Sample.direct_ancestor_uuid' is marked as `required_on_create` in the schema yaml
            # The application-specific code should have already validated the 'direct_ancestor_uuid'
            parent_id = json_data_dict['direct_ancestor_uuid']
            json_to_post['parent_ids'] = [parent_id]

            # 'Sample.sample_category' is marked as `required_on_create` in the schema yaml
            if json_data_dict['sample_category'].lower() == 'organ':
                # The 'organ' field containing the 2 digit organ code is required in this case
                # The organ code has been validated in app.py before reaching here
                json_to_post['organ_code'] = json_data_dict['organ']
        else:
            # `Dataset.direct_ancestor_uuids` is `required_on_create` in yaml
            json_to_post['parent_ids'] = json_data_dict['direct_ancestor_uuids']

    request_headers = {
        'Authorization': f'Bearer {user_token}'
    }

    query_parms = {'entity_count': count}

    logger.info("======create_hubmap_ids() json_to_post to uuid-api======")
    logger.info(json_to_post)

    # Disable ssl certificate verification
    target_url = _uuid_api_url + SchemaConstants.UUID_API_ID_ENDPOINT
    response = requests.post(url = target_url, headers = request_headers, json = json_to_post, verify = False, params = query_parms)
    
    # Invoke .raise_for_status(), an HTTPError will be raised with certain status codes
    response.raise_for_status()
    
    if response.status_code == 200:
        # For Collection/Dataset/Activity/Upload, no submission_id gets 
        # generated, the uuid-api response looks like:
        """
        [{
            "uuid": "3bcc20f4f9ba19ed837136d19f530fbe",
            "hubmap_base_id": "965PRGB226",
            "hubmap_id": "HBM965.PRGB.226"
        }]
        """

        # For Donor/Sample, submission_id will be added:
        """
        [{
            "uuid": "c0276b5937ba8e0d7d1185020bade18f",
            "hubmap_base_id": "535RWXB646",
            "hubmap_id": "HBM535.RWXB.646",
            "submission_id": "TTDCT0001"
        }]
        """
        ids_list = response.json()

        # Remove the "hubmap_base_id" key from each dict in the list
        for d in ids_list:
            # Return None when the key is not in the dict
            # Will get keyError exception without the default value when the key is not found
            d.pop('hubmap_base_id', None)

        logger.info("======create_hubmap_ids() generated ids from uuid-api======")
        logger.info(ids_list)

        return ids_list
    else:
        msg = f"Unable to create new ids via the uuid-api service during the creation of this new {normalized_class}" 
        
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        logger.debug("======create_hubmap_ids() status code from uuid-api======")
        logger.debug(response.status_code)

        logger.debug("======create_hubmap_ids() response text from uuid-api======")
        logger.debug(response.text)

        # Also bubble up the error message from uuid-api
        raise requests.exceptions.RequestException(response.text)

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
def get_entity_group_info(user_hmgroupids_list, default_group = None):
    global _auth_helper

    # Default
    group_info = {
        'uuid': '',
        'name': ''
    }

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = _auth_helper.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']

    # A list of data provider uuids
    data_provider_uuids = []
    for uuid_key in groups_by_id_dict:
        if ('data_provider' in groups_by_id_dict[uuid_key]) and groups_by_id_dict[uuid_key]['data_provider']:
            data_provider_uuids.append(uuid_key)

    user_data_provider_uuids = []
    for group_uuid in user_hmgroupids_list:
        if group_uuid in data_provider_uuids:
            user_data_provider_uuids.append(group_uuid)

    if len(user_data_provider_uuids) == 0:
        msg = "No data_provider groups found for this user. Can't continue."
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.NoDataProviderGroupException(msg)

    if len(user_data_provider_uuids) > 1:
        if not default_group is None and default_group in user_hmgroupids_list:
            uuid = default_group
        else:
                msg = "More than one data_provider groups found for this user and no group_uuid specified. Can't continue."
                # Log the full stack trace, prepend a line with our message
                logger.exception(msg)
                raise schema_errors.MultipleDataProviderGroupException(msg)
    else:
        # By now only one data provider group found, this is what we want
        uuid = user_data_provider_uuids[0]
    group_info['uuid'] = uuid
    group_info['name'] = groups_by_id_dict[uuid]['displayname']
    
    return group_info


"""
Check if the given group uuid is valid

Parameters
----------
group_uuid : str
    The target group uuid string
user_group_uuids: list
    An optional list of group uuids to check against, a subset of all the data provider group uuids
"""
def validate_entity_group_uuid(group_uuid, user_group_uuids = None):
    global _auth_helper

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = _auth_helper.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']

    # First make sure the group_uuid is one of the valid group UUIDs defiend in the json
    if group_uuid not in groups_by_id_dict:
        msg = f"No data_provider groups found for the given group_uuid: {group_uuid}. Can't continue."
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        raise schema_errors.NoDataProviderGroupException(msg)

    # Optional check depending if user_group_uuids is provided
    if user_group_uuids:
        # Next, make sure the given group_uuid is associated with the user
        if group_uuid not in user_group_uuids:
            msg = f"The user doesn't belong to the given group of uuid: {group_uuid}. Can't continue."
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            raise schema_errors.UnmatchedDataProviderGroupException(msg)


"""
Get the group_name based on the given group_uuid

Parameters
----------
group_uuid : str
    UUID of the target group

Returns
-------
str
    The group_name corresponding to this group_uuid
"""
def get_entity_group_name(group_uuid):
    global _auth_helper
    
    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = _auth_helper.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
    group_dict = groups_by_id_dict[group_uuid]
    group_name = group_dict['displayname']

    return group_name


"""
Generate properties data of the target Activity node

Parameters
----------
normalized_entity_type : str
    One of the entity types defined in the schema yaml: Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
user_info_dict : dict
    A dictionary that contains all user info to be used to generate the related properties
count : int
    The number of Activities to be generated

Returns
-------
dict: A dict of gnerated Activity data
"""
def generate_activity_data(normalized_entity_type, user_token, user_info_dict):
    # Activity is not an Entity
    normalized_activity_type = 'Activity'

    # Target entity type dict
    # Will be used when calling `set_activity_creation_action()` trigger method
    normalized_entity_type_dict = {'normalized_entity_type': normalized_entity_type}

    # Create new ids for the Activity node
    # This resulting list has only one dict
    new_ids_dict_list = create_hubmap_ids(normalized_activity_type, json_data_dict = None, user_token = user_token, user_info_dict = None)
    data_dict_for_activity = {**user_info_dict, **normalized_entity_type_dict, **new_ids_dict_list[0]}

    # Generate property values for Activity node
    generated_activity_data_dict = generate_triggered_data( trigger_type=TriggerTypeEnum.BEFORE_CREATE
                                                            , normalized_class=normalized_activity_type
                                                            , user_token=user_token
                                                            , existing_data_dict={}
                                                            , new_data_dict=data_dict_for_activity)

    return generated_activity_data_dict


"""
Get the ingest-api URL to be used by trigger methods

Returns
-------
str
    The ingest-api URL
"""
def get_ingest_api_url():
    global _ingest_api_url
    
    return _ingest_api_url


"""
Get the entity-api URL to be used by trigger methods

Returns
-------
str
    The entity-api URL
"""


def get_entity_api_url():
    global _entity_api_url

    return _entity_api_url


"""
Get the AUthHelper instance to be used by trigger methods

Returns
-------
AuthHelper
    The AuthHelper instance
"""
def get_auth_helper_instance():
    global _auth_helper
    
    return _auth_helper


"""
Get the neo4j.Driver instance to be used by trigger methods

Returns
-------
neo4j.Driver
    The neo4j.Driver instance
"""
def get_neo4j_driver_instance():
    global _neo4j_driver
    
    return _neo4j_driver


"""
Get the Memcached client instance to be used by trigger methods

Returns
-------
neo4j.Driver
    The neo4j.Driver instance
"""
def get_memcached_client_instance():
    global _memcached_client
    
    return _memcached_client


"""
Get the configured Memcached prefix to be used by trigger methods

Returns
-------
neo4j.Driver
    The neo4j.Driver instance
"""
def get_memcached_prefix():
    global _memcached_prefix
    
    return _memcached_prefix


"""
Convert a string representation of the Python list/dict (either nested or not) to a Python list/dict object
with removing any non-printable control characters if presents.

Note: string representation of Python string can still contain control characters and should not be used by this method
But if a string representation of Python string is used as input by mistake, control characters gets removed as a result. 

Parameters
----------
data_str: str
    The string representation of the Python list/dict stored in Neo4j.
    It's not stored in Neo4j as a json string! And we can't store it as a json string 
    due to the way that Cypher handles single/double quotes.

Returns
-------
list or dict or str
    The desired Python list or dict object after evaluation or the original string input
"""
def convert_str_literal(data_str):
    if isinstance(data_str, str):
        # First remove those non-printable control characters that will cause SyntaxError
        # Use unicodedata.category(), we can check each character starting with "C" is the control character
        data_str = "".join(char for char in data_str if unicodedata.category(char)[0] != "C")

        # ast uses compile to compile the source string (which must be an expression) into an AST
        # If the source string is not a valid expression (like an empty string), a SyntaxError will be raised by compile
        # If, on the other hand, the source string would be a valid expression (e.g. a variable name like foo), 
        # compile will succeed but then literal_eval() might fail with a ValueError
        # Also this fails with a TypeError: literal_eval("{{}: 'value'}")
        try:
            data = ast.literal_eval(data_str)

            if isinstance(data, (list, dict)):
                logger.info(f"The input string literal has been converted to {type(data)} successfully")
                return data
            else:
                logger.info(f"The input string literal is not list or dict after evaluation, return the original string input")
                return data_str
        except (SyntaxError, ValueError, TypeError) as e:
            msg = f"Invalid expression (string value): {data_str} to be evaluated by ast.literal_eval()"
            logger.exception(msg)
    else:
        # Skip any non-string data types
        return data_str


"""
Get the response to an HTTP request of the target URL

Parameters
----------
target_url: str
    The target HTTP request URL

Returns
-------
requests.Response
    The Response object, which contains a server's response to an HTTP request
"""
def make_request_get(target_url, internal_token_used = False):
    global _memcached_client
    global _memcached_prefix

    response = None

    if _memcached_client and _memcached_prefix:
        cache_key = f'{_memcached_prefix}{target_url}'
        response = _memcached_client.get(cache_key)

    # Use the cached data if found and still valid
    # Otherwise, make a fresh query and add to cache
    if response is None:
        if _memcached_client and _memcached_prefix:
            logger.info(f'HTTP response cache not found or expired. Making a new HTTP request of GET {target_url} at time {datetime.now()}')
        
        if internal_token_used:
            # Use modified version of globus app secret from configuration as the internal token
            auth_helper_instance = get_auth_helper_instance()
            request_headers = {
                'Authorization': f'Bearer {auth_helper_instance.getProcessSecret()}'
            }

            # Disable ssl certificate verification
            response = requests.get(url = target_url, headers = request_headers, verify = False)
        else:
            response = requests.get(url = target_url, verify = False)

        if _memcached_client and _memcached_prefix:
            logger.info(f'Creating HTTP response cache of GET {target_url} at time {datetime.now()}')

            cache_key = f'{_memcached_prefix}{target_url}'
            _memcached_client.set(cache_key, response, expire = SchemaConstants.MEMCACHED_TTL)
    else:
        logger.info(f'Using HTTP response cache of GET {target_url} at time {datetime.now()}')

    return response


"""
Delete the cached data for the given entity uuids

Parameters
----------
uuids_list : list
    A list of target uuids
"""
def delete_memcached_cache(uuids_list):
    global _memcached_client
    global _memcached_prefix

    if _memcached_client and _memcached_prefix:
        cache_keys = []
        for uuid in uuids_list:
            cache_keys.append(f'{_memcached_prefix}_neo4j_{uuid}')
            cache_keys.append(f'{_memcached_prefix}_complete_{uuid}')
            cache_keys.append(f'{_memcached_prefix}_complete_index_{uuid}')
        _memcached_client.delete_many(cache_keys)

        logger.info(f"Deleted cache by key: {', '.join(cache_keys)}")


"""
Retrive the organ types from ontology-api

Returns
-------
dict
    The available organ types in the following format:

    {
        "AO": "Aorta",
        "BD": "Blood",
        "BL": "Bladder",
        "BM": "Bone Marrow",
        "BR": "Brain",
        "HT": "Heart",
        ...
    }
"""
def get_organ_types():
    global _ontology_api_url
    global _organ_types

    if _organ_types is None:
        target_url = _ontology_api_url + SchemaConstants.ONTOLOGY_API_ORGAN_TYPES_ENDPOINT

        # Use Memcached to improve performance
        response = make_request_get(target_url, internal_token_used = True)

        # Invoke .raise_for_status(), an HTTPError will be raised with certain status codes
        response.raise_for_status()

        if response.status_code == 200:
            _organ_types = response.json()
            return _organ_types
        else:
            # Log the full stack trace, prepend a line with our message
            logger.exception("Unable to make a request to query the organ types via ontology-api")

            logger.debug("======get_organ_types() status code from ontology-api======")
            logger.debug(response.status_code)

            logger.debug("======get_organ_types() response text from ontology-api======")
            logger.debug(response.text)

            # Also bubble up the error message from ontology-api
            raise requests.exceptions.RequestException(response.text)
    else:
        return _organ_types


####################################################################################################
## Internal functions
####################################################################################################


