from flask import Flask, jsonify, abort, request, Response
import sys
import os
import yaml
import requests
from urllib3.exceptions import InsecureRequestWarning
import json
from cachetools import cached, TTLCache
import functools
from pathlib import Path
import logging
from neo4j import CypherError

# Local modules
import neo4j_queries
import schema_triggers

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.neo4j_connection import Neo4jConnection

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['UUID_API_URL'] = app.config['UUID_API_URL'].strip('/')

# Set logging level (default is warning)
logging.basicConfig(level=logging.DEBUG)

# LRU Cache implementation with per-item time-to-live (TTL) value
# with a memoizing callable that saves up to maxsize results based on a Least Frequently Used (LFU) algorithm
# with a per-item time-to-live (TTL) value
# Here we use two hours, 7200 seconds for ttl
cache = TTLCache(maxsize=app.config['CACHE_MAXSIZE'], ttl=app.config['CACHE_TTL'])

####################################################################################################
## Provenance yaml schema loading
####################################################################################################
@cached(cache)
def load_provenance_schema_yaml_file(file):
    with open(file, 'r') as stream:
        try:
            schema = yaml.safe_load(stream)

            app.logger.info("======schema yaml loaded successfully======")
            app.logger.info(schema)

            return schema
        except yaml.YAMLError as exc:
            app.logger.info("======schema yaml failed to load======")
            app.logger.info(exc)
   
# Have the schema informaiton available for any requests
schema = load_provenance_schema_yaml_file(app.config['SCHEMA_YAML_FILE'])

####################################################################################################
## Neo4j connection
####################################################################################################
neo4j_connection = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
neo4j_driver = neo4j_connection.get_driver()

# Error handler for 400 Bad Request with custom error message
@app.errorhandler(400)
def http_bad_request(e):
    return jsonify(error=str(e)), 400

# Error handler for 404 Not Found with custom error message
@app.errorhandler(404)
def http_not_found(e):
    return jsonify(error=str(e)), 404

# Error handler for 404 Not Found with custom error message
@app.errorhandler(500)
def http_internal_server_error(e):
    return jsonify(error=str(e)), 500

####################################################################################################
## Default route, status, cache clear
####################################################################################################

"""
The default route

Returns
-------
string
    A welcome message
"""
@app.route('/', methods = ['GET'])
def index():
    return "Hello! This is HuBMAP Entity API service :)"

"""
Show status of neo4j connection with the current VERSION and BUILD

Returns
-------
json
    A json containing the status details
"""
@app.route('/status', methods = ['GET'])
def status():
    response_data = {
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        'version': (Path(__file__).parent / 'VERSION').read_text().strip(),
        'build': (Path(__file__).parent / 'BUILD').read_text().strip(),
        'neo4j_connection': False
    }

    conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
    driver = conn.get_driver()
    is_connected = neo4j_connection.check_connection(driver)
    
    if is_connected:
        response_data['neo4j_connection'] = True

    return jsonify(response_data)

"""
Force cache clear even before it expires

Returns
-------
string
    A confirmation message
"""
@app.route('/cache_clear', methods = ['GET'])
def cache_clear():
    cache.clear()
    
    app.logger.info("======schema yaml cache cleared======")
    app.logger.info(schema)

    return "schema yaml cache cleared"

####################################################################################################
## API
####################################################################################################


"""
Retrive the properties of a given entity by eiter uuid or hubmap_id

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
id : string
    Either the uuid or the hubmap_id of target entity 

Returns
-------
json
    All the properties of the target entity
"""
@app.route('/<entity_class>/<id>', methods = ['GET'])
def get_entity(entity_class, id):
    # Validate user provied entity_class from URL
    validate_entity_class(entity_class)

    # Normalize user provided entity_class
    normalized_entity_class = normalize_entity_class(entity_class)

    # Query target entity against neo4j and return as a dict if exists
    entity_dict = query_target_entity(normalized_entity_class, id)

    # Dictionaries to be used by trigger methods
    normalized_entity_class_dict = {"normalized_entity_class": normalized_entity_class}

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**parameters_dict, **entity_dict}

    on_read_trigger_data_dict = generate_triggered_data("on_read_trigger", "ENTITIES", data_dict)

    # Merge two dictionaries (unique keys in each dict)
    result_dict = {**entity_dict, **on_read_trigger_data_dict}

    return json_response(normalized_entity_class, result_dict)

"""
Create a new entity node in neo4j

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/<entity_class>', methods = ['POST'])
def create_entity(entity_class):
    # Validate user provied entity_class from URL
    validate_entity_class(entity_class)

    # Normalize user provided entity_class
    normalized_entity_class = normalize_entity_class(entity_class)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, "ENTITIES", normalized_entity_class)

    # Dictionaries to be used by trigger methods
    normalized_entity_class_dict = {"normalized_entity_class": normalized_entity_class}
    user_info_dict = get_user_info(request)
    new_ids_dict = create_new_ids(normalized_entity_class)

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_class_dict, **user_info_dict, **new_ids_dict}

    on_create_trigger_data_dict = generate_triggered_data("on_create_trigger", "ENTITIES", data_dict)

    # Merge two dictionaries
    merged_dict = {**json_data_dict, **on_create_trigger_data_dict}

    # For Dataset associated with Collections
    collection_uuids_list = []
    if 'collection_uuids' in merged_dict:
        collection_uuids_list = merged_dict['collection_uuids']

    # Check existence of those collections
    for collection_uuid in collection_uuids_list:
        collection_dict = query_target_entity('Collection', collection_uuid)

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.info("======create entity node with escaped_json_list_str======")
    app.logger.info(escaped_json_list_str)

    # Create new entity
    # If `collection_uuids_list` is not an empty list, meaning the target entity is Dataset and 
    # we'll be also creating relationships between the new entity node to the Collection nodes
    result_dict = neo4j_queries.create_entity(neo4j_driver, normalized_entity_class, escaped_json_list_str, collection_uuids_list = collection_uuids_list)

    return json_response(normalized_entity_class, result_dict)

"""
Create a new entity node from the specified source node in neo4j

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
source_entity_class : str
    One of the normalized entity classes: Dataset, Sample, Donor, but NOT Collection
source_entity_id : string
    Either the uuid or the hubmap_id of the source entity 

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/derived/<entity_class>/from/<source_entity_class>/<source_entity_id>', methods = ['POST'])
def create_derived_entity(entity_class, source_entity_class, source_entity_id):
    source_entity_uuid = None

    # Validate entity_class of the derived entity and source_entity_class from URL
    # Collection can not be derived
    validate_derived_entity_class(entity_class)
    validate_entity_class(source_entity_class)

    # Normalize user provided entity_class and source_entity_class
    normalized_entity_class = normalize_entity_class(entity_class)
    normalized_source_entity_class = normalize_entity_class(source_entity_class)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, "ENTITIES", normalized_entity_class)

    # Query source entity against neo4j and return as a dict if exists
    source_entity_dict = query_target_entity(normalized_source_entity_class, source_entity_id)

    # Otherwise get the uuid of the source entity for later use
    source_entity_uuid = source_entity_dict['uuid']

    # Dictionaries to be used by trigger methods
    normalized_entity_class_dict = {"normalized_entity_class": normalized_entity_class}
    user_info_dict = get_user_info(request)
    new_ids_dict = create_new_ids(normalized_entity_class)

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_class_dict, **user_info_dict, **new_ids_dict}

    on_create_trigger_data_dict = generate_triggered_data("on_create_trigger", "ENTITIES", data_dict)

    # Merge two dictionaries
    merged_dict = {**json_data_dict, **on_create_trigger_data_dict}

    # For Dataset associated with Collections
    collection_uuids_list = []
    if 'collection_uuids' in merged_dict:
        collection_uuids_list = merged_dict['collection_uuids']

    # Check existence of those collections
    for collection_uuid in collection_uuids_list:
        collection_dict = query_target_entity('Collection', collection_uuid)

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.info("======create entity node with escaped_json_list_str======")
    app.logger.info(escaped_json_list_str)

    # For Activity creation, since Activity is not an Entity, we use "class" for reference
    normalized_activity_class = "Activity"

    # Dictionaries to be used by trigger methods
    normalized_activity_class_dict = {
        "normalized_activity_class": normalized_activity_class
    }
    # Create new ids for the Activity node
    new_ids_dict_for_activity = create_new_ids(normalized_activity_class)

    # Build a merged dict for Activity
    data_dict_for_activity = {**parameters_dict, **normalized_activity_class_dict, **user_info_dict, **new_ids_dict_for_activity}

    # Get trigger generated data for Activity
    on_create_trigger_data_dict_for_activity = generate_triggered_data("on_create_trigger", "ACTIVITIES", data_dict_for_activity)
    
    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [on_create_trigger_data_dict_for_activity]
    
    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    app.logger.info("======create activity node with activity_json_list_str======")
    app.logger.info(activity_json_list_str)

    # Create the derived entity alone with the Activity node and relationships
    result_dict = neo4j_queries.create_entity(neo4j_driver, normalized_entity_class, escaped_json_list_str, activity_json_list_str = activity_json_list_str, source_entity_uuid = source_entity_uuid, collection_uuids_list = collection_uuids_list)

    return json_response(normalized_entity_class, result_dict)


"""
Update the properties of a given entity in neo4j by uuid

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
id : string
    The uuid of target entity 

Returns
-------
json
    All the updated properties of the target entity
"""
@app.route('/<entity_class>/<id>', methods = ['PUT'])
def update_entity(entity_class, id):
    # Validate user provied entity_class from URL
    validate_entity_class(entity_class)

    # Normalize user provided entity_class
    normalized_entity_class = normalize_entity_class(entity_class)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Get target entity and return as a dict if exists
    entity_dict = query_target_entity(normalized_entity_class, id)

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, "ENTITIES", normalized_entity_class)

    # Dictionaries to be used by trigger methods
    normalized_entity_class_dict = {"normalized_entity_class": normalized_entity_class}
    user_info_dict = get_user_info(request)

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_class_dict, **user_info_dict}

    on_update_trigger_data_dict = generate_triggered_data("on_update_trigger", "ENTITIES", data_dict)

    # Add new properties if updating for the first time
    # Otherwise just overwrite existing values (E.g., last_modified_timestamp)
    triggered_data_keys = on_update_trigger_data_dict.keys()
    for key in triggered_data_keys:
        entity_dict[key] = on_update_trigger_data_dict[key]
 
    # Overwrite old property values with updated values
    json_data_keys = json_data_dict.keys()
    for key in json_data_keys:
        entity_dict[key] = json_data_dict[key]

    # By now the entity dict contains all user updates and all triggered data
    # `UNWIND` in Cypher expects List<T>
    data_list = [entity_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.info("======update entity node with json_list_str======")
    app.logger.info(json_list_str)

    # Update the exisiting entity
    updated_entity_dict = neo4j_queries.update_entity(neo4j_driver, normalized_entity_class, escaped_json_list_str, id)

    return json_response(normalized_entity_class, updated_entity_dict)


####################################################################################################
## Internal Functions
####################################################################################################

"""
Throws error for 400 Bad Reqeust with message

Parameters
----------
err_msg : str
    The custom error message to return to end users
"""
def bad_request_error(err_msg):
    abort(400, description = err_msg)

"""
Throws error for 404 Not Found with message

Parameters
----------
err_msg : str
    The custom error message to return to end users
"""
def not_found_error(err_msg):
    abort(404, description = err_msg)

"""
Throws error for 500 Internal Server Error with message

Parameters
----------
err_msg : str
    The custom error message to return to end users
"""
def internal_server_error(err_msg):
    abort(500, description = err_msg)

"""
Lowercase and captalize the entity type string

Parameters
----------
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
id : string
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
Validate the user specifed entity type in URL

Parameters
----------
entity_class : str
    The user specifed entity type in URL
"""
def validate_entity_class(entity_class):
    separator = ", "
    accepted_entity_classs = ["Dataset", "Donor", "Sample", "Collection"]

    # Validate provided entity_class
    if normalize_entity_class(entity_class) not in accepted_entity_classs:
        bad_request_error("The specified entity type in URL must be one of the following: " + separator.join(accepted_entity_classs))

"""
Validate the user specifed entity type for derived entity

Parameters
----------
entity_class : str
    The user specifed entity type in URL
"""
def validate_derived_entity_class(entity_class):
    separator = ", "
    # Collection can not be derived
    accepted_entity_classs = ["Dataset", "Donor", "Sample"]

    # Validate provided entity_class
    if normalize_entity_class(entity_class) not in accepted_entity_classs:
        bad_request_error("Invalid entity type specified for the derived entity. Accepted type: " + separator.join(accepted_entity_classs))


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
id : string
    Either the uuid or hubmap_id of target entity 

Returns
-------
string
    The uuid string from the uuid-api call

    The list returned by uuid-api that contains all the associated ids, e.g.:
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
def get_target_uuid(id):
    target_url = app.config['UUID_API_URL'] + '/' + id
    # Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
    requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

    # Use modified version of globus app secrect from configuration as the internal token
    # All API endpoints specified in gateway regardless of auth is required or not, 
    # will consider this internal token as valid and has the access to HuBMAP-Read group
    request_headers = create_request_headers()

    # Disable ssl certificate verification
    response = requests.get(url = target_url, headers = request_headers, verify = False) 
    
    if response.status_code == 200:
        ids_list = response.json()

        if len(ids_list) == 0:
            internal_server_error('unable to find information on identifier: ' + id)
        if len(ids_list) > 1:
            internal_server_error('found multiple records for identifier: ' + id)
        
        return ids_list[0]['hmuuid']
    else:
        not_found_error("Could not find the target uuid via uuid-api service associatted with the provided id of " + id)

"""
Create a set of new ids for the new entity to be created
Make a POST call to uuid-api with the following JSON:
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
def create_new_ids(normalized_entity_class, generate_doi = True):
    target_url = app.config['UUID_API_URL']

    # Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
    requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

    # Must use "generateDOI": "true" to generate the doi (doi_suffix_id) and displayDoi (hubmap_id)
    json_to_post = {
        'entityType': normalized_entity_class, 
        'generateDOI': str(generate_doi).lower() # Convert python bool to JSON string "true" or "false"
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
            'uuid': ids_dict['uuid']
        }

        # Add extra fields
        if generate_doi:
            new_ids_dict['doi_suffix_id'] = ids_dict['doi']
            new_ids_dict['hubmap_id'] = ids_dict['displayDoi']

        return new_ids_dict
    else:
        app.logger.info("======Failed to create new ids via the uuid-api service for during the creation of this new entity======")
        app.logger.info("response status code: " + str(response.status_code))
        app.logger.info("response text: " + response.text)

        internal_server_error("Failed to create new ids via the uuid-api service for during the creation of this new entity")


"""
Get target entity dict

Parameters
----------
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
id : string
    The uuid or hubmap_id of target entity 

Returns
-------
dict
    A dictionary of entity details returned from neo4j
"""
def query_target_entity(normalized_entity_class, id):
    # Make a call to uuid-api to get back the uuid
    uuid = get_target_uuid(id)

    try:
        entity_dict = neo4j_queries.get_entity(neo4j_driver, normalized_entity_class, uuid)
    except Exception as e:
        app.logger.info("======Exception from calling neo4j_queries.get_entity()======")
        app.logger.info(e)

        internal_server_error(e)
    except CypherError as ce:
        app.logger.info("======CypherError from calling neo4j_queries.get_entity()======")
        app.logger.info(ce)
        
        internal_server_error(ce)

    # Existence check
    if not bool(entity_dict):
        not_found_error("Could not find the target " + normalized_entity_class + " of id " + id)

    return entity_dict


"""
Validate JSON data from user request against the schema

Parameters
----------
json_data_dict : dict
    The JSON data dict from user request
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
"""
def validate_json_data_against_schema(json_data_dict, normalized_schema_section_key, normalized_entity_class):
    properties = schema[normalized_schema_section_key][normalized_entity_class]['properties']
    schema_keys = properties.keys() 
    json_data_keys = json_data_dict.keys()
    separator = ", "

    # Check if keys in request JSON are supported
    unsupported_keys = []
    for key in json_data_keys:
        if key not in schema_keys:
            unsupported_keys.append(key)

    if len(unsupported_keys) > 0:
        bad_request_error("Unsupported keys in request json: " + separator.join(unsupported_keys))

    # Check if keys in request JSON are immutable
    immutable_keys = []
    for key in json_data_keys:
        if 'immutable' in properties[key]:
            if properties[key]:
                immutable_keys.append(key)

    if len(immutable_keys) > 0:
        bad_request_error("Immutable keys are not allowed in request json: " + separator.join(immutable_keys))
    
    # Check if keys in request JSON are generated transient keys
    transient_keys = []
    for key in json_data_keys:
        if 'transient' in properties[key]:
            if properties[key]:
                transient_keys.append(key)

    if len(transient_keys) > 0:
        bad_request_error("Transient keys are not allowed in request json: " + separator.join(transient_keys))

    # Check if any schema keys that are user_input_required but missing from request
    missing_required_keys = []
    for key in schema_keys:
        # Schema rules: 
        # - By default, the schema treats all entity properties as optional. Use `user_input_required: true` to mark an attribute as required
        # - If an attribute is marked as `user_input_required: true`, it can't have `trigger` at the same time
        # It's reenforced here because we can't guarantee this rule is being followed correctly in the schema yaml
        if 'user_input_required' in properties[key]:
            if properties[key]['user_input_required'] and ('trigger' not in properties[key]) and (key not in json_data_keys):
                missing_required_keys.append(key)

    if len(missing_required_keys) > 0:
        bad_request_error("Missing required keys in request json: " + separator.join(missing_required_keys))

    # By now all the keys in request json have passed the above two checks: existence cehck in schema and required check in schema
    # Verify data types of keys
    invalid_data_type_keys = []
    for key in json_data_keys:
        # boolean starts with bool, string starts with str, integer starts with int
        # ??? How to handle other data types?
        if not properties[key]['type'].startswith(type(json_data_dict[key]).__name__):
            invalid_data_type_keys.append(key)
    
    if len(invalid_data_type_keys) > 0:
        bad_request_error("Keys in request json with invalid data types: " + separator.join(invalid_data_type_keys))


"""
Generating triggered data based on the target events and methods

Parameters
----------
trigger_type : str
    One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger

data_dict : dict
    A merged dictionary that contains all possible data to be used by the trigger methods

Returns
-------
dict
    A dictionary of trigger event methods generated data
"""
def generate_triggered_data(trigger_type, normalized_schema_section_key, data_dict):
    accepted_section_keys = ['ACTIVITIES', 'ENTITIES']
    separator = ", "
    normalized_class = None

    if normalized_schema_section_key not in accepted_section_keys:
        internal_server_error('Unsupported schema section key: ' + normalized_schema_section_key + ". Must be one of the following: " + separator.join(accepted_section_keys))

    # Use normalized_entity_class for all classes under the ENTITIES section
    if normalized_schema_section_key == 'ENTITIES':
        normalized_class = data_dict['normalized_entity_class']

    # Use normalized_activity_class for all classes under the ACTIVITIES section
    # ACTIVITIES section has only one prov class: Activity
    if normalized_schema_section_key == 'ACTIVITIES':
        normalized_class = data_dict['normalized_activity_class']

    properties = schema[normalized_schema_section_key][normalized_class]['properties']
    schema_keys = properties.keys() 

    # Always pass the `neo4j_driver` along with the data_dict to schema_triggers.py module
    neo4j_driver_dict = {"neo4j_driver": neo4j_driver}
    combined_data_dict = {**neo4j_driver_dict, **data_dict}

    # Put all resulting data into a dictionary too
    trigger_generated_data_dict = {}
    for key in schema_keys:
        if trigger_type in properties[key]:
            trigger_method_name = properties[key][trigger_type]
            # Call the target trigger method of schema_triggers.py module
            trigger_method_to_call = getattr(schema_triggers, trigger_method_name)
            trigger_generated_data_dict[key] = trigger_method_to_call(combined_data_dict)

    return trigger_generated_data_dict

"""
Generate the final response data

Parameters
----------
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
entity_dict : dict
    The target entity dict
    
Returns
-------
str
    A response string
"""
def json_response(normalized_entity_class, entity_dict):
    result = {
        normalized_entity_class.lower(): entity_dict
    }

    return jsonify(result)

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

    app.logger.info("======get_user_info()======")
    app.logger.info(user_info)

    # If returns error response, invalid header or token
    if isinstance(user_info, Response):
        bad_request_error("Failed to query the user info with the given globus token")

    return user_info

"""
Always expect a json body from user request

request : Flask request object
    The Flask request passed from the API endpoint
"""
def require_json(request):
    if not request.is_json:
        bad_request_error("A JSON body and appropriate Content-Type header are required")

