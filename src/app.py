from flask import Flask, jsonify, abort, request, Response
import sys
import os
import yaml
import json
from cachetools import cached, TTLCache
import functools
from pathlib import Path
import logging
import datetime
from neo4j import CypherError

# Local modules
import neo4j_queries

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.neo4j_connection import Neo4jConnection

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Set logging level (default is warning)
logging.basicConfig(level=logging.DEBUG)

# LRU Cache implementation with per-item time-to-live (TTL) value
# with a memoizing callable that saves up to maxsize results based on a Least Frequently Used (LFU) algorithm
# with a per-item time-to-live (TTL) value
# Here we use two hours, 7200 seconds for ttl
cache = TTLCache(maxsize=app.config['CACHE_MAXSIZE'], ttl=app.config['CACHE_TTL'])

####################################################################################################
## Yaml schema loading
####################################################################################################
@cached(cache)
def load_schema_yaml_file(file):
    with open(file, 'r') as stream:
        try:
            schema = yaml.safe_load(stream)

            app.logger.info("======schema yaml loaded successfully======")
            app.logger.info(schema)
        except yaml.YAMLError as exc:
            app.logger.info("======schema yaml failed to load======")
            app.logger.info(exc)

        return schema
        
# Have the schema informaiton available for any requests
schema = load_schema_yaml_file(app.config['SCHEMA_YAML_FILE'])

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
@app.route('/', methods = ['GET'])
def index():
    return "Hello! This is HuBMAP Entity API service :)"

# Show status of neo4j connection
@app.route('/status', methods = ['GET'])
def status():
    response_data = {
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        'version': (Path(__file__).parent / 'VERSION').read_text().strip(),
        'build': (Path(__file__).parent / 'BUILD').read_text().strip(),
        'neo4j_connection': False
    }

    is_connected = neo4j_connection.check_connection(driver)
    
    if is_connected:
        response_data['neo4j_connection'] = True

    return jsonify(response_data)

# Force cache clear even before it expires
@app.route('/cache_clear', methods = ['GET'])
def cache_clear():
    cache.clear()
    app.logger.info("All gatewat API Auth function cache cleared.")
    return "All function cache cleared."

####################################################################################################
## API
####################################################################################################

# id is either a `uuid` or `hubmap_id` (like HBM123.ABCD.987)
@app.route('/<entity_type>/<id>', methods = ['GET'])
def get_entity(entity_type, id):
    # Validate user provied entity_type from URL
    validate_entity_type(entity_type)

    # Normalize user provided entity_type
    normalized_entity_type = normalize_entity_type(entity_type)

    # Query target entity against neo4j and return as a dict if exists
    entity_dict = query_target_entity(normalized_entity_type, id)

    return get_resulting_entity(normalized_entity_type, entity_dict)

@app.route('/<entity_type>', methods = ['POST'])
def create_entity(entity_type):
    # Validate user provied entity_type from URL
    validate_entity_type(entity_type)

    # Normalize user provided entity_type
    normalized_entity_type = normalize_entity_type(entity_type)

    # Always expect a json body
    request_json_required(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, normalized_entity_type)

    # Make sure there's no entity node with the same uuid/hubmap-id already exists
    entity_dict = query_target_entity(normalized_entity_type, json_data_dict['uuid'])

    if bool(entity_dict):
        bad_request_error("Entity with the same uuid " + json_data_dict['uuid'] + " already exists in the neo4j database.")

    # Construct the final data to include generated triggered data
    triggered_data_on_create = generate_triggered_data_on_create(normalized_entity_type, request)

    # Merge two dictionaries (without the same keys in this case)
    merged_dict = {**json_data_dict, **triggered_data_on_create}

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.info("======create entity node with json_list_str======")
    app.logger.info(json_list_str)

    # Create new entity
    new_entity_dict = neo4j_queries.create_entity(neo4j_driver, normalized_entity_type, escaped_json_list_str)

    return get_resulting_entity(normalized_entity_type, new_entity_dict)


@app.route('/<entity_type>/<id>', methods = ['PUT'])
def update_entity(entity_type, id):
    # Validate user provied entity_type from URL
    validate_entity_type(entity_type)

    # Normalize user provided entity_type
    normalized_entity_type = normalize_entity_type(entity_type)

    # Always expect a json body
    request_json_required(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # A bit more validation to make sure the target id in URL matches the one in json
    if json_data_dict['uuid'] != id:
        bad_request_error("The target id specified in URL does not match the uuid in JSON.")

    # Get target entity and return as a dict if exists
    entity_dict = query_target_entity(normalized_entity_type, id)

    # Existence check
    if not bool(entity_dict):
        not_found_error("Could not find the target " + normalized_entity_type + " with id of " + id)

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, normalized_entity_type)

    # Construct the final data to include generated triggered data
    triggered_data_on_save = generate_triggered_data_on_save(normalized_entity_type, request)

    # Add new properties if updating for the first time
    # Otherwise just overwrite existing values (E.g., last_modified_timestamp)
    triggered_data_keys = triggered_data_on_save.keys()
    for key in triggered_data_keys:
        entity_dict[key] = triggered_data_on_save[key]
 
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
    updated_entity_dict = neo4j_queries.update_entity(neo4j_driver, normalized_entity_type, escaped_json_list_str, id)

    return get_resulting_entity(normalized_entity_type, updated_entity_dict)


####################################################################################################
## Internal Functions
####################################################################################################

def normalize_entity_type(entity_type):
    normalized_entity_type = entity_type.lower().capitalize()
    return normalized_entity_type

def validate_entity_type(entity_type):
    separator = ", "
    accepted_entity_types = ["Dataset", "Donor", "Sample", "Collection"]

    # Validate provided entity_type
    if normalize_entity_type(entity_type) not in accepted_entity_types:
        bad_request_error("The specified entity type in URL must be one of the following: " + separator.join(accepted_entity_types))

# Get target entity dict
def query_target_entity(normalized_entity_type, id):
    try:
        entity_dict = neo4j_queries.get_entity(neo4j_driver, normalized_entity_type, id)
    except Exception as e:
        app.logger.info("======Exception from calling neo4j_queries.get_entity()======")
        app.logger.info(e)

        internal_server_error(e)
    except CypherError as ce:
        app.logger.info("======CypherError from calling neo4j_queries.get_entity()======")
        app.logger.info(ce)
        
        internal_server_error(ce)

    return entity_dict

"""
Validate JSON from user request against the schema

Parameters
----------
json_data_dict : dict
    The JSON data dict from user request
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
"""
def validate_json_data_against_schema(json_data_dict, entity_type):
    attributes = schema['ENTITIES'][entity_type]['attributes']
    schema_keys = attributes.keys() 
    json_data_keys = json_data_dict.keys()
    separator = ", "

    # Check if keys in request are supported
    unsupported_keys = []
    for key in json_data_keys:
        if key not in schema_keys:
            unsupported_keys.append(key)

    if len(unsupported_keys) > 0:
        bad_request_error("Unsupported keys in request json: " + separator.join(unsupported_keys))

    # Check if any required keys (except the triggered ones) are missing from request
    missing_keys = []
    for key in schema_keys:
        if attributes[key]['required'] and ('trigger-event' not in attributes[key]) and (key not in json_data_keys):
            missing_keys.append(key)

    if len(missing_keys) > 0:
        bad_request_error("Missing required keys in request json: " + separator.join(missing_keys))

    # By now all the keys in request json have passed the above two checks: existence cehck in schema and required check in schema
    # Verify data types of keys
    invalid_data_type_keys = []
    for key in json_data_keys:
        # boolean starts with bool, string starts with str, integer starts with int
        if not attributes[key]['type'].startswith(type(json_data_dict[key]).__name__):
            invalid_data_type_keys.append(key)
    
    if len(invalid_data_type_keys) > 0:
        bad_request_error("Keys in request json with invalid data types: " + separator.join(invalid_data_type_keys))

# Handling "on_create" trigger events
def generate_triggered_data_on_create(normalized_entity_type, request):
    attributes = schema['ENTITIES'][normalized_entity_type]['attributes']
    schema_keys = attributes.keys() 

    user_info = get_user_info(request)

    triggered_data_dict = {}
    for key in schema_keys:
        if 'trigger-event' in attributes[key]:
            if attributes[key]['trigger-event']['event'] == "on_create":
                method_name, method_to_call = get_trigger_event_method(attributes[key])

                # Some trigger methods require input argument
                if method_name == "get_entity_type":
                    triggered_data_dict[key] = method_to_call(normalized_entity_type)
                elif method_name.startswith("get_user_"):
                    triggered_data_dict[key] = method_to_call(user_info)
                else:
                    triggered_data_dict[key] = method_to_call()

    return triggered_data_dict

# TO-DO
# Handling "on_save" trigger events
def generate_triggered_data_on_save(normalized_entity_type, request):
    attributes = schema['ENTITIES'][normalized_entity_type]['attributes']
    schema_keys = attributes.keys() 

    user_info = get_user_info(request)

    triggered_data_dict = {}
    for key in schema_keys:
        if 'trigger-event' in attributes[key]:
            if attributes[key]['trigger-event']['event'] == "on_save":
                method_name, method_to_call = get_trigger_event_method(attributes[key])

                # Some trigger methods require input argument
                if method_name == "get_new_id":
                    triggered_data_dict[key] = method_to_call()
                elif method_name == "connect_datasets":
                    triggered_data_dict[key] = method_to_call()
                elif method_name.startswith("get_user_"):
                    triggered_data_dict[key] = method_to_call(user_info)
                else:
                    triggered_data_dict[key] = method_to_call()

    return triggered_data_dict

# TO-DO
# Handling "on_response" trigger events
def generate_triggered_data_on_response(normalized_entity_type):
    attributes = schema['ENTITIES'][normalized_entity_type]['attributes']
    schema_keys = attributes.keys() 

    triggered_data_dict = {}
    for key in schema_keys:
        if 'trigger-event' in attributes[key]:
            if attributes[key]['trigger-event']['event'] == "on_response":
                method_name, method_to_call = get_trigger_event_method(attributes[key])

                # Some trigger methods require input argument
                if method_name == "fill_contacts":
                    triggered_data_dict[key] = method_to_call()
                elif method_name == "get_dataset_ids":
                    triggered_data_dict[key] = method_to_call()
                elif method_name == "fill_creators":
                    triggered_data_dict[key] = method_to_call()
                else:
                    triggered_data_dict[key] = method_to_call()

    return triggered_data_dict

def get_resulting_entity(normalized_entity_type, entity_dict):
    result = {
        normalized_entity_type.lower(): entity_dict
    }

    return jsonify(result)

# Initialize AuthHelper (AuthHelper from HuBMAP commons package)
# HuBMAP commons AuthHelper handles "MAuthorization" or "Authorization"
def init_auth_helper():
    if AuthHelper.isInitialized() == False:
        auth_helper = AuthHelper.create(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'])
    else:
        auth_helper = AuthHelper.instance()
    
    return auth_helper

# Get user infomation dict based on the http request(headers)
def get_user_info(request):
    auth_helper = init_auth_helper()
    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    return auth_helper.getUserInfoUsingRequest(request, False)

# Throws error for 400 Bad Reqeust with message
def bad_request_error(err_msg):
    abort(400, description = err_msg)

# Throws error for 404 Not Found with message
def not_found_error(err_msg):
    abort(404, description = err_msg)

# Throws error for 500 Internal Server Error with message
def internal_server_error(err_msg):
    abort(500, description = err_msg)

# Always expect a json body
def request_json_required(request):
    if not request.is_json:
        bad_request_error("A JSON body and appropriate Content-Type header are required")

def get_trigger_event_method(attribute):
    method_name = attribute['trigger-event']['method']
    # The target method of this same module
    method_to_call = getattr(sys.modules[__name__], method_name)

    return method_name, method_to_call


####################################################################################################
## Schema trigger methods - DO NOT RENAME
####################################################################################################

def get_current_timestamp():
    current_time = datetime.datetime.now() 
    return current_time.timestamp() 

def get_entity_type(normalized_entity_type):
    return normalized_entity_type

def get_user_sub(user_info):
    return user_info['sub']

def get_user_email(user_info):
    return user_info['email']

def get_user_name(user_info):
    return user_info['name']


# To-DO
def get_new_id():
    return "dummy"

# To-DO
def connect_datasets():
    return "dummy"

# To-DO
def get_dataset_ids():
    return "dummy"

# To-DO
def fill_creators():
    return "dummy"

# To-DO
def fill_contacts():
    return "dummy"