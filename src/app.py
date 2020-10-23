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
## Entities yaml schema loading
####################################################################################################
@cached(cache)
def load_schema_yaml_file(file):
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
Retrive the properties of a given entity by uuid

Parameters
----------
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
id : string
    The uuid of target entity 

Returns
-------
json
    All the properties of the target entity
"""
@app.route('/<entity_type>/<id>', methods = ['GET'])
def get_entity(entity_type, id):
    # Validate user provied entity_type from URL
    validate_entity_type(entity_type)

    # Normalize user provided entity_type
    normalized_entity_type = normalize_entity_type(entity_type)

    # Query target entity against neo4j and return as a dict if exists
    entity_dict = query_target_entity(normalized_entity_type, id)

    # Dictionaries to be used by trigger methods
    normalized_entity_type_dict = {"normalized_entity_type": normalized_entity_type}

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_type_dict}

    on_read_trigger_data_dict = generate_triggered_data("on_read_trigger", data_dict)

    # Merge two dictionaries (without the same keys in this case)
    merged_dict = {**entity_dict, **on_read_trigger_data_dict}

    return get_resulting_entity(normalized_entity_type, merged_dict)

"""
Create a new entity node in neo4j

Parameters
----------
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor

Returns
-------
json
    All the properties of the newly created entity
"""
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

    # Dictionaries to be used by trigger methods
    normalized_entity_type_dict = {"normalized_entity_type": normalized_entity_type}
    user_info_dict = get_user_info(request)
    created_ids_dict = create_new_ids(normalized_entity_type)

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_type_dict, **user_info_dict, **created_ids_dict}

    on_create_trigger_data_dict = generate_triggered_data("on_create_trigger", data_dict)

    # Make sure there's no entity node with the same uuid/hubmap-id already exists
    entity_dict = query_target_entity(normalized_entity_type, on_create_trigger_data_dict['uuid'])

    if bool(entity_dict):
        bad_request_error("Entity with the same uuid " + on_create_trigger_data_dict['uuid'] + " already exists in the neo4j database.")

    # Merge two dictionaries (without the same keys in this case)
    merged_dict = {**json_data_dict, **on_create_trigger_data_dict}

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

"""
Update the properties of a given entity in neo4j by uuid

Parameters
----------
entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
id : string
    The uuid of target entity 

Returns
-------
json
    All the updated properties of the target entity
"""
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

    # Get target entity and return as a dict if exists
    entity_dict = query_target_entity(normalized_entity_type, id)

    # Existence check
    if not bool(entity_dict):
        not_found_error("Could not find the target " + normalized_entity_type + " of id " + id)

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, normalized_entity_type)

    # Dictionaries to be used by trigger methods
    normalized_entity_type_dict = {"normalized_entity_type": normalized_entity_type}
    user_info_dict = get_user_info(request)

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_type_dict, **user_info_dict}

    on_update_trigger_data_dict = generate_triggered_data("on_update_trigger", request)

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
    updated_entity_dict = neo4j_queries.update_entity(neo4j_driver, normalized_entity_type, escaped_json_list_str, id)

    return get_resulting_entity(normalized_entity_type, updated_entity_dict)


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
normalized_entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
id : string
    The uuid of target entity 

Returns
-------
string
    One of the normalized entity type: Dataset, Collection, Sample, Donor
"""
def normalize_entity_type(entity_type):
    normalized_entity_type = entity_type.lower().capitalize()
    return normalized_entity_type

"""
Validate the user specifed entity type in URL

Parameters
----------
entity_type : str
    The user specifed entity type in URL
"""
def validate_entity_type(entity_type):
    separator = ", "
    accepted_entity_types = ["Dataset", "Donor", "Sample", "Collection"]

    # Validate provided entity_type
    if normalize_entity_type(entity_type) not in accepted_entity_types:
        bad_request_error("The specified entity type in URL must be one of the following: " + separator.join(accepted_entity_types))

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
def create_new_ids(normalized_entity_type):
    target_url = app.config['UUID_API_URL']

    # Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
    requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

    # Must use "generateDOI": "true" to generate the doi (doi_suffix_id) and displayDoi (hubmap_id)
    json_to_post = {
        "entityType": normalized_entity_type, 
        "generateDOI": "true"
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
        created_ids_dict = {
            "uuid": ids_dict['uuid'],
            "doi_suffix_id": ids_dict['doi'],
            "hubmap_id": ids_dict['displayDoi']
        }

        return created_ids_dict
    else:
        app.logger.info("======Failed to create new ids via the uuid-api service for during the creation of this new entity======")
        app.logger.info("response status code: " + str(response.status_code))
        app.logger.info("response text: " + response.text)

        internal_server_error("Failed to create new ids via the uuid-api service for during the creation of this new entity")


"""
Get target entity dict

Parameters
----------
normalized_entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
id : string
    The uuid or hubmap_id of target entity 

Returns
-------
dict
    A dictionary of entity details returned from neo4j
"""
def query_target_entity(normalized_entity_type, id):
    # Make a call to uuid-api to get back the uuid
    uuid = get_target_uuid(id)

    try:
        entity_dict = neo4j_queries.get_entity(neo4j_driver, normalized_entity_type, uuid)
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
Validate JSON data from user request against the schema

Parameters
----------
json_data_dict : dict
    The JSON data dict from user request
normalized_entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
"""
def validate_json_data_against_schema(json_data_dict, normalized_entity_type):
    attributes = schema['ENTITIES'][normalized_entity_type]['attributes']
    schema_keys = attributes.keys() 
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
        if 'immutable' in attributes[key]:
            if attributes[key]:
                immutable_keys.append(key)

    if len(immutable_keys) > 0:
        bad_request_error("Immutable keys are not allowed in request json: " + separator.join(immutable_keys))
    
    # Check if keys in request JSON are generated transient keys
    transient_keys = []
    for key in json_data_keys:
        if 'transient' in attributes[key]:
            if attributes[key]:
                transient_keys.append(key)

    if len(transient_keys) > 0:
        bad_request_error("Transient keys are not allowed in request json: " + separator.join(transient_keys))

    # Check if any schema required keys are missing from request
    missing_keys = []
    for key in schema_keys:
        # Schema rules: 
        # - By default, the schema treats all entity attributes as optional. Use `required: true` to mark an attribute as required
        # - If an attribute is marked as `required: true`, it can't have `trigger` at the same time
        # It's reenforced here because we can't guarantee this rule is being followed correctly in the schema yaml
        if 'required' in attributes[key]:
            if attributes[key]['required'] and ('trigger' not in attributes[key]) and (key not in json_data_keys):
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
def generate_triggered_data(trigger_type, data_dict):
    normalized_entity_type = data_dict['normalized_entity_type']
    attributes = schema['ENTITIES'][normalized_entity_type]['attributes']
    schema_keys = attributes.keys() 

    triggered_data_dict = {}
    for key in schema_keys:
        if trigger_type in attributes[key]:
            trigger_method_name = attributes[key][trigger_type]
            # Call the target trigger method of schema_triggers.py module
            trigger_method_to_call = getattr(schema_triggers, trigger_method_name)
            triggered_data_dict[key] = trigger_method_to_call(data_dict)

    return triggered_data_dict

"""
Generate the final response data

Parameters
----------
normalized_entity_type : str
    One of the normalized entity type: Dataset, Collection, Sample, Donor
entity_dict : dict
    The target entity dict
    
Returns
-------
str
    A response string
"""
def get_resulting_entity(normalized_entity_type, entity_dict):
    result = {
        normalized_entity_type.lower(): entity_dict
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
"""
def get_user_info(request):
    auth_helper = init_auth_helper()
    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    return auth_helper.getUserInfoUsingRequest(request, False)

"""
Always expect a json body from user request

request : Flask request object
    The Flask request passed from the API endpoint
"""
def request_json_required(request):
    if not request.is_json:
        bad_request_error("A JSON body and appropriate Content-Type header are required")

