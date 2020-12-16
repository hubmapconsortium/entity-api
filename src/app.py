from flask import Flask, g, jsonify, abort, request, Response, redirect
from neo4j import GraphDatabase
from neo4j.exceptions import TransactionError
import sys
import os
import re
import json
import requests
from urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
import logging
import urllib

# Local modules
import app_neo4j_queries
from schema import schema_manager
from schema import schema_errors

# HuBMAP commons
from hubmap_commons import string_helper
from hubmap_commons import file_helper
from hubmap_commons import neo4j_driver
from hubmap_commons import globus_groups
from hubmap_commons.hm_auth import AuthHelper

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['UUID_API_URL'] = app.config['UUID_API_URL'].strip('/')
app.config['SEARCH_API_URL'] = app.config['SEARCH_API_URL'].strip('/')

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)


####################################################################################################
## Neo4j connection
####################################################################################################

# The neo4j_driver (from commons package) is a singleton module
# This neo4j_driver_instance will be used for application-specifc neo4j queries
neo4j_driver_instance = neo4j_driver.instance(app.config['NEO4J_URI'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])

"""
Close the current neo4j connection at the end of every request
"""
@app.teardown_appcontext
def close_neo4j_driver(error):
    if hasattr(g, 'neo4j_driver_instance'):
        # Close the driver instance
        neo4j_driver.close()
        # Also remove neo4j_driver_instance from Flask's application context
        g.neo4j_driver_instance = None


####################################################################################################
## Schema initialization
####################################################################################################

try:
    # Pass in the neo4j connection (uri, username, password) parameters in addition to the schema yaml
    # Because some of the schema trigger methods may issue queries to the neo4j.
    schema_manager.initialize(app.config['SCHEMA_YAML_FILE'], 
                              app.config['NEO4J_URI'], 
                              app.config['NEO4J_USERNAME'], 
                              app.config['NEO4J_PASSWORD'],
                              app.config['UUID_API_URL'],
                              app.config['APP_CLIENT_ID'], 
                              app.config['APP_CLIENT_SECRET'])
# Use a broad catch-all here
except Exception:
    msg = "Failed to initialize the schema_manager module"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)
    # Terminate and let the users know
    internal_server_error(msg)


####################################################################################################
## AuthHelper initialization
####################################################################################################

# Initialize AuthHelper (AuthHelper from HuBMAP commons package)
# auth_helper will be used to get the globus user info and 
# the secret token for making calls to other APIs
if AuthHelper.isInitialized() == False:
    auth_helper = AuthHelper.create(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'])
else:
    auth_helper = AuthHelper.instance()


####################################################################################################
## Error handlers
####################################################################################################

# Error handler for 400 Bad Request with custom error message
@app.errorhandler(400)
def http_bad_request(e):
    return jsonify(error=str(e)), 400

# Error handler for 403 Forbidden with custom error message
@app.errorhandler(403)
def http_forbidden(e):
    return jsonify(error=str(e)), 403

# Error handler for 404 Not Found with custom error message
@app.errorhandler(404)
def http_not_found(e):
    return jsonify(error=str(e)), 404

# Error handler for 500 Internal Server Error with custom error message
@app.errorhandler(500)
def http_internal_server_error(e):
    return jsonify(error=str(e)), 500


####################################################################################################
## API Endpoints
####################################################################################################

"""
The default route

Returns
-------
str
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
def get_status():
    status_data = {
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        'version': (Path(__file__).parent / 'VERSION').read_text().strip(),
        'build': (Path(__file__).parent / 'BUILD').read_text().strip(),
        'neo4j_connection': False
    }
    
    # Don't use try/except here
    is_connected = app_neo4j_queries.check_connection(neo4j_driver_instance)
    
    if is_connected:
        status_data['neo4j_connection'] = True

    return jsonify(status_data)

"""
Force delete cache even before it expires

Returns
-------
str
    A confirmation message
"""
@app.route('/cache', methods = ['DELETE'])
def delete_cache():
    cache.clear()
    msg = "Function cache deleted."
    logger.info(msg)
    return msg

"""
Retrive the properties of a given entity by id
Result filtering is supported based on query string
For example: /entities/<id>?property=data_access_level

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
json
    All the properties or filtered property of the target entity
"""
@app.route('/entities/<id>', methods = ['GET'])
def get_entity_by_id(id):
    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id)

    # Handle Collection retrieval using a different endpoint 
    if entity_dict['entity_class'] == 'Collection':
        bad_request_error("Please use another API endpoint `/collections/<id>` to query a collection")

    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(entity_dict)

    # Will also filter the result based on schema
    final_result = schema_manager.normalize_entity_result_for_response(complete_dict)

    # Result filtering based on query string
    result_filtering_accepted_property_keys = ['data_access_level']
    separator = ', '
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error("Only the following property keys are supported in the query string: " + separator.join(result_filtering_accepted_property_keys))
            
            # Only return the property value
            property_value = final_entity_dict[property_key]

            # Final result
            final_result = property_value
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    # Response with the final result
    return jsonify(final_result)

"""
Show all the supported entity classes

Returns
-------
json
    A list of all the available entity classes defined in the schema yaml
"""
@app.route('/entity-classes', methods = ['GET'])
def get_entity_classes():
    return jsonify(schema_manager.get_all_entity_classes())

"""
Retrive all the entity nodes for a given entity class
Result filtering is supported based on query string
For example: /<entity_class>/entities?property=uuid

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Sample, Donor
    Will handle Collection via API endpoint `/collections`

Returns
-------
json
    All the entity nodes in a list of the target entity class
"""
@app.route('/<entity_class>/entities', methods = ['GET'])
def get_entities_by_class(entity_class):
    # Normalize user provided entity_class
    normalized_entity_class = schema_manager.normalize_entity_class(entity_class)

    # Validate the normalized_entity_class to ensure it's one of the accepted classes
    try:
        schema_manager.validate_normalized_entity_class(normalized_entity_class)
    except schema_errors.InvalidNormalizedEntityClassException as e:
        bad_request_error("Invalid entity class provided: " + entity_class)

    # Handle Collections retrieval using a different endpoint 
    if normalized_entity_class == 'Collection':
        bad_request_error("Please use another API endpoint `/collections` to query collections")

    # Get back a list of entity dicts for the given entity class
    entities_list = app_neo4j_queries.get_entities_by_class(neo4j_driver_instance, normalized_entity_class)

    # Generate trigger data and merge into a big dict
    complete_entities_list = schema_manager.get_complete_entities_list(entities_list)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    # Result filtering based on query string
    result_filtering_accepted_property_keys = ['uuid']
    separator = ', '
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error("Only the following property keys are supported in the query string: " + separator.join(result_filtering_accepted_property_keys))
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_entities_by_class(neo4j_driver_instance, normalized_entity_class, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    # Response with the final result
    return jsonify(final_result)


"""
Retrive the collection detail by id

An optional Globus nexus token can be provided in a standard Authentication Bearer header. If a valid token
is provided with group membership in the HuBMAP-Read group any collection matching the id will be returned.
otherwise if no token is provided or a valid token with no HuBMAP-Read group membership then
only a public collection will be returned.  Public collections are defined as being published via a DOI 
(collection.has_doi == true) and at least one of the connected datasets is public
(dataset.data_access_level == 'public'). For public collections only connected datasets that are
public are returned with it.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target collection 

Returns
-------
json
    The collection detail with a list of connected datasets (only public datasets 
    if user doesn't have the right access permission)
"""
@app.route('/collections/<id>', methods = ['GET'])
def get_collection(id):
    # Query target entity against uuid-api and neo4j and return as a dict if exists
    collection_dict = query_target_entity(id)

    # A bit validation 
    if collection_dict['entity_class'] != 'Collection':
        bad_request_error("Target entity of the given id is not a collection")

    # Get user data_access_level based on token
    user_info = auth_helper.getUserDataAccessLevel(request) 

    # If the user can only access public collections, modify the collection result
    # by only returning public datasets attached to this collection
    if user_info['data_access_level'] == 'public': 
        # When the requested collection is not public but the user can only access public data
        if ('has_doi' not in collection_dict) or (not collection_dict['has_doi']):
            bad_request_error("The reqeust collection is not public, please send a Globus token with the right access permission in the request.")

        # Only return the public datasets attached to this collection for Collection.datasets property
        complete_dict = get_complete_public_collection_dict(collection_dict)
    else:
        # We'll need to return all the properties including those 
        # generated by `on_read_trigger` to have a complete result
        complete_dict = schema_manager.get_complete_entity_result(collection_dict)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_entity_result_for_response(complete_dict)

    # Response with the final result
    return jsonify(normalized_complete_dict)


"""
Retrive all the collection nodes
Result filtering is supported based on query string
For example: /collections?property=uuid

An optional Globus nexus token can be provided in a standard Authentication Bearer header. If a valid token
is provided with group membership in the HuBMAP-Read group any collection matching the id will be returned.
otherwise if no token is provided or a valid token with no HuBMAP-Read group membership then
only a public collection will be returned.  Public collections are defined as being published via a DOI 
(collection.has_doi == true) and at least one of the connected datasets is public
(dataset.data_access_level == 'public'). For public collections only connected datasets that are
public are returned with it.

Returns
-------
json
    A list of all the collection dictionaries (only public collection with 
    attached public datasts if the user/token doesn't have the right access permission)
"""
@app.route('/collections', methods = ['GET'])
def get_collections():
    normalized_entity_class = 'Collection'

    # Get user data_access_level based on token
    user_info = auth_helper.getUserDataAccessLevel(request) 

    # If the user can only access public collections, modify the resultss
    # by only returning public datasets attached to each collection
    if user_info['data_access_level'] == 'public': 
        # Get back a list of public collections dicts
        collections_list = app_neo4j_queries.get_public_collections(neo4j_driver_instance)
        
        # Modify the Collection.datasets property for each collection dict 
        # to contain only public datasets
        for collection_dict in collections_list:
            # Only return the public datasets attached to this collection for Collection.datasets property
            collection_dict = get_complete_public_collection_dict(collection_dict)
    else:
        # Get back a list of all collections dicts
        collections_list = app_neo4j_queries.get_entities_by_class(neo4j_driver_instance, normalized_entity_class)

    # Generate trigger data and merge into a big dict
    complete_collections_list = schema_manager.get_complete_entities_list(collections_list)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_collections_list)

    # Result filtering based on query string
    result_filtering_accepted_property_keys = ['uuid']
    separator = ', '
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error("Only the following property keys are supported in the query string: " + separator.join(result_filtering_accepted_property_keys))
            
            if user_info['data_access_level'] == 'public':
                # Only return a list of the filtered property value of each public collection
                property_list = app_neo4j_queries.get_public_collections(neo4j_driver_instance, property_key)
            else:
                # Only return a list of the filtered property value of each entity
                property_list = app_neo4j_queries.get_entities_by_class(neo4j_driver_instance, normalized_entity_class, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    # Response with the final result
    return jsonify(final_result)


"""
Create an entity of the target class in neo4j

Parameters
----------
entity_class : str
    One of the target entity classes (case-insensitive since will be normalized): Dataset, Collection, Sample, but NOT Donor or Collection

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/entities/<entity_class>', methods = ['POST'])
def create_entity(entity_class):
    # Normalize user provided entity_class
    normalized_entity_class = schema_manager.normalize_entity_class(entity_class)

    # Validate the normalized_entity_class to make sure it's one of the accepted classes
    try:
        schema_manager.validate_normalized_entity_class(normalized_entity_class)
    except schema_errors.InvalidNormalizedEntityClassException as e:
        bad_request_error("Invalid entity class provided: " + entity_class)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    try:
        schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_class)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        bad_request_error(str(e))

    # Sample and Dataset: additional validation, create entity, after_create_trigger
    # Collection and Donor: create entity
    if normalized_entity_class == 'Sample':
        # A bit more validation for new sample to be linked to existing source entity
        has_direct_ancestor_uuid = False
        if ('direct_ancestor_uuid' in json_data_dict) and json_data_dict['direct_ancestor_uuid']:
            has_source_uuid = True

            direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
            # Check existence of the source entity (either another Sample or Donor)
            source_dict = query_target_entity(direct_ancestor_uuid)

        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_class, json_data_dict)

        # For new sample to be linked to existing direct ancestor
        if has_direct_ancestor_uuid:
            after_create(normalized_entity_class, merged_dict)
    elif normalized_entity_class == 'Dataset':    
        # A bit more validation if `direct_ancestor_uuids` provided
        has_direct_ancestor_uuids = False
        if ('direct_ancestor_uuids' in json_data_dict) and (json_data_dict['direct_ancestor_uuids']):
            has_direct_ancestor_uuids = True

            # Check existence of those source entities
            for direct_ancestor_uuid in json_data_dict['direct_ancestor_uuids']:
                direct_ancestor_dict = query_target_entity(direct_ancestor_uuid)

        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_class, json_data_dict)

        # Handle direct_ancestor_uuids via `after_create_trigger` methods 
        if has_direct_ancestor_uuids:
            after_create(normalized_entity_class, merged_dict)
    else:
        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_class, json_data_dict)

    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(merged_dict)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_entity_result_for_response(complete_dict)

    return jsonify(normalized_complete_dict)


"""
Update the properties of a given entity, no Collection stuff

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
json
    All the updated properties of the target entity
"""
@app.route('/entities/<id>', methods = ['PUT'])
def update_entity(id):
    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Get target entity and return as a dict if exists
    entity_dict = query_target_entity(id)

    # Normalize user provided entity_class
    normalized_entity_class = schema_manager.normalize_entity_class(entity_dict['entity_class'])

    # Validate request json against the yaml schema
    # Pass in the entity_dict for missing required key check, this is different from creating new entity
    try:
        schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_class, existing_entity_dict = entity_dict)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        bad_request_error(str(e))

    # Sample and Dataset: additional validation, update entity, after_update_trigger
    # Collection and Donor: update entity
    if normalized_entity_class == 'Sample':
        # A bit more validation for new sample to be linked to existing source entity
        has_direct_ancestor_uuid = False
        if ('direct_ancestor_uuid' in json_data_dict) and json_data_dict['direct_ancestor_uuid']:
            has_source_uuid = True

            direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
            # Check existence of the source entity (either another Sample or Donor)
            source_dict = query_target_entity(direct_ancestor_uuid)

        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_class, json_data_dict, entity_dict)

        # For sample to be linked to existing direct ancestor
        if has_direct_ancestor_uuid:
            after_update(normalized_entity_class, merged_updated_dict)
    elif normalized_entity_class == 'Dataset':    
        # A bit more validation if `direct_ancestor_uuids` provided
        has_direct_ancestor_uuids = False
        if ('direct_ancestor_uuids' in json_data_dict) and (json_data_dict['direct_ancestor_uuids']):
            has_direct_ancestor_uuids = True

            # Check existence of those source entities
            for direct_ancestor_uuid in json_data_dict['direct_ancestor_uuids']:
                direct_ancestor_dict = query_target_entity(direct_ancestor_uuid)
        
        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_class, json_data_dict, entity_dict)

        # Handle direct_ancestor_uuids via `after_update_trigger` methods 
        if has_direct_ancestor_uuids:
            after_update(normalized_entity_class, merged_updated_dict)
    else:
        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_class, json_data_dict, entity_dict)

    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(merged_updated_dict)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_entity_result_for_response(complete_dict)

    # How to handle reindex collection?
    # Also reindex the updated entity node in elasticsearch via search-api
    reindex_entity(entity_dict['uuid'])

    return jsonify(normalized_complete_dict)

"""
Get all ancestors of the given entity
Result filtering based on query string
For example: /ancestors/<id>?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity 

Returns
-------
json
    A list of all the ancestors of the target entity
"""
@app.route('/ancestors/<id>', methods = ['GET'])
def get_ancestors(id):
    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id)
    uuid = entity_dict['uuid']
    
    ancestors_list = app_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid)

    # Generate trigger data and merge into a big dict
    complete_entities_list = schema_manager.get_complete_entities_list(ancestors_list)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    # Result filtering based on query string
    result_filtering_accepted_property_keys = ['uuid']
    separator = ', '
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error("Only the following property keys are supported in the query string: " + separator.join(result_filtering_accepted_property_keys))
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    return jsonify(final_result)


"""
Get all descendants of the given entity
Result filtering based on query string
For example: /descendants/<id>?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the descendants of the target entity
"""
@app.route('/descendants/<id>', methods = ['GET'])
def get_descendants(id):
    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id)
    uuid = entity_dict['uuid']

    descendants_list = app_neo4j_queries.get_descendants(neo4j_driver_instance, uuid)

    # Generate trigger data and merge into a big dict
    complete_entities_list = schema_manager.get_complete_entities_list(descendants_list)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    # Result filtering based on query string
    result_filtering_accepted_property_keys = ['uuid']
    separator = ', '
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error("Only the following property keys are supported in the query string: " + separator.join(result_filtering_accepted_property_keys))
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_descendants(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    return jsonify(final_result)

"""
Get all parents of the given entity
Result filtering based on query string
For example: /parents/<id>?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the parents of the target entity
"""
@app.route('/parents/<id>', methods = ['GET'])
def get_parents(id):
    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id)
    uuid = entity_dict['uuid']
 
    parents_list = app_neo4j_queries.get_parents(neo4j_driver_instance, uuid)

    # Generate trigger data and merge into a big dict
    complete_entities_list = schema_manager.get_complete_entities_list(parents_list)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    # Result filtering based on query string
    result_filtering_accepted_property_keys = ['uuid']
    separator = ', '
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error("Only the following property keys are supported in the query string: " + separator.join(result_filtering_accepted_property_keys))
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_parents(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    return jsonify(final_result)

"""
Get all chilren of the given entity
Result filtering based on query string
For example: /children/<id>?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the children of the target entity
"""
@app.route('/children/<id>', methods = ['GET'])
def get_children(id):
    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id)
    uuid = entity_dict['uuid']

    children_list = app_neo4j_queries.get_children(neo4j_driver_instance, uuid)

    # Generate trigger data and merge into a big dict
    complete_entities_list = schema_manager.get_complete_entities_list(children_list)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    # Result filtering based on query string
    result_filtering_accepted_property_keys = ['uuid']
    separator = ', '
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error("Only the following property keys are supported in the query string: " + separator.join(result_filtering_accepted_property_keys))
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_children(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    return jsonify(final_result)



"""
Link the given list of datasets to the target collection

JSON request body example:
{
    "dataset_uuids": [
        "fb6757b606ac35be7fa85062fde9c2e1",
        "81a9fa68b2b4ea3e5f7cb17554149473",
        "3ac0768d61c6c84f0ec59d766e123e05",
        "0576b972e074074b4c51a61c3d17a6e3"
    ]
}

Parameters
----------
collection_uuid : str
    The UUID of target collection 

Returns
-------
json
    JSON string containing a success message with 200 status code
"""
@app.route('/collections/<collection_uuid>/add-datasets', methods = ['PUT'])
def add_datasets_to_collection(collection_uuid):
    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(collection_uuid)
    if entity_dict['entity_class'] != 'Collection':
        bad_request_error("The UUID provided in URL is not a Collection: " + collection_uuid)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python list object)
    json_data_dict = request.get_json()

    if 'dataset_uuids' not in json_data_dict:
        bad_request_error("Missing 'dataset_uuids' key in the request JSON.")

    if not json_data_dict['dataset_uuids']:
        bad_request_error("JSON field 'dataset_uuids' can not be empty list.")

    # Now we have a list of uuids
    dataset_uuids_list = json_data_dict['dataset_uuids']

    # Make sure all the given uuids are datasets
    for dataset_uuid in dataset_uuids_list:
        entity_dict = query_target_entity(dataset_uuid)
        if entity_dict['entity_class'] != 'Dataset':
            bad_request_error("The UUID provided in JSON is not a Dataset: " + dataset_uuid)

    try:
        app_neo4j_queries.add_datasets_to_collection(neo4j_driver_instance, collection_uuid, dataset_uuids_list)
    except TransactionError:
        msg = "Failed to create the linkage between the given datasets and the target collection"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

    # Send response with success message
    return jsonify(message = "Successfully added all the specified datasets to the target collection")

"""
Redirect a request from a doi service for a collection of data

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity
"""
@app.route('/collection/redirect/<id>', methods = ['GET'])
def collection_redirect(id):
    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id)

    # Only for collection
    if entity_dict['entity_class'] != 'Collection':
        bad_request_error("The target entity of the specified id is not a Collection")

    uuid = entity_dict['uuid']

    # URL template
    redirect_url = app.config['COLLECTION_REDIRECT_URL']

    if redirect_url.lower().find('<identifier>') == -1:
        internal_server_error("Incorrect configuration value for 'COLLECTION_REDIRECT_URL'")

    rep_pattern = re.compile(re.escape('<identifier>'), re.RegexFlag.IGNORECASE)
    redirect_url = rep_pattern.sub(uuid, redirect_url)
    
    return redirect(redirect_url, code = 307)

"""
Get the Globus URL to the given dataset

It will provide a Globus URL to the dataset directory in of three Globus endpoints based on the access
level of the user (public, consortium or protected), public only, of course, if no token is provided.
If a dataset isn't found a 404 will be returned. There is a chance that a 500 can be returned, but not
likely under normal circumstances, only for a misconfigured or failing in some way endpoint. If the 
Auth token is provided but is expired or invalid a 401 is returned.  If access to the dataset is not
allowed for the user (or lack of user) a 403 is returned.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
Response
    200 with the Globus Application URL to the datasets's directory
    404 Dataset not found
    403 Access Forbidden
    401 Unauthorized (bad or expired token)
    500 Unexpected server or other error
"""
@app.route('/dataset/globus-url/<id>', methods = ['GET'])
def get_dataset_globus_url(id):
    # For now, don't use the constants from commons
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    # Then retrieve the allowable data access level (public, protected or consortium)
    # for the dataset and HuBMAP Component ID that the dataset belongs to
    entity_dict = query_target_entity(id)
    
    # Only for dataset
    if entity_dict['entity_class'] != 'Dataset':
        bad_request_error("The target entity enity of the specified id is not a Dataset")

    uuid = entity_dict['uuid']

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
    
    # 'data_access_level' is always available since it's transint property
    data_access_level = entity_dict['data_access_level']

    if not 'group_uuid' in entity_dict or string_helper.isBlank(entity_dict['group_uuid']):
        internal_server_error("Group uuid not set for dataset with id: " + id)

    #look up the Component's group ID, return an error if not found
    data_group_id = entity_dict['group_uuid']
    if not data_group_id in groups_by_id_dict:
        internal_server_error("Can not find dataset group: " + data_group_id + " for id: " + id)

    # Get the user information (if available) for the caller
    # getUserDataAccessLevel will return just a "data_access_level" of public
    # if no auth token is found
    user_info = auth_helper.getUserDataAccessLevel(request)        
    
    #construct the Globus URL based on the highest level of access that the user has
    #and the level of access allowed for the dataset
    #the first "if" checks to see if the user is a member of the Consortium group
    #that allows all access to this dataset, if so send them to the "protected"
    #endpoint even if the user doesn't have full access to all protected data
    globus_server_uuid = None        
    dir_path = ""
    
    #the user is in the Globus group with full access to thie dataset,
    #so they have protected level access to it
    if 'hmgroupids' in user_info and data_group_id in user_info['hmgroupids']:
        user_access_level = 'protected'
    else:
        if not 'data_access_level' in user_info:
            internal_server_error("Unexpected error, data access level could not be found for user trying to access dataset uuid:" + uuid)        
        user_access_level = user_info['data_access_level']

    #public access
    if data_access_level == ACCESS_LEVEL_PUBLIC:
        globus_server_uuid = app.config['GLOBUS_PUBLIC_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['PUBLIC_DATA_SUBDIR'])
        dir_path = dir_path +  access_dir + "/"
    #consortium access
    elif data_access_level == ACCESS_LEVEL_CONSORTIUM and not user_access_level == ACCESS_LEVEL_PUBLIC:
        globus_server_uuid = app.config['GLOBUS_CONSORTIUM_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['CONSORTIUM_DATA_SUBDIR'])
        dir_path = dir_path + access_dir + groups_by_id_dict[data_group_id]['displayname'] + "/"
    #protected access
    elif user_access_level == ACCESS_LEVEL_PROTECTED and data_access_level == ACCESS_LEVEL_PROTECTED:
        globus_server_uuid = app.config['GLOBUS_PROTECTED_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['PROTECTED_DATA_SUBDIR'])
        dir_path = dir_path + access_dir + groups_by_id_dict[data_group_id]['displayname'] + "/"
        
    if globus_server_uuid is None:
        forbidden_error("Access not granted")   

    dir_path = dir_path + uuid + "/"
    dir_path = urllib.parse.quote(dir_path, safe='')
    
    #https://app.globus.org/file-manager?origin_id=28bbb03c-a87d-4dd7-a661-7ea2fb6ea631&origin_path=%2FIEC%20Testing%20Group%2F03584b3d0f8b46de1b629f04be156879%2F
    url = file_helper.ensureTrailingSlashURL(app.config['GLOBUS_APP_BASE_URL']) + "file-manager?origin_id=" + globus_server_uuid + "&origin_path=" + dir_path  
            
    return Response(url, 200)


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
Throws error for 403 Forbidden with message

Parameters
----------
err_msg : str
    The custom error message to return to end users
"""
def forbidden_error(err_msg):
    abort(403, description = err_msg)

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
Return the complete collection dict for a given raw collection dict

Parameters
----------
collection_dict : dict
    The raw collection dict returned by Neo4j

Returns
-------
dict
    A dictionary of complete collection detail with all the generated 'on_read_trigger' data
    The generated Collection.datasts contains only public datasets 
    if user/token doesn't have the right access permission
"""
def get_complete_public_collection_dict(collection_dict):
    # Collection.datasets is transient property and generated by the trigger method
    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(collection_dict)
    logger.info("--------complete_dict before---------")
    logger.info(complete_dict)

    # Loop through Collection.datasets and only return the public datasets
    public_datasets = [] 
    for dataset in complete_dict['datasets']:
        if dataset['data_access_level'] == 'public':
            public_datasets.append(dataset)

    # Modify the result and only show the public datasets in this collection
    complete_dict['datasets'] = public_datasets

    logger.info("--------complete_dict after---------")
    logger.info(complete_dict)

    return complete_dict


"""
Generate 'before_create_triiger' data and create the entity details in Neo4j

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
json_data_dict: dict
    The json request dict

Returns
-------
dict
    A dict of all the newly created entity detials
"""
def create_entity_details(request, normalized_entity_class, json_data_dict):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Create new ids for the new entity
    new_ids_dict = schema_manager.create_hubmap_ids(normalized_entity_class)

    # Merge all the above dictionaries and pass to the trigger methods
    data_dict = {**user_info_dict, **new_ids_dict}

    try:
        generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data('before_create_trigger', normalized_entity_class, data_dict)
    # If one of the before_create_trigger methods fails, we can't create the entity
    except schema_errors.BeforeCreateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_create_trigger' methods, can't create the entity"
        logger.exception(msg)
        internal_server_error(msg)
    except (schema_errors.NoDataProviderGroupException, schema_errors.MultipleDataProviderGroupException):
        # Log the full stack trace, prepend a line with our message
        msg = "The user does not have the correct Globus group associated with, can't create the entity"
        logger.exception(msg)
        bad_request_error(msg)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_before_create_trigger_data_dict}

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    logger.debug("======create_entity_details() escaped_json_list_str======")
    logger.debug(escaped_json_list_str)

    # Create new entity
    try:
        entity_dict = app_neo4j_queries.create_entity(neo4j_driver_instance, normalized_entity_class, escaped_json_list_str)
    except TransactionError:
        msg = "Failed to create the new " + normalized_entity_class
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

    # Add user_info_dict because it may be used by after_update_trigger methods
    merged_final_dict = {**merged_dict, **user_info_dict}

    # Note: return merged_final_dict instead of entity_dict because 
    # it contains all the user json data that the generated that entity_dict may not have
    return merged_final_dict

"""
Execute 'after_create_triiger' methods

Parameters
----------
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
entity_dict: dict
    The entity dict newly created
"""
def after_create(normalized_entity_class, entity_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        schema_manager.generate_triggered_data('after_create_trigger', normalized_entity_class, entity_dict)
    except schema_errors.AfterCreateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "The entity has been created, but failed to execute one of the 'after_create_trigger' methods"
        logger.exception(msg)
        internal_server_error(msg)


"""
Generate 'before_create_triiger' data and create the entity details in Neo4j

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
json_data_dict: dict
    The json request dict
existing_entity_dict: dict
    Dict of the exiting entity information

Returns
-------
dict
    A dict of all the updated entity detials
"""
def update_entity_details(request, normalized_entity_class, json_data_dict, existing_entity_dict):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Merge user_info_dict and the existing_entity_dict for passing to the trigger methods
    data_dict = {**user_info_dict, **existing_entity_dict}

    try:
        generated_before_update_trigger_data_dict = schema_manager.generate_triggered_data('before_update_trigger', normalized_entity_class, data_dict)
    # If one of the before_update_trigger methods fails, we can't update the entity
    except schema_errors.BeforeUpdateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_update_trigger' methods, can't update the entity"
        logger.exception(msg)
        internal_server_error(msg)

    # Merge dictionaries
    merged_dict = {**json_data_dict, **generated_before_update_trigger_data_dict}

    # By now the merged_dict contains all user updates and all triggered data to be added to the entity node
    # Any properties in merged_dict that are not on the node will be added.
    # Any properties not in merged_dict that are on the node will be left as is.
    # Any properties that are in both merged_dict and the node will be replaced in the node. However, if any property in the map is null, it will be removed from the node.
    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    logger.debug("======update_entity_details() json_list_str======")
    logger.debug(json_list_str)

    # Update the exisiting entity
    try:
        updated_entity_dict = app_neo4j_queries.update_entity(neo4j_driver_instance, normalized_entity_class, escaped_json_list_str, existing_entity_dict['uuid'])
    except TransactionError:
        msg = "Failed to update the entity with id " + id
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

    # The duplicate key-value pairs existing in json_data_dict get replaced by the same key-value pairs
    # from the updated_entity_dict, but json_data_dict may contain keys that are not in json_data_dict
    # E.g., direct_ancestor_uuids for Dataset and direct_ancestor_uuid for Sample, both are transient properties
    # Add user_info_dict because it may be used by after_update_trigger methods
    merged_final_dict = {**json_data_dict, **user_info_dict, **updated_entity_dict}

    # Use merged_final_dict instead of merged_dict because 
    # merged_dict only contains properties to be updated, not all properties
    return merged_final_dict

"""
Execute 'after_update_triiger' methods

Parameters
----------
normalized_entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
entity_dict: dict
    The entity dict newly updated
"""
def after_update(normalized_entity_class, entity_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        schema_manager.generate_triggered_data('after_update_trigger', normalized_entity_class, entity_dict)
    except schema_errors.AfterUpdateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "The entity information has been updated, but failed to execute one of the 'after_update_trigger' methods"
        logger.exception(msg)
        internal_server_error(msg)


"""
Get target entity dict

Parameters
----------
id : str
    The uuid or hubmap_id of target entity

Returns
-------
dict
    A dictionary of entity details returned from neo4j
"""
def query_target_entity(id):
    try:
        hubmap_ids = schema_manager.get_hubmap_ids(id)

        # Get the target uuid if all good
        uuid = hubmap_ids['hmuuid']
        entity_dict = app_neo4j_queries.get_entity(neo4j_driver_instance, uuid)

        # The uuid exists via uuid-api doesn't mean it's also in Neo4j
        if not bool(entity_dict):
            not_found_error("Entity of id: " + id + " not found in Neo4j")

        return entity_dict
    except requests.exceptions.HTTPError as e:
        not_found_error(e)
    except requests.exceptions.RequestException as e:
        # Something wrong with the request to uuid-api
        internal_server_error(e)

"""
Always expect a json body from user request

request : Flask request object
    The Flask request passed from the API endpoint
"""
def require_json(request):
    if not request.is_json:
        bad_request_error("A json body and appropriate Content-Type header are required")

"""
Make a call to search-api to reindex this entity node in elasticsearch

Parameters
----------
request : Flask request object
    The Flask request passed from the API endpoint 

Returns
-------
dict
"""
def reindex_entity(uuid):
    try:
        response = requests.put(app.config['SEARCH_API_URL'] + "/reindex/" + uuid)
        # The reindex takes time, so 202 Accepted response status code indicates that 
        # the request has been accepted for processing, but the processing has not been completed
        if response.status_code == 202:
            logger.info("The search-api has accepted the reindex request for uuid: " + uuid)
        else:
            logger.error("The search-api failed to initialize the reindex for uuid: " + uuid)
    except Exception:
        msg = "Failed to send the reindex request to search-api for entity with uuid: " + uuid
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

"""
Ensure the access level dir with leading and trailing slashes

dir_name : str
    The name of the sub directory corresponding to each access level

Returns
-------
str 
    One of the formatted dir path string: /public/, /protected/, /consortium/
"""
def access_level_prefix_dir(dir_name):
    if string_helper.isBlank(dir_name):
        return ''
    else:
        return file_helper.ensureTrailingSlashURL(file_helper.ensureBeginningSlashURL(dir_name))

