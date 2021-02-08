from flask import Flask, g, jsonify, abort, request, Response, redirect
from neo4j.exceptions import TransactionError
import os
import re
import json
import requests
import urllib
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
import logging

# Local modules
import app_neo4j_queries
import provenance
from file_upload_helper import UploadFileHelper
from schema import schema_manager
from schema import schema_errors

# HuBMAP commons
from hubmap_commons import string_helper
from hubmap_commons import file_helper as hm_file_helper
from hubmap_commons import neo4j_driver
from hubmap_commons import globus_groups
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.exceptions import HTTPException


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
## Register error handlers
####################################################################################################

# Error handler for 400 Bad Request with custom error message
@app.errorhandler(400)
def http_bad_request(e):
    return jsonify(error=str(e)), 400

# Error handler for 401 Unauthorized with custom error message
@app.errorhandler(401)
def http_unauthorized(e):
    return jsonify(error=str(e)), 401

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
## AuthHelper initialization
####################################################################################################

# Initialize AuthHelper class and ensure singleton
try:
    if AuthHelper.isInitialized() == False:
        auth_helper_instance = AuthHelper.create(app.config['APP_CLIENT_ID'], 
                                                 app.config['APP_CLIENT_SECRET'])

        logger.info("Initialized AuthHelper class successfully :)")
    else:
        auth_helper_instance = AuthHelper.instance()
except Exception:
    msg = "Failed to initialize the AuthHelper class"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## Neo4j connection initialization
####################################################################################################

# The neo4j_driver (from commons package) is a singleton module
# This neo4j_driver_instance will be used for application-specifc neo4j queries
# as well as being passed to the schema_manager
try:
    neo4j_driver_instance = neo4j_driver.instance(app.config['NEO4J_URI'], 
                                                  app.config['NEO4J_USERNAME'], 
                                                  app.config['NEO4J_PASSWORD'])

    logger.info("Initialized neo4j_driver module successfully :)")
except Exception:
    msg = "Failed to initialize the neo4j_driver module"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


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
## File upload initialization
####################################################################################################

try:
    # Initialize the UploadFileHelper class and ensure singleton
    if UploadFileHelper.is_initialized() == False:
        file_upload_helper_instance = UploadFileHelper.create(app.config['FILE_UPLOAD_TEMP_DIR'], 
                                                              app.config['FILE_UPLOAD_DIR'],
                                                              app.config['UUID_API_URL'])

        logger.info("Initialized UploadFileHelper class successfully :)")

        # This will delete all the temp dirs on restart
        file_upload_helper_instance.clean_temp_dir()
    else:
        file_upload_helper_instance = UploadFileHelper.instance()
# Use a broad catch-all here
except Exception:
    msg = "Failed to initialize the UploadFileHelper class"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## Schema initialization
####################################################################################################

try:
    # The schema_manager is a singleton module
    # Pass in auth_helper_instance, neo4j_driver instance, and file_upload_helper instance
    schema_manager.initialize(app.config['SCHEMA_YAML_FILE'], 
                              app.config['UUID_API_URL'],
                              auth_helper_instance,
                              neo4j_driver_instance,
                              file_upload_helper_instance)

    logger.info("Initialized schema_manager module successfully :)")
# Use a broad catch-all here
except Exception:
    msg = "Failed to initialize the schema_manager module"
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)

####################################################################################################
## Constants
####################################################################################################

# For now, don't use the constants from commons
# All lowercase for easy comparision
ACCESS_LEVEL_PUBLIC = 'public'
ACCESS_LEVEL_CONSORTIUM = 'consortium'
ACCESS_LEVEL_PROTECTED = 'protected'


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
        'version': (Path(__file__).absolute().parent.parent / 'VERSION').read_text().strip(),
        'build': (Path(__file__).absolute().parent.parent / 'BUILD').read_text().strip(),
        'neo4j_connection': False
    }
    
    # Don't use try/except here
    is_connected = app_neo4j_queries.check_connection(neo4j_driver_instance)
    
    if is_connected:
        status_data['neo4j_connection'] = True

    return jsonify(status_data)


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
    # Use the internal token to query the target entity 
    # since public entities don't require user token
    token = get_internal_token() 

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    entity_data_access_level = entity_dict['data_access_level']

    # Handle Collection retrieval using a different endpoint 
    if entity_dict['entity_type'] == 'Collection':
        bad_request_error("Please use another API endpoint `/collections/<id>` to query a collection")

    error_msg_401 = f"A valid globus token is required to retrieve the entity information for {normalized_entity_type}: {id}"
    error_msg_403 = f"Not authorized to retrieve the entity information for {normalized_entity_type}: {id}"

    # The `data_access_level` of Dataset can be 'public', consortium', or 'protected'
    if normalized_entity_type == 'Dataset':
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != 'published':
            # Get user data_access_level based on token
            try:
                # The user_info contains HIGHEST access level of the user based on the token
                # This call raises an HTTPException with a 401 if any auth issues encountered
                user_info = auth_helper_instance.getUserDataAccessLevel(request)
            # If returns HTTPException with a 401, invalid header format or expired/invalid token
            except HTTPException:
                unauthorized_error(error_msg_401)

            # Now there's a valid token, will need to check the access level
            user_data_access_level = user_info['data_access_level'].lower()

            # Use two checks instead of merging them into one long check for readibility
            if (entity_data_access_level == ACCESS_LEVEL_CONSORTIUM) and (user_data_access_level == ACCESS_LEVEL_PUBLIC):
                forbidden_error(error_msg_403)

            if (entity_data_access_level == ACCESS_LEVEL_PROTECTED) and (user_data_access_level != ACCESS_LEVEL_PROTECTED):
                forbidden_error(error_msg_403)

            # Pass the user token instead the internal token for subsequent function calls
            token = get_user_token(request.headers)
    else:
        # The `data_access_level` of Donor/Sample can only be either 'public' or 'consortium'
        if entity_data_access_level == ACCESS_LEVEL_CONSORTIUM:
            # Get user data_access_level based on token
            try:
                # The user_info contains HIGHEST access level of the user based on the token
                # This call raises an HTTPException with a 401 if any auth issues encountered
                user_info = auth_helper_instance.getUserDataAccessLevel(request)
            # If returns HTTPException with a 401, invalid header format or expired/invalid token
            except HTTPException:
                unauthorized_error(error_msg_401)

            # Now there's a valid token, will need to check the access level
            user_data_access_level = user_info['data_access_level'].lower()

            if user_data_access_level == ACCESS_LEVEL_PUBLIC:
                forbidden_error(error_msg_403)

            # Pass the user token instead the internal token for subsequent function calls
            token = get_user_token(request.headers)

    # By now, either the entity is public accessible or the user token has the correct access level
    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(token, entity_dict)

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
                bad_request_error(f"Only the following property keys are supported in the query string: {separator.join(result_filtering_accepted_property_keys)}")
            
            # Only return the property value
            property_value = final_entity_dict[property_key]

            # Final result
            final_result = property_value
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    # Response with the final result
    return jsonify(final_result)


"""
Retrive the full tree above the referenced entity and build the provenance document

This endpoint is marked as public in Gateway
But we need to enforce the access control here

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
json
    All the provenance details associated with this entity
"""
@app.route('/entities/<id>/provenance', methods = ['GET'])
def get_entity_provenance(id):
    # Use the internal token to query the target entity 
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    uuid = entity_dict['uuid']
    normalized_entity_type = entity_dict['entity_type']
    entity_data_access_level = entity_dict['data_access_level']

    # A bit validation to prevent Lab or Collection being queried
    supported_entity_types = ['Donor', 'Sample', 'Dataset']
    separator = ', '
    if normalized_entity_type not in supported_entity_types:
        bad_request_error(f"Unable to get the provenance for this {normalized_entity_type}, the requested entity must be one of: {separator.join(supported_entity_types)}")

    error_msg_401 = f"A valid globus token is required to retrieve the provenance information for {normalized_entity_type}: {id}"
    error_msg_403 = f"Not authorized to retrieve the provenance information for {normalized_entity_type}: {id}"

    # The `data_access_level` of Dataset can be 'public', consortium', or 'protected'
    if normalized_entity_type == 'Dataset':
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != 'published':
            # Get user data_access_level based on token
            try:
                # The user_info contains HIGHEST access level of the user based on the token
                # This call raises an HTTPException with a 401 if any auth issues encountered
                user_info = auth_helper_instance.getUserDataAccessLevel(request)
            # If returns HTTPException with a 401, invalid header format or expired/invalid token
            except HTTPException:
                unauthorized_error(error_msg_401)

            # Now there's a valid token, will need to check the access level
            user_data_access_level = user_info['data_access_level'].lower()

            # Use two checks instead of merging them into one long check for readibility
            if (entity_data_access_level == ACCESS_LEVEL_CONSORTIUM) and (user_data_access_level == ACCESS_LEVEL_PUBLIC):
                forbidden_error(error_msg_403)

            if (entity_data_access_level == ACCESS_LEVEL_PROTECTED) and (user_data_access_level != ACCESS_LEVEL_PROTECTED):
                forbidden_error(error_msg_403)

            # Pass the user token instead the internal token for subsequent function calls
            token = get_user_token(request.headers)
    else:
        # The `data_access_level` of Donor/Sample can only be either 'public' or 'consortium'
        if entity_data_access_level == ACCESS_LEVEL_CONSORTIUM:
            # Get user data_access_level based on token
            try:
                # The user_info contains HIGHEST access level of the user based on the token
                # This call raises an HTTPException with a 401 if any auth issues encountered
                user_info = auth_helper_instance.getUserDataAccessLevel(request)
            # If returns HTTPException with a 401, invalid header format or expired/invalid token
            except HTTPException:
                unauthorized_error(error_msg_401)

            # Now there's a valid token, will need to check the access level
            user_data_access_level = user_info['data_access_level'].lower()

            if user_data_access_level == ACCESS_LEVEL_PUBLIC:
                forbidden_error(error_msg_403)

            # Pass the user token instead the internal token for subsequent function calls
            token = get_user_token(request.headers)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Will just proceed to get the provenance informaiton
    # Get the `depth` from query string if present and it's used by neo4j query
    # to set the maximum number of hops in the traversal
    depth = None
    if 'depth' in request.args:
        depth = int(request.args.get('depth'))

    # Convert neo4j json to dict
    neo4j_result = app_neo4j_queries.get_provenance(neo4j_driver_instance, uuid, depth)
    raw_provenance_dict = dict(neo4j_result['json'])

    # Normalize the raw provenance nodes based on the yaml schema
    normalized_provenance_dict = {
        'relationships': raw_provenance_dict['relationships'],
        'nodes':[]
    }

    for node_dict in raw_provenance_dict['nodes']:
        # The schema yaml doesn't handle Lab nodes, just leave it as is
        if (node_dict['label'] == 'Entity') and (node_dict['entity_type'] != 'Lab'):
            # We'll need to return all the properties including those 
            # generated by `on_read_trigger` to have a complete result
            properties_to_skip = ['direct_ancestors', 'direct_ancestor']
            complete_entity_dict = schema_manager.get_complete_entity_result(token, node_dict, properties_to_skip)
            # Filter out properties not defined or not to be exposed in the schema yaml
            normalized_entity_dict = schema_manager.normalize_entity_result_for_response(complete_entity_dict)

            # Now the node to be used by provenance is all regulated by the schmea
            normalized_provenance_dict['nodes'].append(normalized_entity_dict)
        elif (node_dict['label'] == 'Activity'):
            # Normalize Activity nodes too
            normalized_activity_dict = schema_manager.normalize_activity_result_for_response(node_dict)
            normalized_provenance_dict['nodes'].append(normalized_activity_dict)
        else:
            # Skip Entity Lab nodes
            normalized_provenance_dict['nodes'].append(node_dict)

    provenance_json = provenance.get_provenance_history(normalized_provenance_dict)
    
    # Response with the provenance details
    return Response(response = provenance_json, mimetype = "application/json")

"""
Show all the supported entity types

Returns
-------
json
    A list of all the available entity types defined in the schema yaml
"""
@app.route('/entity-types', methods = ['GET'])
def get_entity_types():
    return jsonify(schema_manager.get_all_entity_types())

"""
Retrive all the entity nodes for a given entity type
Result filtering is supported based on query string
For example: /<entity_type>/entities?property=uuid

Parameters
----------
entity_type : str
    One of the normalized entity types: Dataset, Sample, Donor
    Will handle Collection via API endpoint `/collections`

Returns
-------
json
    All the entity nodes in a list of the target entity type
"""
@app.route('/<entity_type>/entities', methods = ['GET'])
def get_entities_by_type(entity_type):
    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_type)

    # Validate the normalized_entity_type to ensure it's one of the accepted types
    try:
        schema_manager.validate_normalized_entity_type(normalized_entity_type)
    except schema_errors.InvalidNormalizedEntityTypeException as e:
        bad_request_error("Invalid entity type provided: " + entity_type)

    # Handle Collections retrieval using a different endpoint 
    if normalized_entity_type == 'Collection':
        bad_request_error("Please use another API endpoint `/collections` to query collections")

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']
            separator = ', '

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {separator.join(result_filtering_accepted_property_keys)}")
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        # Get user token from Authorization header
        user_token = get_user_token(request.headers)

        # Get back a list of entity dicts for the given entity type
        entities_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        # direct_ancestor for Sample, and direct_ancestors for Dataset
        properties_to_skip = ['direct_ancestors', 'direct_ancestor']
        complete_entities_list = schema_manager.get_complete_entities_list(user_token, entities_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

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
    # Use the internal token to query the target collection 
    # since public collections don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    collection_dict = query_target_entity(id, token)

    # A bit validation 
    if collection_dict['entity_type'] != 'Collection':
        bad_request_error("Target entity of the given id is not a collection")

    try:
        # Get user data_access_level based on token if provided
        # If no Authorization header, default user_info['data_access_level'] == 'public'
        # The user_info contains HIGHEST access level of the user based on the token
        # This call raises an HTTPException with a 401 if any auth issues encountered
        user_info = auth_helper_instance.getUserDataAccessLevel(request)
    # If returns HTTPException with a 401, expired/invalid token
    except HTTPException:
        unauthorized_error("The token is invalid or expired")

    # If the user can only access public collections, modify the collection result
    # by only returning public datasets attached to this collection
    if user_info['data_access_level'] == 'public': 
        # When the requested collection is not public but the user can only access public data
        if ('has_doi' not in collection_dict) or (not collection_dict['has_doi']):
            bad_request_error("The reqeusted collection is not public, please send a Globus token with the right access permission in the request.")

        # Only return the public datasets attached to this collection for Collection.datasets property
        complete_dict = get_complete_public_collection_dict(collection_dict)
    else:
        # There must be a user token in the case when user_info['data_access_level'] != 'public'
        user_token = get_user_token(request.headers)

        # We'll need to return all the properties including those 
        # generated by `on_read_trigger` to have a complete result
        complete_dict = schema_manager.get_complete_entity_result(user_token, collection_dict)

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
    normalized_entity_type = 'Collection'

    try:
        # Get user data_access_level based on token if provided
        # If no Authorization header, default user_info['data_access_level'] == 'public'
        # The user_info contains HIGHEST access level of the user based on the token
        # This call raises an HTTPException with a 401 if any auth issues encountered
        user_info = auth_helper_instance.getUserDataAccessLevel(request)
    # If returns HTTPException with a 401, expired/invalid token
    except HTTPException:
        unauthorized_error("The token is invalid or expired")

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']
            separator = ', '

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {separator.join(result_filtering_accepted_property_keys)}")
            
            if user_info['data_access_level'] == 'public':
                # Only return a list of the filtered property value of each public collection
                property_list = app_neo4j_queries.get_public_collections(neo4j_driver_instance, property_key)
            else:
                # Only return a list of the filtered property value of each entity
                property_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else: 
        token = None

        # If the user can only access public collections, modify the resultss
        # by only returning public datasets attached to each collection
        if user_info['data_access_level'] == 'public': 
            # Use the internal token since no user token is requried to access public collections
            token = get_internal_token()

            # Get back a list of public collections dicts
            collections_list = app_neo4j_queries.get_public_collections(neo4j_driver_instance)
            
            # Modify the Collection.datasets property for each collection dict 
            # to contain only public datasets
            for collection_dict in collections_list:
                # Only return the public datasets attached to this collection for Collection.datasets property
                collection_dict = get_complete_public_collection_dict(collection_dict)
        else:
            # There must be a user token in the case when user_info['data_access_level'] != 'public'
            token = get_user_token(request.headers)

            # Get back a list of all collections dicts
            collections_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = ['datasets']
        complete_collections_list = schema_manager.get_complete_entities_list(token, collections_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_collections_list)

    # Response with the final result
    return jsonify(final_result)


"""
Create an entity of the target type in neo4j

Parameters
----------
entity_type : str
    One of the target entity types (case-insensitive since will be normalized): Dataset, Collection, Sample, but NOT Donor or Collection

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/entities/<entity_type>', methods = ['POST'])
def create_entity(entity_type):
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_type)

    # Validate the normalized_entity_type to make sure it's one of the accepted types
    try:
        schema_manager.validate_normalized_entity_type(normalized_entity_type)
    except schema_errors.InvalidNormalizedEntityTypeException as e:
        bad_request_error(f"Invalid entity type provided: {entity_type}")

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    try:
        schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_type)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        bad_request_error(str(e))

    # Sample and Dataset: additional validation, create entity, after_create_trigger
    # Collection and Donor: create entity
    if normalized_entity_type == 'Sample':
        # A bit more validation for new sample to be linked to existing source entity
        direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
        # Check existence of the direct ancestor (either another Sample or Donor)
        direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)

        # Creating the ids require organ code to be specified for the samples to be created when the 
        # sample's direct ancestor is a Donor.
        # Must be one of the codes from: https://github.com/hubmapconsortium/search-api/blob/test-release/src/search-schema/data/definitions/enums/organ_types.yaml
        if direct_ancestor_dict['entity_type'] == 'Donor':
            if 'organ' not in json_data_dict:
                bad_request_error('The key "organ" with a valid organ code is required since the direct ancestor is a Donor')

        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_type, user_token, json_data_dict)

        # For new sample to be linked to existing direct ancestor
        after_create(normalized_entity_type, user_token, merged_dict)
    elif normalized_entity_type == 'Dataset':    
        # `direct_ancestor_uuids` is required for creating new Dataset
        # Check existence of those direct ancestors
        for direct_ancestor_uuid in json_data_dict['direct_ancestor_uuids']:
            direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)

        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_type, user_token, json_data_dict)

        # Handle direct_ancestor_uuids via `after_create_trigger` methods 
        after_create(normalized_entity_type, user_token, merged_dict)
    else:
        # Generate 'before_create_triiger' data and create the entity details in Neo4j
        merged_dict = create_entity_details(request, normalized_entity_type, user_token, json_data_dict)

    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(user_token, merged_dict)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_entity_result_for_response(complete_dict)

    # Also index the new entity node in elasticsearch via search-api
    reindex_entity(complete_dict['uuid'])

    return jsonify(normalized_complete_dict)


"""
Create multiple samples from the same source entity

Parameters
----------
entity_type : str
    One of the target entity types (case-insensitive since will be normalized): Dataset, Collection, Sample, but NOT Donor or Collection

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/entities/multiple-samples/<count>', methods = ['POST'])
def create_multiple_samples(count):
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Normalize user provided entity_type
    normalized_entity_type = 'Sample'

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    try:
        schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_type)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        bad_request_error(str(e))

    # `direct_ancestor_uuid` is required on create
    # Check existence of the direct ancestor (either another Sample or Donor)
    direct_ancestor_dict = query_target_entity(json_data_dict['direct_ancestor_uuid'], user_token)

    # Creating the ids require organ code to be specified for the samples to be created when the 
    # sample's direct ancestor is a Donor.
    # Must be one of the codes from: https://github.com/hubmapconsortium/search-api/blob/test-release/src/search-schema/data/definitions/enums/organ_types.yaml
    if direct_ancestor_dict['entity_type'] == 'Donor':
        if 'organ' not in json_data_dict:
            bad_request_error('The key "organ" with a valid organ code is required since the direct ancestor is a Donor')

    # Generate 'before_create_triiger' data and create the entity details in Neo4j
    generated_ids_dict_list = create_multiple_samples_details(request, normalized_entity_type, user_token, json_data_dict, count)

    # Also index the each new Sample node in elasticsearch via search-api
    for id_dict in generated_ids_dict_list:
        reindex_entity(id_dict['uuid'])

    return jsonify(generated_ids_dict_list)


"""
Update the properties of a given entity, no Collection stuff

Parameters
----------
entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
json
    All the updated properties of the target entity
"""
@app.route('/entities/<id>', methods = ['PUT'])
def update_entity(id):
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json() 

    # Get target entity and return as a dict if exists
    entity_dict = query_target_entity(id, user_token)

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_dict['entity_type'])

    # Validate request json against the yaml schema
    # Pass in the entity_dict for missing required key check, this is different from creating new entity
    try:
        schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_type, existing_entity_dict = entity_dict)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        bad_request_error(str(e))

    # Sample and Dataset: additional validation, update entity, after_update_trigger
    # Collection and Donor: update entity
    if normalized_entity_type == 'Sample':
        # A bit more validation for new sample to be linked to existing source entity
        has_direct_ancestor_uuid = False
        if ('direct_ancestor_uuid' in json_data_dict) and json_data_dict['direct_ancestor_uuid']:
            has_direct_ancestor_uuid = True

            direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
            # Check existence of the source entity (either another Sample or Donor)
            direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)

        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # For sample to be linked to existing direct ancestor
        if has_direct_ancestor_uuid:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    elif normalized_entity_type == 'Dataset':    
        # A bit more validation if `direct_ancestor_uuids` provided
        has_direct_ancestor_uuids = False
        if ('direct_ancestor_uuids' in json_data_dict) and (json_data_dict['direct_ancestor_uuids']):
            has_direct_ancestor_uuids = True

            # Check existence of those source entities
            for direct_ancestor_uuid in json_data_dict['direct_ancestor_uuids']:
                direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)
        
        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle direct_ancestor_uuids via `after_update_trigger` methods 
        if has_direct_ancestor_uuids:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    else:
        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(user_token, merged_updated_dict)

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
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']
    
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']
            separator = ', '

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {separator.join(result_filtering_accepted_property_keys)}")
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        ancestors_list = app_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        # datasts for Collection, director_ancestor for Sample, and direct_ancestors for Dataset
        properties_to_skip = ['datasets', 'direct_ancestor', 'direct_ancestors']
        complete_entities_list = schema_manager.get_complete_entities_list(user_token, ancestors_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

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
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']
            separator = ', '

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {separator.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_descendants(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        descendants_list = app_neo4j_queries.get_descendants(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        # datasts for Collection, director_ancestor for Sample, and direct_ancestors for Dataset
        properties_to_skip = ['datasets', 'direct_ancestor', 'direct_ancestors']
        complete_entities_list = schema_manager.get_complete_entities_list(user_token, descendants_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

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
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']
 
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']
            separator = ', '

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {separator.join(result_filtering_accepted_property_keys)}")
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_parents(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        parents_list = app_neo4j_queries.get_parents(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        # datasts for Collection, director_ancestor for Sample, and direct_ancestors for Dataset
        properties_to_skip = ['datasets', 'direct_ancestor', 'direct_ancestors']
        complete_entities_list = schema_manager.get_complete_entities_list(user_token, parents_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

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
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Make sure the id exists in uuid-api and 
    # the corresponding entity also exists in neo4j
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']
            separator = ', '

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {separator.join(result_filtering_accepted_property_keys)}")
            
            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_children(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        children_list = app_neo4j_queries.get_children(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        # datasts for Collection, director_ancestor for Sample, and direct_ancestors for Dataset
        properties_to_skip = ['datasets', 'direct_ancestor', 'direct_ancestors']
        complete_entities_list = schema_manager.get_complete_entities_list(user_token, children_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

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
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(collection_uuid, user_token)
    if entity_dict['entity_type'] != 'Collection':
        bad_request_error(f"The UUID provided in URL is not a Collection: {collection_uuid}")

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
        entity_dict = query_target_entity(dataset_uuid, user_token)
        if entity_dict['entity_type'] != 'Dataset':
            bad_request_error(f"The UUID provided in JSON is not a Dataset: {dataset_uuid}")

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
    # Get user token from Authorization header
    user_token = get_user_token(request.headers)

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, user_token)

    # Only for collection
    if entity_dict['entity_type'] != 'Collection':
        bad_request_error("The target entity of the specified id is not a Collection")

    uuid = entity_dict['uuid']

    # URL template
    redirect_url = app.config['COLLECTION_REDIRECT_URL']

    if redirect_url.lower().find('<identifier>') == -1:
        # Log the full stack trace, prepend a line with our message
        msg = "Incorrect configuration value for 'COLLECTION_REDIRECT_URL'"
        logger.exception(msg)
        internal_server_error(msg)

    rep_pattern = re.compile(re.escape('<identifier>'), re.RegexFlag.IGNORECASE)
    redirect_url = rep_pattern.sub(uuid, redirect_url)
    
    return redirect(redirect_url, code = 307)

"""
Get the Globus URL to the given dataset

It will provide a Globus URL to the dataset directory in of three Globus endpoints based on the access
level of the user (public, consortium or protected), public only, of course, if no token is provided.
If a dataset isn't found a 404 will be returned. There is a chance that a 500 can be returned, but not
likely under normal circumstances, only for a misconfigured or failing in some way endpoint. 

If the Auth token is provided but is expired or invalid a 401 is returned. If access to the dataset 
is not allowed for the user (or lack of user) a 403 is returned.

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
# Thd old route for backward compatibility - will be removed later
@app.route('/entities/dataset/globus-url/<id>', methods = ['GET'])
# New route
@app.route('/dataset/globus-url/<id>', methods = ['GET'])
def get_dataset_globus_url(id):
    # Use the internal token to query the target entity 
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    # Then retrieve the allowable data access level (public, protected or consortium)
    # for the dataset and HuBMAP Component ID that the dataset belongs to
    entity_dict = query_target_entity(id, token)
    uuid = entity_dict['uuid']
    normalized_entity_type = entity_dict['entity_type']
    
    # Only for dataset
    if normalized_entity_type != 'Dataset':
        bad_request_error("The target entity of the specified id is not a Dataset")
    
    # If no access level is present on the dataset default to protected
    if not 'data_access_level' in entity_dict or string_helper.isBlank(entity_dict['data_access_level']):
        entity_data_access_level = ACCESS_LEVEL_PROTECTED
    else:
        entity_data_access_level = entity_dict['data_access_level']

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = globus_groups.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']
  
    if not 'group_uuid' in entity_dict or string_helper.isBlank(entity_dict['group_uuid']):
        msg = f"The 'group_uuid' property is not set for dataset with uuid: {uuid}"
        logger.exception(msg)
        internal_server_error(msg)

    group_uuid = entity_dict['group_uuid']

    # Validate the group_uuid
    try:
        schema_manager.validate_entity_group_uuid(group_uuid)
    except schema_errors.NoDataProviderGroupException:
        msg = f"Invalid 'group_uuid': {group_uuid} for dataset with uuid: {uuid}"
        logger.exception(msg)
        internal_server_error(msg)

    group_name = groups_by_id_dict[group_uuid]['displayname']

    try:
        # Get user data_access_level based on token if provided
        # If no Authorization header, default user_info['data_access_level'] == 'public'
        # The user_info contains HIGHEST access level of the user based on the token
        # This call raises an HTTPException with a 401 if any auth issues encountered
        user_info = auth_helper_instance.getUserDataAccessLevel(request)
    # If returns HTTPException with a 401, expired/invalid token
    except HTTPException:
        unauthorized_error("The provided token is invalid or expired")   

    # The user is in the Globus group with full access to thie dataset,
    # so they have protected level access to it
    if ('hmgroupids' in user_info) and (group_uuid in user_info['hmgroupids']):
        user_data_access_level = ACCESS_LEVEL_PROTECTED
    else:
        if not 'data_access_level' in user_info:
            msg = f"Unexpected error, data access level could not be found for user trying to access dataset id: {id}"
            logger.exception(msg)
            return internal_server_error(msg)        
        
        user_data_access_level = user_info['data_access_level'].lower()

    #construct the Globus URL based on the highest level of access that the user has
    #and the level of access allowed for the dataset
    #the first "if" checks to see if the user is a member of the Consortium group
    #that allows all access to this dataset, if so send them to the "protected"
    #endpoint even if the user doesn't have full access to all protected data
    globus_server_uuid = None     
    dir_path = ''
    
    # public access
    if entity_data_access_level == ACCESS_LEVEL_PUBLIC:
        globus_server_uuid = app.config['GLOBUS_PUBLIC_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['PUBLIC_DATA_SUBDIR'])
        dir_path = dir_path +  access_dir + "/"
    # consortium access
    elif (entity_data_access_level == ACCESS_LEVEL_CONSORTIUM) and (not user_data_access_level == ACCESS_LEVEL_PUBLIC):
        globus_server_uuid = app.config['GLOBUS_CONSORTIUM_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['CONSORTIUM_DATA_SUBDIR'])
        dir_path = dir_path + access_dir + group_name + "/"
    # protected access
    elif (entity_data_access_level == ACCESS_LEVEL_PROTECTED) and (user_data_access_level == ACCESS_LEVEL_PROTECTED):
        globus_server_uuid = app.config['GLOBUS_PROTECTED_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['PROTECTED_DATA_SUBDIR'])
        dir_path = dir_path + access_dir + group_name + "/"

    if globus_server_uuid is None:
        forbidden_error("Access not granted")

    dir_path = dir_path + uuid + "/"
    dir_path = urllib.parse.quote(dir_path, safe='')

    #https://app.globus.org/file-manager?origin_id=28bbb03c-a87d-4dd7-a661-7ea2fb6ea631&origin_path=%2FIEC%20Testing%20Group%2F03584b3d0f8b46de1b629f04be156879%2F
    url = hm_file_helper.ensureTrailingSlashURL(app.config['GLOBUS_APP_BASE_URL']) + "file-manager?origin_id=" + globus_server_uuid + "&origin_path=" + dir_path  
            
    return Response(url, 200)

"""
File upload handling for Donor (for now, Sample will need file upload too)

Returns
-------
json
    A JSON containing the temp file uuid
"""
@app.route('/file-upload', methods=['POST'])
def upload_file():
    # Check if the post request has the file part
    if 'file' not in request.files:
        bad_request_error('No file part')

    file = request.files['file']

    if file.filename == '':
        bad_request_error('No selected file')

    try:
        temp_id = file_upload_helper_instance.save_temp_file(file)
        rspn_data = {
            "temp_file_id": temp_id
        }

        return jsonify(rspn_data), 201
    except Exception as e:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to upload files"
        logger.exception(msg)
        internal_server_error(msg)


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
Throws error for 401 Unauthorized with message

Parameters
----------
err_msg : str
    The custom error message to return to end users
"""
def unauthorized_error(err_msg):
    abort(401, description = err_msg)

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
Parase the token from Authorization header

Parameters
----------
request_headers: request.headers
    The http request headers

Returns
-------
str
    The token string if valid
"""
def get_user_token(request_headers):
    # Get user token from Authorization header
    # getAuthorizationTokens() also handles MAuthorization header but we are not using that here
    user_token = auth_helper_instance.getAuthorizationTokens(request_headers) 

    # The user_token is flask.Response on error
    if isinstance(user_token, Response):
        # The Response.data returns binary string, need to decode
        unauthorized_error(user_token.data.decode())

    return user_token

"""
Get the token for internal use only

Returns
-------
str
    The token string 
"""
def get_internal_token():
    return auth_helper_instance.getProcessSecret()


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
    # Use internal token to query entity since 
    # no user token is required to access a public collection
    token = get_internal_token()

    # Collection.datasets is transient property and generated by the trigger method
    # We'll need to return all the properties including those 
    # generated by `on_read_trigger` to have a complete result
    complete_dict = schema_manager.get_complete_entity_result(token, collection_dict)

    # Loop through Collection.datasets and only return the public datasets
    public_datasets = [] 
    for dataset in complete_dict['datasets']:
        if dataset['data_access_level'] == 'public':
            public_datasets.append(dataset)

    # Modify the result and only show the public datasets in this collection
    complete_dict['datasets'] = public_datasets

    return complete_dict


"""
Generate 'before_create_triiger' data and create the entity details in Neo4j

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
user_token: str
    The user's globus nexus token
json_data_dict: dict
    The json request dict from user input

Returns
-------
dict
    A dict of all the newly created entity detials
"""
def create_entity_details(request, normalized_entity_type, user_token, json_data_dict):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Create new ids for the new entity
    try:
        new_ids_dict_list = schema_manager.create_hubmap_ids(normalized_entity_type, json_data_dict, user_token, user_info_dict)
        new_ids_dict = new_ids_dict_list[0]
    # When group_uuid is provided by user, it can be invalid
    except schema_errors.NoDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        if 'group_uuid' in json_data_dict:
            msg = "Invalid 'group_uuid' value, can't create the entity"
        else:
            msg = "The user does not have the correct Globus group associated with, can't create the entity"
        
        logger.exception(msg)
        bad_request_error(msg)
    except schema_errors.UnmatchedDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        bad_request_error(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        bad_request_error(msg)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        bad_request_error(e)

    # Merge all the above dictionaries and pass to the trigger methods
    new_data_dict = {**json_data_dict, **user_info_dict, **new_ids_dict}

    try:
        # Use {} since no existing dict
        generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data('before_create_trigger', normalized_entity_type, user_token, {}, new_data_dict)
    # If one of the before_create_trigger methods fails, we can't create the entity
    except schema_errors.BeforeCreateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_create_trigger' methods, can't create the entity"
        logger.exception(msg)
        internal_server_error(msg)
    except schema_errors.NoDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        if 'group_uuid' in json_data_dict:
            msg = "Invalid 'group_uuid' value, can't create the entity"
        else:
            msg = "The user does not have the correct Globus group associated with, can't create the entity"
        
        logger.exception(msg)
        bad_request_error(msg)
    except schema_errors.UnmatchedDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        bad_request_error(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        bad_request_error(msg)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        bad_request_error(e)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_before_create_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the properties with None value
    # Meaning the returned target property key is different from the original key
    # E.g., Donor.image_files
    filtered_merged_dict = {}
    for k, v in merged_dict.items():
        if v is not None:
            filtered_merged_dict[k] = v

    # Create new entity
    try:
        entity_dict = app_neo4j_queries.create_entity(neo4j_driver_instance, normalized_entity_type, filtered_merged_dict)
    except TransactionError:
        msg = "Failed to create the new " + normalized_entity_type
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

    # Add user_info_dict because it may be used by after_update_trigger methods
    merged_final_dict = {**filtered_merged_dict, **user_info_dict}

    # Note: return merged_final_dict instead of entity_dict because 
    # it contains all the user json data that the generated that entity_dict may not have
    return merged_final_dict


"""
Create multiple sample nodes and relationships with the source entity node

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
user_token: str
    The user's globus nexus token
json_data_dict: dict
    The json request dict from user input
count : int
    The number of samples to create

Returns
-------
list
    A list of all the newly generated ids via uuid-api
"""
def create_multiple_samples_details(request, normalized_entity_type, user_token, json_data_dict, count):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Create new ids for the new entity
    try:
        new_ids_dict_list = schema_manager.create_hubmap_ids(normalized_entity_type, json_data_dict, user_token, user_info_dict, count)
    # When group_uuid is provided by user, it can be invalid
    except schema_errors.NoDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        if 'group_uuid' in json_data_dict:
            msg = "Invalid 'group_uuid' value, can't create the entity"
        else:
            msg = "The user does not have the correct Globus group associated with, can't create the entity"
        
        logger.exception(msg)
        bad_request_error(msg)
    except schema_errors.UnmatchedDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        bad_request_error(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        bad_request_error(msg)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        bad_request_error(e)

    logger.debug("new_ids_dict_list for multiple samples")
    logger.debug(new_ids_dict_list)

    # Use the same json_data_dict and user_info_dict for each sample to be created
    # Only difference is the hubmap_ids generated
    samples_dict_list = []
    for new_ids_dict in new_ids_dict_list:
        # Merge all the dictionaries and pass to the trigger methods
        new_data_dict = {**json_data_dict, **user_info_dict, **new_ids_dict}

        try:
            # Use {} since no existing dict
            generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data('before_create_trigger', normalized_entity_type, user_token, {}, new_data_dict)
        # If one of the before_create_trigger methods fails, we can't create the entity
        except schema_errors.BeforeCreateTriggerException:
            # Log the full stack trace, prepend a line with our message
            msg = "Failed to execute one of the 'before_create_trigger' methods, can't create the entity"
            logger.exception(msg)
            internal_server_error(msg)
        except schema_errors.NoDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            if 'group_uuid' in json_data_dict:
                msg = "Invalid 'group_uuid' value, can't create the entity"
            else:
                msg = "The user does not have the correct Globus group associated with, can't create the entity"
            
            logger.exception(msg)
            bad_request_error(msg)
        except schema_errors.UnmatchedDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            msg = "The user does not belong to the given Globus group, can't create the entity"
            logger.exception(msg)
            bad_request_error(msg)
        except schema_errors.MultipleDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
            logger.exception(msg)
            bad_request_error(msg)
        except KeyError as e:
            # Log the full stack trace, prepend a line with our message
            logger.exception(e)
            bad_request_error(e)

        # Merge the user json data and generated trigger data into one dictionary
        merged_dict = {**json_data_dict, **generated_before_create_trigger_data_dict}

        # Filter out the merged_dict by getting rid of the properties with None value
        # Meaning the returned target property key is different from the original key
        # E.g., Donor.image_files
        filtered_merged_dict = {}
        for k, v in merged_dict.items():
            if v is not None:
                filtered_merged_dict[k] = v

        # Add to the list
        samples_dict_list.append(filtered_merged_dict)

    # Generate property values for Activity node
    activity_data_dict = schema_manager.generate_activity_data(normalized_entity_type, user_token, user_info_dict)
    
    # Create new sample nodes and needed relationships as well as activity node in one transaction
    try:
        # No return value
        app_neo4j_queries.create_multiple_samples(neo4j_driver_instance, samples_dict_list, activity_data_dict, json_data_dict['direct_ancestor_uuid'])
    except TransactionError:
        msg = "Failed to create multiple samples"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

    # Return the generated ids for UI
    return new_ids_dict_list


"""
Execute 'after_create_triiger' methods

Parameters
----------
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
user_token: str
    The user's globus nexus token
merged_data_dict: dict
    The merged dict that contains the entity dict newly created and 
    information from user request json that are not stored in Neo4j
"""
def after_create(normalized_entity_type, user_token, merged_data_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        # Use {} since no new dict
        schema_manager.generate_triggered_data('after_create_trigger', normalized_entity_type, user_token, merged_data_dict, {})
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
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
user_token: str
    The user's globus nexus token
json_data_dict: dict
    The json request dict
existing_entity_dict: dict
    Dict of the exiting entity information

Returns
-------
dict
    A dict of all the updated entity detials
"""
def update_entity_details(request, normalized_entity_type, user_token, json_data_dict, existing_entity_dict):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)

    # Merge user_info_dict and the json_data_dict for passing to the trigger methods
    new_data_dict = {**user_info_dict, **json_data_dict}

    try:
        generated_before_update_trigger_data_dict = schema_manager.generate_triggered_data('before_update_trigger', normalized_entity_type, user_token, existing_entity_dict, new_data_dict)
    # If one of the before_update_trigger methods fails, we can't update the entity
    except schema_errors.BeforeUpdateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_update_trigger' methods, can't update the entity"
        logger.exception(msg)
        internal_server_error(msg)

    # Merge dictionaries
    merged_dict = {**json_data_dict, **generated_before_update_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the properties with None value
    # Meaning the returned target property key is different from the original key
    # E.g., Donor.image_files
    filtered_merged_dict = {}
    for k, v in merged_dict.items():
        if v is not None:
            filtered_merged_dict[k] = v

    # By now the filtered_merged_dict contains all user updates and all triggered data to be added to the entity node
    # Any properties in filtered_merged_dict that are not on the node will be added.
    # Any properties not in filtered_merged_dict that are on the node will be left as is.
    # Any properties that are in both filtered_merged_dict and the node will be replaced in the node. However, if any property in the map is null, it will be removed from the node.

    # Update the exisiting entity
    try:
        updated_entity_dict = app_neo4j_queries.update_entity(neo4j_driver_instance, normalized_entity_type, filtered_merged_dict, existing_entity_dict['uuid'])
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
normalized_entity_type : str
    One of the normalized entity types: Dataset, Collection, Sample, Donor
user_token: str
    The user's globus nexus token
entity_dict: dict
    The entity dict newly updated
"""
def after_update(normalized_entity_type, user_token, entity_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        # Use {} sicne no new dict
        schema_manager.generate_triggered_data('after_update_trigger', normalized_entity_type, user_token, entity_dict, {})
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
user_token: str
    The user's globus nexus token from the incoming request

Returns
-------
dict
    A dictionary of entity details returned from neo4j
"""
def query_target_entity(id, user_token):
    try:
        """
        The dict returned by uuid-api that contains all the associated ids, e.g.:
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
        hubmap_ids = schema_manager.get_hubmap_ids(id, user_token)

        # Get the target uuid if all good
        uuid = hubmap_ids['hm_uuid']
        entity_dict = app_neo4j_queries.get_entity(neo4j_driver_instance, uuid)

        # The uuid exists via uuid-api doesn't mean it's also in Neo4j
        if not entity_dict:
            not_found_error(f"Entity of id: {id} not found in Neo4j")

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
uuid : str
    The uuid of the target entity
"""
def reindex_entity(uuid):
    try:
        response = requests.put(app.config['SEARCH_API_URL'] + "/reindex/" + uuid)
        # The reindex takes time, so 202 Accepted response status code indicates that 
        # the request has been accepted for processing, but the processing has not been completed
        if response.status_code == 202:
            logger.info(f"The search-api has accepted the reindex request for uuid: {uuid}")
        else:
            logger.error(f"The search-api failed to initialize the reindex for uuid: {uuid}")
    except Exception:
        msg = f"Failed to send the reindex request to search-api for entity with uuid: {uuid}"
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
    
    return hm_file_helper.ensureTrailingSlashURL(hm_file_helper.ensureBeginningSlashURL(dir_name))


# For local development/testing
if __name__ == "__main__":
    try:
        app.run(host='0.0.0.0', port="5002")
    except Exception as e:
        print("Error during starting debug server.")
        print(str(e))
        logger.error(e, exc_info=True)
        print("Error during startup check the log file for further information")