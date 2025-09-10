import sys
import collections
from typing import Callable, List, Optional, Annotated
from datetime import datetime
from flask import Flask, g, jsonify, abort, request, Response, redirect, make_response
from neo4j.exceptions import TransactionError
import os
import re
import csv
import requests
from requests.adapters import HTTPAdapter, Retry
import threading
import urllib.request
from io import StringIO
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from pathlib import Path
import logging
import json
import time

# pymemcache.client.base.PooledClient is a thread-safe client pool 
# that provides the same API as pymemcache.client.base.Client
from pymemcache.client.base import PooledClient
from pymemcache import serde

# Local modules
import app_neo4j_queries
import provenance
from schema import schema_manager
from schema import schema_errors
from schema import schema_triggers
from schema import schema_validators
from schema import schema_neo4j_queries
from schema.schema_constants import SchemaConstants
from schema.schema_constants import DataVisibilityEnum
from schema.schema_constants import MetadataScopeEnum
from schema.schema_constants import TriggerTypeEnum
from metadata_constraints import get_constraints, constraints_json_is_valid
# from lib.ontology import initialize_ubkg, init_ontology, Ontology, UbkgSDK

# HuBMAP commons
from hubmap_commons import string_helper
from hubmap_commons import file_helper as hm_file_helper
from hubmap_commons import neo4j_driver
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.exceptions import HTTPException
from hubmap_commons.S3_worker import S3Worker

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config = True)
app.config.from_pyfile('app.cfg')

# Root logger configuration
global logger

# Set logging format and level
if app.config['DEBUG_MODE']:
    logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')
else:
    logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

# Use `getLogger()` instead of `getLogger(__name__)` to apply the config to the root logger
# will be inherited by the sub-module loggers
logger = logging.getLogger()

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['UUID_API_URL'] = app.config['UUID_API_URL'].strip('/')
app.config['INGEST_API_URL'] = app.config['INGEST_API_URL'].strip('/')
app.config['ONTOLOGY_API_URL'] = app.config['ONTOLOGY_API_URL'].strip('/')
app.config['ENTITY_API_URL'] = app.config['ENTITY_API_URL'].strip('/')
app.config['SEARCH_API_URL'] = app.config['SEARCH_API_URL'].strip('/')

S3_settings_dict = {'large_response_threshold': app.config['LARGE_RESPONSE_THRESHOLD']
                    , 'aws_access_key_id': app.config['AWS_ACCESS_KEY_ID']
                    , 'aws_secret_access_key': app.config['AWS_SECRET_ACCESS_KEY']
                    , 'aws_s3_bucket_name': app.config['AWS_S3_BUCKET_NAME']
                    , 'aws_object_url_expiration_in_secs': app.config['AWS_OBJECT_URL_EXPIRATION_IN_SECS']
                    , 'service_configured_obj_prefix': app.config['AWS_S3_OBJECT_PREFIX']}

# This mode when set True disables the PUT and POST calls, used on STAGE to make entity-api READ-ONLY 
# to prevent developers from creating new UUIDs and new entities or updating existing entities
READ_ONLY_MODE = app.config['READ_ONLY_MODE']

# Whether Memcached is being used or not
# Default to false if the property is missing in the configuration file

if 'MEMCACHED_MODE' in app.config:
    MEMCACHED_MODE = app.config['MEMCACHED_MODE']
    # Use prefix to distinguish the cached data of same source across different deployments
    MEMCACHED_PREFIX = app.config['MEMCACHED_PREFIX']
else:
    MEMCACHED_MODE = False
    MEMCACHED_PREFIX = 'NONE'

# Read the secret key which may be submitted in HTTP Request Headers to override the lockout of
# updates to entities with characteristics prohibiting their modification.
LOCKED_ENTITY_UPDATE_OVERRIDE_KEY = app.config['LOCKED_ENTITY_UPDATE_OVERRIDE_KEY']

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)


####################################################################################################
## Register error handlers
####################################################################################################

# Error handler for 400 Bad Request with custom error message
@app.errorhandler(400)
def http_bad_request(e):
    return jsonify(error = str(e)), 400


# Error handler for 401 Unauthorized with custom error message
@app.errorhandler(401)
def http_unauthorized(e):
    return jsonify(error = str(e)), 401


# Error handler for 403 Forbidden with custom error message
@app.errorhandler(403)
def http_forbidden(e):
    return jsonify(error = str(e)), 403


# Error handler for 404 Not Found with custom error message
@app.errorhandler(404)
def http_not_found(e):
    return jsonify(error = str(e)), 404


# Error handler for 500 Internal Server Error with custom error message
@app.errorhandler(500)
def http_internal_server_error(e):
    return jsonify(error = str(e)), 500

####################################################################################################
## AuthHelper initialization
####################################################################################################

# Initialize AuthHelper class and ensure singleton
try:
    if AuthHelper.isInitialized() == False:
        auth_helper_instance = AuthHelper.create(app.config['APP_CLIENT_ID'],
                                                 app.config['APP_CLIENT_SECRET'])

        logger.info('Initialized auth_helper_instance successfully :)')
    else:
        auth_helper_instance = AuthHelper.instance()
except Exception:
    msg = 'Failed to initialize the auth_helper_instance :('
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## Neo4j connection initialization
####################################################################################################

# The neo4j_driver (from commons package) is a singleton module
# This neo4j_driver_instance will be used for application-specific neo4j queries
# as well as being passed to the schema_manager
try:
    neo4j_driver_instance = neo4j_driver.instance(app.config['NEO4J_URI'],
                                                  app.config['NEO4J_USERNAME'],
                                                  app.config['NEO4J_PASSWORD'])
    logger.info('Initialized neo4j_driver_instance successfully :)')
except Exception:
    msg = 'Failed to initialize the neo4j_driver_instance :('
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## Memcached client initialization
####################################################################################################

memcached_client_instance = None

if MEMCACHED_MODE:
    try:
        # Use client pool to maintain a pool of already-connected clients for improved performance
        # The uwsgi config launches the app across multiple threads (16) inside each process (16), making essentially 256 processes
        # Set the connect_timeout and timeout to avoid blocking the process when memcached is slow, defaults to "forever"
        # connect_timeout: seconds to wait for a connection to the memcached server
        # timeout: seconds to wait for send or reveive calls on the socket connected to memcached
        # Use the ignore_exc flag to treat memcache/network errors as cache misses on calls to the get* methods
        # Set the no_delay flag to sent TCP_NODELAY (disable Nagle's algorithm to improve TCP/IP networks and decrease the number of packets)
        # If you intend to use anything but str as a value, it is a good idea to use a serializer
        memcached_client_instance = PooledClient(app.config['MEMCACHED_SERVER'], 
                                                 max_pool_size = 256,
                                                 connect_timeout = 1,
                                                 timeout = 30,
                                                 ignore_exc = True, 
                                                 no_delay = True,
                                                 serde = serde.pickle_serde)

        # memcached_client_instance can be instantiated without connecting to the Memcached server
        # A version() call will throw error (e.g., timeout) when failed to connect to server
        # Need to convert the version in bytes to string
        logger.info('Initialized memcached_client_instance successfully :)')
    except Exception:
        msg = 'Failed to initialize memcached_client_instance :('
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        # Turn off the caching
        MEMCACHED_MODE = False


####################################################################################################
## Schema initialization
####################################################################################################

try:
    try:
        _schema_yaml_file = app.config['SCHEMA_YAML_FILE']
    except KeyError as ke:
        logger.error("Expected configuration failed to load %s from app_config=%s.", ke, app.config)
        raise Exception("Expected configuration failed to load. See the logs.")

    # The schema_manager is a singleton module
    # Pass in auth_helper_instance, neo4j_driver instance, and memcached_client_instance
    schema_manager.initialize(_schema_yaml_file,
                              app.config['UUID_API_URL'],
                              app.config['INGEST_API_URL'],
                              app.config['ONTOLOGY_API_URL'],
                              app.config['ENTITY_API_URL'],
                              auth_helper_instance,
                              neo4j_driver_instance,
                              memcached_client_instance,
                              MEMCACHED_PREFIX)

    logger.info('Initialized schema_manager successfully :)')
except Exception:
    msg =   f"Failed to initialize the schema_manager with" \
            f" _schema_yaml_file={_schema_yaml_file}."
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## Initialize an S3Worker from hubmap-commons
####################################################################################################

try:
    anS3Worker = S3Worker(ACCESS_KEY_ID=S3_settings_dict['aws_access_key_id']
                          , SECRET_ACCESS_KEY=S3_settings_dict['aws_secret_access_key']
                          , S3_BUCKET_NAME=S3_settings_dict['aws_s3_bucket_name']
                          , S3_OBJECT_URL_EXPIRATION_IN_SECS=S3_settings_dict['aws_object_url_expiration_in_secs']
                          , LARGE_RESPONSE_THRESHOLD=S3_settings_dict['large_response_threshold']
                          , SERVICE_S3_OBJ_PREFIX=S3_settings_dict['service_configured_obj_prefix'])
    logger.info('Initialized anS3Worker successfully :)')
except Exception:
    msg = 'Failed to initialize anS3Worker :('
    # Log the full stack trace, prepend a line with our message
    logger.exception(msg)


####################################################################################################
## REFERENCE DOI Redirection
####################################################################################################

## Read tsv file with the REFERENCE entity redirects
## sets the reference_redirects dict which is used
## by the /redirect method below
try:
    reference_redirects = {}
    url = app.config['REDIRECTION_INFO_URL']
    # Use Memcached to improve performance
    response = schema_manager.make_request_get(url)
    resp_txt = response.content.decode('utf-8')
    cr = csv.reader(resp_txt.splitlines(), delimiter='\t')

    first = True
    id_column = None
    redir_url_column = None
    for row in cr:
        if first:
            first = False
            header = row
            column = 0
            for label in header:
                if label == 'hubmap_id': id_column = column
                if label == 'data_information_page': redir_url_column = column
                column = column + 1
            if id_column is None: raise Exception(f"Column hubmap_id not found in {url}")
            if redir_url_column is None: raise Exception (f"Column data_information_page not found in {url}")
        else:
            reference_redirects[row[id_column].upper().strip()] = row[redir_url_column]
    rr = redirect('abc', code = 307)
    print(rr)
except Exception:
    logger.exception("Failed to read tsv file with REFERENCE redirect information")


####################################################################################################
## Constants
####################################################################################################

# For now, don't use the constants from commons
# All lowercase for easy comparision
#
# Places where these constants are used should be evaluated for refactoring to directly reference the
# constants in SchemaConstants.  Constants defined here should be evaluated to move to SchemaConstants.
# All this should be done when the endpoints with changed code can be verified with solid tests.
ACCESS_LEVEL_PUBLIC = SchemaConstants.ACCESS_LEVEL_PUBLIC
ACCESS_LEVEL_CONSORTIUM = SchemaConstants.ACCESS_LEVEL_CONSORTIUM
ACCESS_LEVEL_PROTECTED = SchemaConstants.ACCESS_LEVEL_PROTECTED
DATASET_STATUS_PUBLISHED = SchemaConstants.DATASET_STATUS_PUBLISHED
COMMA_SEPARATOR = ','


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
Show status of Neo4j connection and Memcached connection (if enabled) with the current VERSION and BUILD

Returns
-------
json
    A json containing the status details
"""
@app.route('/status', methods = ['GET'])
def get_status():

    try:
        file_version_content = (Path(__file__).absolute().parent.parent / 'VERSION').read_text().strip()
    except Exception as e:
        file_version_content = str(e)

    try:
        file_build_content = (Path(__file__).absolute().parent.parent / 'BUILD').read_text().strip()
    except Exception as e:
        file_build_content = str(e)

    status_data = {
        # Use strip() to remove leading and trailing spaces, newlines, and tabs
        'version': file_version_content,
        'build': file_build_content,
        'neo4j_connection': False
    }

    # Don't use try/except here
    is_neo4j_connected = app_neo4j_queries.check_connection(neo4j_driver_instance)

    if is_neo4j_connected:
        status_data['neo4j_connection'] = True

    # Only show the Memcached connection status when the caching is enabled
    if MEMCACHED_MODE:
        status_data['memcached_connection'] = False

        try:
            # If can't connect, won't be able to get the Memcached version
            memcached_client_instance.version()
            logger.info(f'Connected to Memcached server {memcached_client_instance.version().decode()} :)')
            status_data['memcached_connection'] = True
        except Exception:
            logger.error('Failed to connect to Memcached server :(')

    return jsonify(status_data)


"""
Currently for debugging purpose 
Essentially does the same as ingest-api's `/metadata/usergroups` using the deprecated commons method
Globus groups token is required by AWS API Gateway lambda authorizer

Returns
-------
json
    A json list of globus groups this user belongs to
"""
@app.route('/usergroups', methods = ['GET'])
def get_user_groups():
    token = get_user_token(request)
    groups_list = auth_helper_instance.get_user_groups_deprecated(token)
    return jsonify(groups_list)


"""
Delete ALL the following cached data from Memcached, Data-Admin access is required in AWS API Gateway:
    - cached individual entity dict
    - cached IDs dict from uuid-api
    - cached yaml content from github raw URLs
    - cached TSV file content for reference DOIs redirect

Returns
-------
str
    A confirmation message
"""
@app.route('/flush-all-cache', methods = ['DELETE'])
def flush_all_cache():
    msg = ''

    if MEMCACHED_MODE:
        memcached_client_instance.flush_all()
        msg = 'All cached data (entities, IDs, yamls, tsv) has been deleted from Memcached'
    else:
        msg = 'No caching is being used because Memcached mode is not enabled at all'

    return msg


"""
Delete the cached data from Memcached for a given entity, HuBMAP-Read access is required in AWS API Gateway

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity (Donor/Dataset/Sample/Upload/Collection/Publication)

Returns
-------
str
    A confirmation message
"""
@app.route('/flush-cache/<id>', methods = ['DELETE'])
def flush_cache(id):
    msg = ''

    if MEMCACHED_MODE:
        entity_dict = query_target_entity(id, get_internal_token())
        delete_cache(entity_dict['uuid'], entity_dict['entity_type'])
        msg = f'The cached data has been deleted from Memcached for entity {id}'
    else:
        msg = 'No caching is being used because Memcached mode is not enabled at all'

    return msg


"""
Retrieve the ancestor organ(s) of a given entity

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity (Dataset/Sample)

Returns
-------
json
    List of organs that are ancestors of the given entity
    - Only dataset entities can return multiple ancestor organs
      as Samples can only have one parent.
    - If no organ ancestors are found an empty list is returned
    - If requesting the ancestor organ of a Sample of type Organ or Donor/Collection/Upload
      a 400 response is returned.
"""
@app.route('/entities/<id>/ancestor-organs', methods = ['GET'])
def get_ancestor_organs(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # A bit validation
    if normalized_entity_type not in ['Sample'] and \
            not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error(f"Unable to get the ancestor organs for this: {normalized_entity_type},"
                          " supported entity types: Sample, Dataset, Publication")

    if normalized_entity_type == 'Sample' and entity_dict['sample_category'].lower() == 'organ':
        bad_request_error("Unable to get the ancestor organ of an organ.")
    public_entity = True
    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            public_entity = False
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required = True)
    else:
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            public_entity = False
            token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or the user token has the correct access level
    organs = app_neo4j_queries.get_ancestor_organs(neo4j_driver_instance, entity_dict['uuid'])
    excluded_fields = schema_manager.get_fields_to_exclude('Sample')      

    # Skip executing the trigger method to get Sample.direct_ancestor
    properties_to_skip = ['direct_ancestor']
    complete_entities_list = schema_manager.get_complete_entities_list(token, organs, properties_to_skip)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
    if public_entity and not user_in_hubmap_read_group(request):
        filtered_organs_list = []
        for organ in final_result:
            filtered_organs_list.append(schema_manager.exclude_properties_from_response(excluded_fields, organ))
        final_result = filtered_organs_list
    
    return jsonify(final_result)


"""
Check if the given entity is a specific type

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity (Dataset/Sample)
type : str
    One of the valid entity types

Returns
-------
bool
"""
@app.route('/entities/<id>/instanceof/<type>', methods=['GET'])
def get_entities_instanceof(id, type):
    try:
        uuid = schema_manager.get_hubmap_ids(id.strip())['uuid']
        instanceof: bool = schema_manager.entity_instanceof(uuid, type)
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code
        if status_code == 400:
            bad_request_error(e.response.text)
        if status_code == 404:
            not_found_error(e.response.text)
        else:
            internal_server_error(e.response.text)
    except:
        bad_request_error("Unable to process request")
    
    return make_response(jsonify({'instanceof': instanceof}), 200)


"""
Check if the given entity type A is an instance of the given type B

Parameters
----------
type_a : str
    The given entity type A
type_a : str
    The given entity type B

Returns
-------
bool
"""
@app.route('/entities/type/<type_a>/instanceof/<type_b>', methods=['GET'])
def get_entities_type_instanceof(type_a, type_b):
    try:
        instanceof: bool = schema_manager.entity_type_instanceof(type_a, type_b)
    except:
        bad_request_error("Unable to process request")
    
    return make_response(jsonify({'instanceof': instanceof}), 200)


"""
Endpoint which sends the "visibility" of an entity using values from DataVisibilityEnum.

Not exposed through the gateway.  Used by services like search-api to, for example, determine if
a Collection can be in a public index while encapsulating the logic to determine that in this service.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target collection 

Returns
-------
json
    A value from DataVisibilityEnum
"""
@app.route('/visibility/<id>', methods = ['GET'])
def get_entity_visibility(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Get the generated complete entity result from cache if exists
    # Otherwise re-generate on the fly.  To verify if a Collection is public, it is
    # necessary to have its Datasets, which are populated as triggered data, so
    # pull back the complete entity
    complete_dict = schema_manager.get_complete_entity_result(token, entity_dict)

    # Determine if the entity is publicly visible base on its data, only.
    entity_scope = _get_entity_visibility(normalized_entity_type=normalized_entity_type, entity_dict=complete_dict)

    return jsonify(entity_scope.value)


"""
Retrieve the full provenance metadata information of a given entity by id, as
produced for metadata.json files.

This endpoint as publicly accessible.  Without presenting a token, only data for
published Datasets may be requested.

When a valid token is presented, a member of the HuBMAP-Read Globus group is authorized to
access any Dataset.  Otherwise, only access to published Datasets is authorized.

An HTTP 400 Response is returned for reasons described in the error message, such as
requesting data for a non-Dataset.

An HTTP 401 Response is returned when a token is presented that is not valid.

An HTTP 403 Response is returned if user is not authorized to access the Dataset, as described above.

An HTTP 404 Response is returned if the requested Dataset is not found.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
json
    Valid JSON for the full provenance metadata of the requested Dataset
"""
@app.route('/datasets/<id>/prov-metadata', methods=['GET'])
def get_provenance_metadata_by_id_for_auth_level(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # The argument id that shadows Python's built-in id should be an identifier for a Dataset.
    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    dataset_dict = query_target_entity(id, token)
    normalized_entity_type = dataset_dict['entity_type']

    # A bit validation
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error(f"Unable to get the provenance metatdata for this: {normalized_entity_type},"
                          " supported entity types: Dataset, Publication")

    # Get the generated complete entity result from cache if exists
    # Otherwise re-generate on the fly
    complete_dict = schema_manager.get_complete_entity_result(token=token
                                                              , entity_dict=dataset_dict)

    # Determine if the entity is publicly visible base on its data, only.
    # To verify if a Collection is public, it is necessary to have its Datasets, which
    # are populated as triggered data.  So pull back the complete entity for
    # _get_entity_visibility() to check.
    entity_scope = _get_entity_visibility(  normalized_entity_type=normalized_entity_type
                                            ,entity_dict=complete_dict)
    public_entity = (entity_scope is DataVisibilityEnum.PUBLIC)

    # Set a variable reflecting the user's authorization by being in the HuBMAP-READ Globus Group
    user_authorized = user_in_hubmap_read_group(request=request)

    # Get user token from Authorization header
    user_token = get_user_token(request)

    # For non-public documents, reject the request if the user is not authorized
    if not public_entity:
        if user_token is None:
            forbidden_error(    f"{normalized_entity_type} for {complete_dict['uuid']} is not"
                                f" accessible without presenting a token.")
        if not user_authorized:
            forbidden_error(    f"The requested {normalized_entity_type} has non-public data."
                                f"  A Globus token with access permission is required.")

    # We'll need to return all the properties including those generated by
    # `on_read_trigger` to have a complete result e.g., the 'next_revision_uuid' and
    # 'previous_revision_uuid' being used below.
    # Collections, however, will filter out only public properties for return.

    # Also normalize the result based on schema
    final_result = schema_manager.normalize_entity_result_for_response(complete_dict)

    # Identify fields to exclude from non-authorized responses for the entity type.
    fields_to_exclude = schema_manager.get_fields_to_exclude(normalized_entity_type)

    # Remove fields which do not belong in provenance metadata, regardless of
    # entity scope or user authorization.
    final_result = schema_manager.exclude_properties_from_response(fields_to_exclude, final_result)

    # Retrieve the associated data for the entity, and add it to the expanded dictionary.
    associated_organ_list = _get_dataset_associated_metadata(   dataset_dict=final_result
                                                                , dataset_visibility=entity_scope
                                                                , valid_user_token=user_token
                                                                , request=request
                                                                , associated_data='Organs')
    final_result['organs'] = associated_organ_list

    associated_sample_list = _get_dataset_associated_metadata(   dataset_dict=final_result
                                                                , dataset_visibility=entity_scope
                                                                , valid_user_token=user_token
                                                                , request=request
                                                                , associated_data='Samples')
    final_result['samples'] = associated_sample_list

    associated_donor_list = _get_dataset_associated_metadata(   dataset_dict=final_result
                                                                , dataset_visibility=entity_scope
                                                                , valid_user_token=user_token
                                                                , request=request
                                                                , associated_data='Donors')

    final_result['donors'] = associated_donor_list

    # Return JSON for the dictionary containing the entity metadata as well as metadata for the associated data.
    return jsonify(final_result)


"""
Retrieve the metadata information of a given entity by id

The gateway treats this endpoint as public accessible

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
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    fields_to_exclude = schema_manager.get_fields_to_exclude(normalized_entity_type)

    # Get the generated complete entity result from cache if exists
    # Otherwise re-generate on the fly
    complete_dict = schema_manager.get_complete_entity_result(token, entity_dict)

    # Determine if the entity is publicly visible base on its data, only.
    # To verify if a Collection is public, it is necessary to have its Datasets, which
    # are populated as triggered data.  So pull back the complete entity for
    # _get_entity_visibility() to check.
    entity_scope = _get_entity_visibility(normalized_entity_type=normalized_entity_type, entity_dict=complete_dict)
    public_entity = False
    # Initialize the user as authorized if the data is public.  Otherwise, the
    # user is not authorized and credentials must be checked.
    if entity_scope == DataVisibilityEnum.PUBLIC:
        user_authorized = True
        public_entity = True
    else:
        # It's highly possible that there's no token provided
        user_token = get_user_token(request)

        # The user_token is flask.Response on error
        # Without token, the user can only access public collections, modify the collection result
        # by only returning public datasets attached to this collection
        if isinstance(user_token, Response):
            forbidden_error(f"{normalized_entity_type} for {id} is not accessible without presenting a token.")
        else:
            # When the groups token is valid, but the user doesn't belong to HuBMAP-READ group
            # Or the token is valid but doesn't contain group information (auth token or transfer token)
            user_authorized = user_in_hubmap_read_group(request)

    # We'll need to return all the properties including those generated by
    # `on_read_trigger` to have a complete result e.g., the 'next_revision_uuid' and
    # 'previous_revision_uuid' being used below.
    # Collections, however, will filter out only public properties for return.
    if not user_authorized:
        forbidden_error(f"The requested {normalized_entity_type} has non-public data."
                        f"  A Globus token with access permission is required.")

    # Also normalize the result based on schema
    final_result = schema_manager.normalize_entity_result_for_response(complete_dict)

    # Result filtering based on query string
    # The `data_access_level` property is available in all entities Donor/Sample/Dataset
    # and this filter is being used by gateway to check the data_access_level for file assets
    # The `status` property is only available in Dataset and being used by search-api for revision
    result_filtering_accepted_property_keys = ['data_access_level', 'status']

    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            if property_key == 'status' and \
                    not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
                bad_request_error(f"Only Dataset or Publication supports 'status' property key in the query string")

            # Response with the property value directly
            # Don't use jsonify() on string value
            return complete_dict[property_key]
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    else:
        # Response with the dict
        if public_entity and not user_in_hubmap_read_group(request):
            final_result = schema_manager.exclude_properties_from_response(fields_to_exclude, final_result)
        return jsonify(final_result)


"""
Retrieve the JSON containing the metadata information for a given entity which is to go into an
OpenSearch document for the entity. Note this is a subset of the "complete" entity metadata returned by the
`GET /entities/<id>` endpoint, with information design and coding design to perform reasonably for
large volumes of indexing.

The gateway treats this endpoint as public accessible.

Result filtering is supported based on query string
For example: /documents/<id>?property=data_access_level

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
json
    Metadata for the entity appropriate for an OpenSearch document, and filtered by an additional
    `property` arguments in the HTTP request.
"""
@app.route('/documents/<id>', methods = ['GET'])
def get_document_by_id(id):

    result_dict = _get_metadata_by_id(entity_id=id, metadata_scope=MetadataScopeEnum.INDEX)
    return jsonify(result_dict)


"""
Retrive the full tree above the referenced entity and build the provenance document

The gateway treats this endpoint as public accessible

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
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    uuid = entity_dict['uuid']
    normalized_entity_type = entity_dict['entity_type']

    # A bit validation to prevent Lab or Collection being queried
    if normalized_entity_type not in ['Donor', 'Sample'] and \
            not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error(f"Unable to get the provenance for this {normalized_entity_type},"
                          " supported entity types: Donor, Sample, Dataset, Publication")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required = True)
    else:
        # The `data_access_level` of Donor/Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Will just proceed to get the provenance information
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
        'nodes': raw_provenance_dict['nodes']
    }

    provenance_json = provenance.get_provenance_history(uuid, normalized_provenance_dict, auth_helper_instance)

    # Response with the provenance details
    return Response(response = provenance_json, mimetype = "application/json")


"""
Show all the supported entity types

The gateway treats this endpoint as public accessible

Returns
-------
json
    A list of all the available entity types defined in the schema yaml
"""
@app.route('/entity-types', methods = ['GET'])
def get_entity_types():
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    return jsonify(schema_manager.get_all_entity_types())


"""
Retrieve all the entity nodes for a given entity type
Result filtering is supported based on query string
For example: /<entity_type>/entities?property=uuid

NOTE: this endpoint is NOT exposed via AWS API Gateway due to performance consideration
It's only used by search-api with making internal calls during index/reindex time bypassing AWS API Gateway

Parameters
----------
entity_type : str
    One of the supported entity types: Dataset, Collection, Sample, Donor

Returns
-------
json
    All the entity nodes in a list of the target entity type
"""
@app.route('/<entity_type>/entities', methods = ['GET'])
def get_entities_by_type(entity_type):
    final_result = []

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_type)

    # Validate the normalized_entity_type to ensure it's one of the accepted types
    try:
        schema_manager.validate_normalized_entity_type(normalized_entity_type)
    except schema_errors.InvalidNormalizedEntityTypeException as e:
        bad_request_error("Invalid entity type provided: " + entity_type)

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        # Generate trigger data and merge into a big dict.  Specify the name of properties which may
        # be time-consuming to generate using triggers for some entities.  The properties will be
        # skipped if generated (and included on entities where they are not generated.)
        # and skip some of the properties that are time-consuming to generate via triggers
        generated_properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Upload
            'datasets',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'previous_revision_uuid',
            'next_revision_uuid'
        ]
        # Get user token from Authorization header.  Since this endpoint is not exposed through the AWS Gateway
        token = get_user_token(request)

        # Get back a list of entity dicts for the given entity type
        entities_list = app_neo4j_queries.get_entities_by_type(neo4j_driver_instance, normalized_entity_type)

        complete_entities_list = schema_manager.get_complete_entities_list(token, entities_list, generated_properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    # Response with the final result
    return jsonify(final_result)

"""
Create an entity of the target type in neo4j

Response result filtering is supported based on query string
For example: /entities/<entity_type>?return_all_properties=true
Default to skip those time-consuming properties

Parameters
----------
entity_type : str
    One of the target entity types (case-insensitive since will be normalized): Dataset, Donor, Sample, Upload, Collection

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/entities/<entity_type>', methods = ['POST'])
def create_entity(entity_type):
    if READ_ONLY_MODE:
        forbidden_error("Access not granted when entity-api in READ-ONLY mode")

    # If an invalid token provided, we need to tell the client with a 401 error, rather
    # than a 500 error later if the token is not good.
    validate_token_if_auth_header_exists(request)
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_type)

    # Validate the normalized_entity_type to make sure it's one of the accepted types
    try:
        schema_manager.validate_normalized_entity_type(normalized_entity_type)
    except schema_errors.InvalidNormalizedEntityTypeException as e:
        bad_request_error(f"Invalid entity type provided: {entity_type}")

    # Execute entity level validator defined in schema yaml before entity creation
    # Currently on Dataset and Upload creation require application header
    try:
        schema_manager.execute_entity_level_validator('before_entity_create_validator', normalized_entity_type, request)
    except schema_errors.MissingApplicationHeaderException as e:
        bad_request_error(e)
    except schema_errors.InvalidApplicationHeaderException as e:
        bad_request_error(e)

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

    # Execute property level validators defined in schema yaml before entity property creation
    # Use empty dict {} to indicate there's no existing_data_dict
    try:
        schema_manager.execute_property_level_validators('before_property_create_validators', normalized_entity_type, request, {}, json_data_dict)
    # Currently only ValueError
    except ValueError as e:
        bad_request_error(e)
    except schema_errors.UnimplementedValidatorException as uve:
        internal_server_error(uve)

    # Check URL parameters before proceeding to any CRUD operations, halting on validation failures.
    #
    # Check if re-indexing is to be suppressed after entity creation.
    try:
        supress_reindex = _suppress_reindex()
    except Exception as e:
        bad_request_error(e)

    # Additional validation for Sample entities
    if normalized_entity_type == 'Sample':
        direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
        # Check existence of the direct ancestor (either another Sample or Donor)
        direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)

        # `sample_category` is required on create
        sample_category = json_data_dict['sample_category'].lower()
        
        # Validations on registering an organ
        if sample_category == 'organ':
            # To register an organ, the source has to be a Donor
            # It doesn't make sense to register an organ with some other sample type as the parent
            if direct_ancestor_dict['entity_type'] != 'Donor':
                bad_request_error("To register an organ, the source has to be a Donor")

            # A valid organ code must be present in the `organ` field
            if ('organ' not in json_data_dict) or (json_data_dict['organ'].strip() == ''):
                bad_request_error("A valid organ code is required when registering an organ associated with a Donor")
            
            # Must be a 2-letter alphabetic code and can be found in UBKG ontology-api
            validate_organ_code(json_data_dict['organ'])
        else:
            if 'organ' in json_data_dict:
                bad_request_error("The sample category must be organ when an organ code is provided")

        # Creating the ids require organ code to be specified for the samples to be created when the
        # sample's direct ancestor is a Donor.
        if direct_ancestor_dict['entity_type'] == 'Donor':
            if sample_category != 'organ':
                bad_request_error("The sample category must be organ when the direct ancestor is a Donor")

            if ('organ' not in json_data_dict) or (json_data_dict['organ'].strip() == ''):
                bad_request_error("A valid organ code is required when registering an organ associated with a Donor")

            validate_organ_code(json_data_dict['organ'])

    # Additional validation for Dataset entities
    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Adding publication to the check for direct ancestors. Derek-Furst 2/17/23
        #
        # `direct_ancestor_uuids` is required for creating new Dataset.
        # Verify all of the direct ancestor UUIDs exist in the Neo4j graph.
        # Form an error response if an Exception is raised.
        try:
            app_neo4j_queries.uuids_all_exist(  neo4j_driver=neo4j_driver_instance
                                                , uuids=json_data_dict['direct_ancestor_uuids'])
        except Exception as e:
            bad_request_error(err_msg=  f"Verifying existence of {len(json_data_dict['direct_ancestor_uuids'])}"
                                        f" ancestor IDs caused: '{str(e)}'")

        # Also check existence of the previous revision dataset if specified
        if 'previous_revision_uuid' in json_data_dict:
            previous_revision = json_data_dict['previous_revision_uuid']
            previous_version_dict = query_target_entity(previous_revision, user_token)

            # Make sure the previous version entity is either a Dataset or Sample (and publication 2/17/23)
            if not schema_manager.entity_type_instanceof(previous_version_dict['entity_type'], 'Dataset'):
                bad_request_error(f"The previous_revision_uuid specified for this dataset must be either a Dataset or Sample or Publication")

            next_revision_is_latest = app_neo4j_queries.is_next_revision_latest(neo4j_driver_instance, previous_version_dict['uuid'])

            # As long as the list is not empty, tell the users to use a different 'previous_revision_uuid'
            if not next_revision_is_latest:
                bad_request_error(f"The previous_revision_uuid specified for this dataset has already had a next revision")

            # Only published datasets can have revisions made of them. Verify that that status of the Dataset specified
            # by previous_revision_uuid is published. Else, bad request error.
            if previous_version_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
                bad_request_error(f"The previous_revision_uuid specified for this dataset must be 'Published' in order to create a new revision from it")

    # If the preceding "additional validations" did not raise an error,
    # generate 'before_create_trigger' data and create the entity details in Neo4j
    merged_dict = create_entity_details(request, normalized_entity_type, user_token, json_data_dict)

    # For Donor: link to parent Lab node
    # For Sample: link to existing direct ancestor
    # For Dataset: link to direct ancestors
    # For Collection: link to member Datasets
    # For Upload: link to parent Lab node
    after_create(normalized_entity_type, user_token, merged_dict)

    # By default we'll return all the properties but skip these time-consuming ones
    # Donor doesn't need to skip any
    properties_to_skip = []

    if normalized_entity_type == 'Sample':
        properties_to_skip = [
            'direct_ancestor'
        ]
    # 2/17/23 - Also skipping these properties for publications ~Derek Furst
    elif schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        properties_to_skip = [
            'direct_ancestors',
            'collections',
            'upload',
            'title', 
            'previous_revision_uuid', 
            'next_revision_uuid'
        ]
    elif normalized_entity_type in ['Upload', 'Collection', 'Epicollection']:
        properties_to_skip = [
            'datasets'
        ]

    # Result filtering based on query string
    # Will return all properties by running all the read triggers
    # If the request specifies `/entities/<entity_type>?return_all_properties=true`
    if bool(request.args):
        # The parsed query string value is a string 'true'
        return_all_properties = request.args.get('return_all_properties')

        if (return_all_properties is not None) and (return_all_properties.lower() == 'true'):
            properties_to_skip = []

    # Generate the filtered or complete entity dict to send back
    complete_dict = schema_manager.get_complete_entity_result(user_token, merged_dict, properties_to_skip)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_entity_result_for_response(complete_dict)

    if supress_reindex:
        logger.log(level=logging.INFO
                   , msg=f"Re-indexing suppressed during creation of {complete_dict['entity_type']}"
                         f" with UUID {complete_dict['uuid']}")
    else:
        # Also index the new entity node in elasticsearch via search-api
        logger.log(level=logging.INFO
                   , msg=f"Re-indexing for creation of {complete_dict['entity_type']}"
                         f" with UUID {complete_dict['uuid']}")
        reindex_entity(complete_dict['uuid'], user_token)

    return jsonify(normalized_complete_dict)


"""
Create multiple samples from the same source entity

Parameters
----------
count : str
    The number of samples to be created

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/entities/multiple-samples/<count>', methods = ['POST'])
def create_multiple_samples(count):
    if READ_ONLY_MODE:
        forbidden_error("Access not granted when entity-api in READ-ONLY mode")

    # Get user token from Authorization header
    user_token = get_user_token(request)

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

    try:
        schema_manager.execute_property_level_validators('before_property_create_validators', normalized_entity_type, request, {}, json_data_dict)
    # Currently only ValueError
    except ValueError as e:
        bad_request_error(e)
    except schema_errors.UnimplementedValidatorException as uve:
        internal_server_error(uve)

    # `direct_ancestor_uuid` is required on create for a Sample.
    # Check existence of the direct ancestor (either another Sample or Donor)
    direct_ancestor_dict = query_target_entity(json_data_dict['direct_ancestor_uuid'], user_token)

    # Creating the ids require organ code to be specified for the samples to be created when the
    # sample's direct ancestor is a Donor.
    # Must be one of the codes from: https://github.com/hubmapconsortium/search-api/blob/main/src/search-schema/data/definitions/enums/organ_types.yaml
    if direct_ancestor_dict['entity_type'] == 'Donor':
        # `sample_category` is required on create
        if json_data_dict['sample_category'].lower() != 'organ':
            bad_request_error("The sample_category must be organ since the direct ancestor is a Donor")

        # Currently we don't validate the provided organ code though
        if ('organ' not in json_data_dict) or (not json_data_dict['organ']):
            bad_request_error("A valid organ code is required since the direct ancestor is a Donor")

    # Generate 'before_create_trigger' data and create the entity details in Neo4j
    generated_ids_dict_list = create_multiple_samples_details(request, normalized_entity_type, user_token, json_data_dict, count)

    # Also index the each new Sample node in elasticsearch via search-api
    for id_dict in generated_ids_dict_list:
        reindex_entity(id_dict['uuid'], user_token)

    return jsonify(generated_ids_dict_list)


"""
Update the properties of a given entity

Parameters
----------
entity_type : str
    One of the normalized entity types: Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
str
    A successful message
"""
@app.route('/entities/<id>', methods = ['PUT'])
def update_entity(id):
    if READ_ONLY_MODE:
        forbidden_error("Access not granted when entity-api in READ-ONLY mode")

    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Normalize user provided status
    if "status" in json_data_dict:
        normalized_status = schema_manager.normalize_status(json_data_dict["status"])
        json_data_dict["status"] = normalized_status

    has_updated_status = False
    if ('status' in json_data_dict) and (json_data_dict['status']):
        has_updated_status = True

    # Normalize user provided status
    if "sub_status" in json_data_dict:
        normalized_status = schema_manager.normalize_status(json_data_dict["sub_status"])
        json_data_dict["sub_status"] = normalized_status

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, user_token)
    entity_uuid = entity_dict['uuid']

    # Check that the user has the correct access to modify this entity
    validate_user_update_privilege(entity_dict, user_token)

    # Normalize user provided entity_type
    normalized_entity_type = schema_manager.normalize_entity_type(entity_dict['entity_type'])

    # Execute entity level validator defined in schema yaml before entity modification.
    lockout_overridden = False
    try:
        schema_manager.execute_entity_level_validator(validator_type='before_entity_update_validator'
                                                      , normalized_entity_type=normalized_entity_type
                                                      , request=request
                                                      , existing_entity_dict=entity_dict)
    except schema_errors.MissingApplicationHeaderException as e:
        bad_request_error(e)
    except schema_errors.InvalidApplicationHeaderException as e:
        bad_request_error(e)
    except schema_errors.LockedEntityUpdateException as leue:
        # HTTP header names are case-insensitive, and request.headers.get() returns None if the header doesn't exist
        locked_entity_update_header = request.headers.get(SchemaConstants.LOCKED_ENTITY_UPDATE_HEADER)
        if locked_entity_update_header and (LOCKED_ENTITY_UPDATE_OVERRIDE_KEY == locked_entity_update_header):
            lockout_overridden = True
            logger.info(f"For {normalized_entity_type} {entity_uuid}"
                        f" update prohibited due to {str(leue)},"
                        f" but being overridden by valid {SchemaConstants.LOCKED_ENTITY_UPDATE_HEADER} in request.")
        else:
            forbidden_error(leue)
    except Exception as e:
        internal_server_error(e)

    # Validate request json against the yaml schema
    # Pass in the entity_dict for missing required key check, this is different from creating new entity
    try:
        schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_type, existing_entity_dict = entity_dict)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        bad_request_error(str(e))

    # Execute property level validators defined in schema yaml before entity property update
    try:
        schema_manager.execute_property_level_validators('before_property_update_validators', normalized_entity_type, request, entity_dict, json_data_dict)
    except (schema_errors.MissingApplicationHeaderException,
            schema_errors.InvalidApplicationHeaderException,
            KeyError,
            ValueError) as e:
        bad_request_error(e)

    # Check URL parameters before proceeding to any CRUD operations, halting on validation failures.
    #
    # Check if re-indexing is to be suppressed after entity creation.
    try:
        suppress_reindex = _suppress_reindex()
    except Exception as e:
        bad_request_error(e)

    # Proceed with per-entity updates after passing any entity-level or property-level validations which
    # would have locked out updates.
    #
    # Sample, Dataset, and Upload: additional validation, update entity, after_update_trigger
    # Collection and Donor: update entity
    if normalized_entity_type == 'Sample':
        # A bit more validation for updating the sample and the linkage to existing source entity
        has_direct_ancestor_uuid = False
        if ('direct_ancestor_uuid' in json_data_dict) and json_data_dict['direct_ancestor_uuid']:
            has_direct_ancestor_uuid = True

            direct_ancestor_uuid = json_data_dict['direct_ancestor_uuid']
            # Check existence of the source entity
            direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)
            # Also make sure it's either another Sample or a Donor
            if direct_ancestor_dict['entity_type'] not in ['Donor', 'Sample']:
                bad_request_error(f"The uuid: {direct_ancestor_uuid} is not a Donor neither a Sample, cannot be used as the direct ancestor of this Sample")

        # Generate 'before_update_triiger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle linkages update via `after_update_trigger` methods
        if has_direct_ancestor_uuid:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    # 2/17/23 - Adding direct ancestor checks to publication as well as dataset.
    elif normalized_entity_type in ['Dataset', 'Publication']:
        # A bit more validation if `direct_ancestor_uuids` provided
        has_direct_ancestor_uuids = False
        has_associated_collection_uuid = False
        if ('direct_ancestor_uuids' in json_data_dict) and (json_data_dict['direct_ancestor_uuids']):
            has_direct_ancestor_uuids = True

            # `direct_ancestor_uuids` is required for updating a Dataset.
            # Verify all of the direct ancestor UUIDs exist in the Neo4j graph.
            # Form an error response if an Exception is raised.
            try:
                app_neo4j_queries.uuids_all_exist(neo4j_driver=neo4j_driver_instance
                                                  , uuids=json_data_dict['direct_ancestor_uuids'])
            except Exception as e:
                bad_request_error(err_msg=  f"Verifying existence of {len(json_data_dict['direct_ancestor_uuids'])}"
                                            f" ancestor IDs caused: '{str(e)}'")

        if ('associated_collection_uuid' in json_data_dict) and (json_data_dict['associated_collection_uuid']):
            has_associated_collection_uuid = True

            # Check existence of associated collection
            associated_collection_dict = query_target_entity(json_data_dict['associated_collection_uuid'], user_token)

        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle linkages update via `after_update_trigger` methods
        if has_direct_ancestor_uuids or has_associated_collection_uuid or has_updated_status:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    elif normalized_entity_type == 'Upload':
        has_dataset_uuids_to_link = False
        if ('dataset_uuids_to_link' in json_data_dict) and (json_data_dict['dataset_uuids_to_link']):
            has_dataset_uuids_to_link = True

            # Check existence of those datasets to be linked
            # If one of the datasets to be linked appears to be already linked,
            # neo4j query won't create the new linkage due to the use of `MERGE`
            for dataset_uuid in json_data_dict['dataset_uuids_to_link']:
                dataset_dict = query_target_entity(dataset_uuid, user_token)
                # Also make sure it's a Dataset (or publication 2/17/23)
                if dataset_dict['entity_type'] not in ['Dataset', 'Publication']:
                    bad_request_error(f"The uuid: {dataset_uuid} is not a Dataset or Publication, cannot be linked to this Upload")

        has_dataset_uuids_to_unlink = False
        if ('dataset_uuids_to_unlink' in json_data_dict) and (json_data_dict['dataset_uuids_to_unlink']):
            has_dataset_uuids_to_unlink = True

            # Check existence of those datasets to be unlinked
            # If one of the datasets to be unlinked appears to be not linked at all,
            # the neo4j cypher will simply skip it because it won't match the "MATCH" clause
            # So no need to tell the end users that this dataset is not linked
            # Let alone checking the entity type to ensure it's a Dataset
            for dataset_uuid in json_data_dict['dataset_uuids_to_unlink']:
                dataset_dict = query_target_entity(dataset_uuid, user_token)

        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle linkages update via `after_update_trigger` methods
        if has_dataset_uuids_to_link or has_dataset_uuids_to_unlink or has_updated_status:
            after_update(normalized_entity_type, user_token, merged_updated_dict)
    elif schema_manager.entity_type_instanceof(normalized_entity_type, 'Collection'):
        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

        # Handle linkages update via `after_update_trigger` methods
        after_update(normalized_entity_type, user_token, merged_updated_dict)
    else:
        # Generate 'before_update_trigger' data and update the entity details in Neo4j
        merged_updated_dict = update_entity_details(request, normalized_entity_type, user_token, json_data_dict, entity_dict)

    # Remove the cached entities if Memcached is being used
    # DO NOT update the cache with new entity dict because the returned dict from PUT (some properties maybe skipped)
    # can be different from the one generated by GET call
    if MEMCACHED_MODE:
        delete_cache(entity_uuid, normalized_entity_type)

    # Also reindex the updated entity in elasticsearch via search-api
    if suppress_reindex:
        logger.log(level=logging.INFO
                   , msg=f"Re-indexing suppressed during modification of {normalized_entity_type}"
                         f" with UUID {entity_uuid}")
    else:
        # Also index the new entity node in elasticsearch via search-api
        logger.log(level=logging.INFO
                   , msg=f"Re-indexing for modification of {normalized_entity_type}"
                         f" with UUID {entity_uuid}")
        reindex_entity(entity_uuid, user_token)

    # Do not return the updated dict to avoid computing overhead - 7/14/2023 by Zhou
    message_returned = f"The update request on {normalized_entity_type} of {id} has been accepted, the backend may still be processing"
    if lockout_overridden:
        message_returned = f"Lockout overridden on {normalized_entity_type} of {id}"

    # Here we use 200 status code instead of 202 mainly for compatibility
    # so the API consumers don't need to update their implementations
    return jsonify({'message': message_returned})


"""
Get all ancestors of the given entity

The gateway treats this endpoint as public accessible

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
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']
    public_entity = True
    # Collection doesn't have ancestors via Activity nodes
    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Collection'):
        bad_request_error(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            public_entity = False
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required = True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            public_entity = False
            token = get_user_token(request, non_public_access_required = True)
    else:
        # Donor and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = schema_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        ancestors_list = schema_neo4j_queries.get_ancestors(neo4j_driver_instance, uuid)

        # Generate trigger data
        # Skip some of the properties that are time-consuming to generate via triggers
        # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
        # checks when the target Dataset is public but the revisions are not public
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(token, ancestors_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
        filtered_final_result = []
        for ancestor in final_result:
            ancestor_entity_type = ancestor.get('entity_type')
            fields_to_exclude = schema_manager.get_fields_to_exclude(ancestor_entity_type)
            if public_entity and not user_in_hubmap_read_group(request):
                filtered_ancestor = schema_manager.exclude_properties_from_response(fields_to_exclude, ancestor)
                filtered_final_result.append(filtered_ancestor)
            else:
                filtered_final_result.append(ancestor)
        final_result = filtered_final_result
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
    global anS3Worker

    final_result = []

    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Collection and Upload don't have descendants via Activity nodes
    # No need to check, it'll always return empty list

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = schema_neo4j_queries.get_descendants(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        descendants_list = schema_neo4j_queries.get_descendants(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(user_token, descendants_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    # Check the size of what is to be returned through the AWS Gateway, and replace it with
    # a response that links to an Object in the AWS S3 Bucket, if appropriate.
    try:
        resp_body = json.dumps(final_result).encode('utf-8')
        s3_url = anS3Worker.stash_response_body_if_big(resp_body)
        if s3_url is not None:
            return Response(response=s3_url
                            , status=303)  # See Other
        # The HuBMAP Commons S3Worker will return None for a URL when the response body is
        # smaller than it is configured to store, so the response should be returned through
        # the AWS Gateway
    except Exception as s3exception:
        logger.error(f"Error using anS3Worker to handle len(resp_body)="
                     f"{len(resp_body)}.")
        logger.error(s3exception, exc_info=True)
        return Response(response=f"Unexpected error storing large results in S3. See logs."
                        , status=500)

    # Return a regular response through the AWS Gateway
    return jsonify(final_result)


"""
Get all parents of the given entity

The gateway treats this endpoint as public accessible

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
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']
    public_entity = True
    # Collection doesn't have ancestors via Activity nodes
    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Collection'):
        bad_request_error(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            public_entity = False
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required = True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            public_entity = False
            token = get_user_token(request, non_public_access_required = True)
    else:
        # Donor and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = schema_neo4j_queries.get_parents(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        parents_list = schema_neo4j_queries.get_parents(neo4j_driver_instance, uuid)

        # Generate trigger data
        # Skip some of the properties that are time-consuming to generate via triggers
        # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
        # checks when the target Dataset is public but the revisions are not public
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(token, parents_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
        filtered_final_result = []
        for parent in final_result:
            parent_entity_type = parent.get('entity_type')
            fields_to_exclude = schema_manager.get_fields_to_exclude(parent_entity_type)
            if public_entity and not user_in_hubmap_read_group(request):
                filtered_parent = schema_manager.exclude_properties_from_response(fields_to_exclude, parent)
                filtered_final_result.append(filtered_parent)
            else:
                filtered_final_result.append(parent)
        final_result = filtered_final_result

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
    final_result = []

    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Collection and Upload don't have children via Activity nodes
    # No need to check, it'll always return empty list

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = schema_neo4j_queries.get_children(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        children_list = schema_neo4j_queries.get_children(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(user_token, children_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all siblings of the given entity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /entities/<id>/siblings?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the siblings of the target entity
"""
@app.route('/entities/<id>/siblings', methods = ['GET'])
def get_siblings(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']
    public_entity = True
    # Collection doesn't have ancestors via Activity nodes
    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Collection'):
        bad_request_error(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            public_entity = False
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required = True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            public_entity = False
            token = get_user_token(request, non_public_access_required = True)
    else:
        # Donor and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    status = None
    property_key = None
    include_revisions = None
    accepted_args = ['property', 'status', 'include-old-revisions']
    if bool(request.args):
        for arg_name in request.args.keys():
            if arg_name not in accepted_args:
                bad_request_error(f"{arg_name} is an unrecognized argument")
        property_key = request.args.get('property')
        status = request.args.get('status')
        include_revisions = request.args.get('include-old-revisions')
        if status is not None:
            status = status.lower()
            if status not in ['new', 'processing', 'published', 'qa', 'error', 'hold', 'invalid', 'submitted']:
                bad_request_error("Invalid Dataset Status. Must be 'new', 'qa', or 'published' Case-Insensitive")
        if property_key is not None:
            property_key = property_key.lower()
            result_filtering_accepted_property_keys = ['uuid']
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")
        if include_revisions is not None:
            include_revisions = include_revisions.lower()
            if include_revisions not in ['true', 'false']:
                bad_request_error("Invalid 'include-old-revisions'. Accepted values are 'true' and 'false' Case-Insensitive")
            if include_revisions == 'true':
                include_revisions = True
            else:
                include_revisions = False
    sibling_list = app_neo4j_queries.get_siblings(neo4j_driver_instance, uuid, status, property_key, include_revisions)
    if property_key is not None:
        return jsonify(sibling_list)
    # Generate trigger data
    # Skip some of the properties that are time-consuming to generate via triggers
    # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
    # checks when the target Dataset is public but the revisions are not public
    properties_to_skip = [
        # Properties to skip for Sample
        'direct_ancestor',
        # Properties to skip for Dataset
        'direct_ancestors',
        'collections',
        'upload',
        'title',
        'next_revision_uuid',
        'previous_revision_uuid',
        'associated_collection',
        'creation_action',
        'local_directory_rel_path'
    ]

    complete_entities_list = schema_manager.get_complete_entities_list(token, sibling_list, properties_to_skip)
    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
    filtered_final_result = []
    for sibling in final_result:
        sibling_entity_type = sibling.get('entity_type')
        fields_to_exclude = schema_manager.get_fields_to_exclude(sibling_entity_type)
        if public_entity and not user_in_hubmap_read_group(request):
            filtered_sibling = schema_manager.exclude_properties_from_response(fields_to_exclude, sibling)
            filtered_final_result.append(filtered_sibling)
        else:
            filtered_final_result.append(sibling)
    final_result = filtered_final_result
    return jsonify(final_result)


"""
Get all tuplets of the given entit: sibling entities sharing an parent activity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /entities/{id}/tuplets?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of all the tuplets of the target entity
"""
@app.route('/entities/<id>/tuplets', methods = ['GET'])
def get_tuplets(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']
    public_entity = True
    # Collection doesn't have ancestors via Activity nodes
    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Collection'):
        bad_request_error(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        # Only published/public datasets don't require token
        if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
            public_entity = False
            # Token is required and the user must belong to HuBMAP-READ group
            token = get_user_token(request, non_public_access_required = True)
    elif normalized_entity_type == 'Sample':
        # The `data_access_level` of Sample can only be either 'public' or 'consortium'
        if entity_dict['data_access_level'] == ACCESS_LEVEL_CONSORTIUM:
            public_entity = False
            token = get_user_token(request, non_public_access_required = True)
    else:
        # Donor and Upload will always get back an empty list
        # becuase their direct ancestor is Lab, which is being skipped by Neo4j query
        # So no need to execute the code below
        return jsonify(final_result)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    status = None
    property_key = None
    accepted_args = ['property', 'status']
    if bool(request.args):
        for arg_name in request.args.keys():
            if arg_name not in accepted_args:
                bad_request_error(f"{arg_name} is an unrecognized argument")
        property_key = request.args.get('property')
        status = request.args.get('status')
        if status is not None:
            status = status.lower()
            if status not in ['new', 'processing', 'published', 'qa', 'error', 'hold', 'invalid', 'submitted']:
                bad_request_error("Invalid Dataset Status. Must be 'new', 'qa', or 'published' Case-Insensitive")
        if property_key is not None:
            property_key = property_key.lower()
            result_filtering_accepted_property_keys = ['uuid']
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")
    tuplet_list = app_neo4j_queries.get_tuplets(neo4j_driver_instance, uuid, status, property_key)
    if property_key is not None:
        return jsonify(tuplet_list)
    # Generate trigger data
    # Skip some of the properties that are time-consuming to generate via triggers
    # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
    # checks when the target Dataset is public but the revisions are not public
    properties_to_skip = [
        # Properties to skip for Sample
        'direct_ancestor',
        # Properties to skip for Dataset
        'direct_ancestors',
        'collections',
        'upload',
        'title',
        'next_revision_uuid',
        'previous_revision_uuid',
        'associated_collection',
        'creation_action',
        'local_directory_rel_path'
    ]

    complete_entities_list = schema_manager.get_complete_entities_list(token, tuplet_list, properties_to_skip)
    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
    filtered_final_result = []
    for tuplet in final_result:
        tuple_entity_type = tuplet.get('entity_type')
        fields_to_exclude = schema_manager.get_fields_to_exclude(tuple_entity_type)
        if public_entity and not user_in_hubmap_read_group(request):
            filtered_tuplet = schema_manager.exclude_properties_from_response(fields_to_exclude, tuplet)
            filtered_final_result.append(filtered_tuplet)
        else:
            filtered_final_result.append(tuplet)
    final_result = filtered_final_result
    return jsonify(final_result)


"""
Get all previous revisions of the given entity
Result filtering based on query string
For example: /previous_revisions/<id>?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of entities that are the previous revisions of the target entity
"""
@app.route('/previous_revisions/<id>', methods = ['GET'])
def get_previous_revisions(id):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_previous_revisions(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        descendants_list = app_neo4j_queries.get_previous_revisions(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            'collections', 
            'upload', 
            'title',
            'direct_ancestors'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(user_token, descendants_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all next revisions of the given entity
Result filtering based on query string
For example: /next_revisions/<id>?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    A list of entities that are the next revisions of the target entity
"""
@app.route('/next_revisions/<id>', methods = ['GET'])
def get_next_revisions(id):
    # Get user token from Authorization header
    user_token = get_user_token(request)

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, user_token)
    uuid = entity_dict['uuid']

    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = app_neo4j_queries.get_next_revisions(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        descendants_list = app_neo4j_queries.get_next_revisions(neo4j_driver_instance, uuid)

        # Generate trigger data and merge into a big dict
        # and skip some of the properties that are time-consuming to generate via triggers
        properties_to_skip = [
            'collections', 
            'upload', 
            'title',
            'direct_ancestors'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(user_token, descendants_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Get all collections of the given entity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /entities/<id>/collections?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity 

Returns
-------
json
    A list of all the collections of the target entity
"""
@app.route('/entities/<id>/collections', methods = ['GET'])
def get_collections(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']
    public_entity = True

    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        public_entity = False
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = schema_neo4j_queries.get_collections(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        collection_list = schema_neo4j_queries.get_collections(neo4j_driver_instance, uuid)

        # Generate trigger data
        # Skip some of the properties that are time-consuming to generate via triggers
        # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
        # checks when the target Dataset is public but the revisions are not public
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(token, collection_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
        filtered_final_result = []
        for collection in final_result:
            collection_entity_type = collection.get('entity_type')
            fields_to_exclude = schema_manager.get_fields_to_exclude(collection_entity_type)
            if public_entity and not user_in_hubmap_read_group(request):
                filtered_collection = schema_manager.exclude_properties_from_response(fields_to_exclude, collection)
                datasets = filtered_collection.get('datasets')
                filtered_datasets = []
                for dataset in datasets:
                    dataset_fields_to_exclude = schema_manager.get_fields_to_exclude(dataset.get('entity_type'))
                    filtered_dataset = schema_manager.exclude_properties_from_response(dataset_fields_to_exclude, dataset)
                    filtered_datasets.append(filtered_dataset)
                filtered_collection['datasets'] = filtered_datasets
                filtered_final_result.append(filtered_collection)
            else:
                filtered_final_result.append(collection)
        final_result = filtered_final_result

    return jsonify(final_result)


"""
Get all uploads of the given entity

The gateway treats this endpoint as public accessible

Result filtering based on query string
For example: /entities/<id>/uploads?property=uuid

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity 

Returns
-------
json
    A list of all the uploads of the target entity
"""
@app.route('/entities/<id>/uploads', methods = ['GET'])
def get_uploads(id):
    final_result = []

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    uuid = entity_dict['uuid']

    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error(f"Unsupported entity type of id {id}: {normalized_entity_type}")

    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or the user token has the correct access level
    # Result filtering based on query string
    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            result_filtering_accepted_property_keys = ['uuid']

            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            # Only return a list of the filtered property value of each entity
            property_list = schema_neo4j_queries.get_uploads(neo4j_driver_instance, uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    # Return all the details if no property filtering
    else:
        uploads_list = schema_neo4j_queries.get_uploads(neo4j_driver_instance, uuid)

        # Generate trigger data
        # Skip some of the properties that are time-consuming to generate via triggers
        # Also skip next_revision_uuid and previous_revision_uuid for Dataset to avoid additional
        # checks when the target Dataset is public but the revisions are not public
        properties_to_skip = [
            # Properties to skip for Sample
            'direct_ancestor',
            # Properties to skip for Dataset
            'direct_ancestors',
            'collections',
            'upload',
            'title',
            'next_revision_uuid',
            'previous_revision_uuid'
        ]

        complete_entities_list = schema_manager.get_complete_entities_list(token, uploads_list, properties_to_skip)

        # Final result after normalization
        final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)

    return jsonify(final_result)


"""
Retrieves and validates constraints based on definitions within lib.constraints

Authentication
-------
No token is required

Query Paramters
-------
N/A

Request Body
-------
Requires a json list in the request body matching the following example
Example:
            [{
<required>      "ancestors": {
<required>            "entity_type": "sample",
<optional>            "sub_type": ["organ"],
<optional>            "sub_type_val": ["BD"],
                 },
<required>      "descendants": {
<required>           "entity_type": "sample",
<optional>           "sub_type": ["suspension"]
                 }
             }]
Returns
--------
JSON
"""
@app.route('/constraints', methods=['POST'])
def validate_constraints():
    if not request.is_json:
        bad_request_error("A json body and appropriate Content-Type header are required")
    json_entry = request.get_json()
    is_valid = constraints_json_is_valid(json_entry)
    if is_valid is not True:
        bad_request_error(is_valid)
    is_match = request.values.get('match')
    order = request.values.get('order')

    results = []
    final_result = {
        'code': 200,
        'description': {},
        'name': "ok"
    }

    for constraint in json_entry:
        if order == 'descendants':
            result = get_constraints(constraint, 'descendants', 'ancestors', is_match)
        else:
            result = get_constraints(constraint, 'ancestors', 'descendants', is_match)
        if result.get('code') != 200:
            final_result = {
                'code': 400,
                'name': 'Bad Request'
            }

        results.append(result)

    final_result['description'] = results
    return make_response(final_result, int(final_result.get('code')), {"Content-Type": "application/json"})


"""
Redirect a request from a doi service for a dataset or collection

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of the target entity
"""
# To continue supporting the already published collection DOIs
@app.route('/collection/redirect/<id>', methods = ['GET'])
# New route
@app.route('/doi/redirect/<id>', methods = ['GET'])
def doi_redirect(id):
    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)

    entity_type = entity_dict['entity_type']

    # Only for collection
    if entity_type not in ['Collection', 'Epicollection', 'Dataset', 'Publication']:
        bad_request_error("The target entity of the specified id must be a Collection or Dataset or Publication")

    uuid = entity_dict['uuid']

    # URL template
    redirect_url = app.config['DOI_REDIRECT_URL']

    if (redirect_url.lower().find('<entity_type>') == -1) or (redirect_url.lower().find('<identifier>') == -1):
        # Log the full stack trace, prepend a line with our message
        msg = "Incorrect configuration value for 'DOI_REDIRECT_URL'"
        logger.exception(msg)
        internal_server_error(msg)

    rep_entity_type_pattern = re.compile(re.escape('<entity_type>'), re.RegexFlag.IGNORECASE)
    redirect_url = rep_entity_type_pattern.sub(entity_type.lower(), redirect_url)

    rep_identifier_pattern = re.compile(re.escape('<identifier>'), re.RegexFlag.IGNORECASE)
    redirect_url = rep_identifier_pattern.sub(uuid, redirect_url)

    resp = Response("Page has moved", 307)
    resp.headers['Location'] = redirect_url.strip()

    return resp


"""
Redirection method created for REFERENCE organ DOI redirection, but can be for others if needed

The gateway treats this endpoint as public accessible

Parameters
----------
hmid : str
    The HuBMAP ID (e.g. HBM123.ABCD.456)
"""
@app.route('/redirect/<hmid>', methods = ['GET'])
def redirect(hmid):
    cid = hmid.upper().strip()
    if cid in reference_redirects:
        redir_url = reference_redirects[cid]
        resp = Response("page has moved", 307)
        resp.headers['Location'] = redir_url.strip()
        return resp
    else:
        return Response(f"{hmid} not found.", 404)


"""
Get the Globus URL to the given Dataset or Upload

The gateway treats this endpoint as public accessible

It will provide a Globus URL to the dataset/upload directory in of three Globus endpoints based on the access
level of the user (public, consortium or protected), public only, of course, if no token is provided.
If a dataset/upload isn't found a 404 will be returned. There is a chance that a 500 can be returned, but not
likely under normal circumstances, only for a misconfigured or failing in some way endpoint. 

If the Auth token is provided but is expired or invalid a 401 is returned. If access to the dataset/upload 
is not allowed for the user (or lack of user) a 403 is returned.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
Response
    200 with the Globus Application URL to the directory of dataset/upload
    404 Dataset/Upload not found
    403 Access Forbidden
    401 Unauthorized (bad or expired token)
    500 Unexpected server or other error
"""
# Thd old routes for backward compatibility - will be deprecated eventually
@app.route('/entities/dataset/globus-url/<id>', methods = ['GET'])
@app.route('/dataset/globus-url/<id>', methods = ['GET'])
# New route
@app.route('/entities/<id>/globus-url', methods = ['GET'])
def get_globus_url(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    # Then retrieve the allowable data access level (public, protected or consortium)
    # for the dataset and HuBMAP Component ID that the dataset belongs to
    entity_dict = query_target_entity(id, token)
    uuid = entity_dict['uuid']
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset and Upload (and publication 2/17/23 ~Derek Furst)
    if normalized_entity_type not in ['Dataset', 'Publication', 'Upload']:
        bad_request_error("The target entity of the specified id is not a Dataset nor a Upload not a Publication")

    # Upload doesn't have this 'data_access_level' property, we treat it as 'protected'
    # For Dataset, if no access level is present, default to protected too
    if not 'data_access_level' in entity_dict or string_helper.isBlank(entity_dict['data_access_level']):
        entity_data_access_level = ACCESS_LEVEL_PROTECTED
    else:
        entity_data_access_level = entity_dict['data_access_level']

    # Get the globus groups info based on the groups json file in commons package
    globus_groups_info = auth_helper_instance.get_globus_groups_info()
    groups_by_id_dict = globus_groups_info['by_id']

    if not 'group_uuid' in entity_dict or string_helper.isBlank(entity_dict['group_uuid']):
        msg = f"The 'group_uuid' property is not set for {normalized_entity_type} with uuid: {uuid}"
        logger.exception(msg)
        internal_server_error(msg)

    group_uuid = entity_dict['group_uuid']

    # Validate the group_uuid
    try:
        schema_manager.validate_entity_group_uuid(group_uuid)
    except schema_errors.NoDataProviderGroupException:
        msg = f"Invalid 'group_uuid': {group_uuid} for {normalized_entity_type} with uuid: {uuid}"
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
            msg = f"Unexpected error, data access level could not be found for user trying to access {normalized_entity_type} id: {id}"
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

    # Note: `entity_data_access_level` for Upload is always default to 'protected'
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
Retrive the latest (newest) revision of a Dataset

Public/Consortium access rules apply - if no token/consortium access then 
must be for a public dataset and the returned Dataset must be the latest public version.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
json
    The detail of the latest revision dataset if exists
    Otherwise an empty JSON object {}
"""
@app.route('/datasets/<id>/latest-revision', methods = ['GET'])
def get_dataset_latest_revision(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    fields_to_exclude = schema_manager.get_fields_to_exclude(normalized_entity_type)
    uuid = entity_dict['uuid']
    public_entity = True

    # Only for Dataset or (Publication 2/17/23 ~Derek Furst)
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error("The entity of given id is not a Dataset or Publication")

    latest_revision_dict = {}

    # Only published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        public_entity = False
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required = True)

        latest_revision_dict = app_neo4j_queries.get_dataset_latest_revision(neo4j_driver_instance, uuid)
    else:
        # Default to the latest "public" revision dataset
        # when no token or not a valid HuBMAP-Read token
        latest_revision_dict = app_neo4j_queries.get_dataset_latest_revision(neo4j_driver_instance, uuid, public = True)

        # Send back the real latest revision dataset if a valid HuBMAP-Read token presents
        if user_in_hubmap_read_group(request):
            latest_revision_dict = app_neo4j_queries.get_dataset_latest_revision(neo4j_driver_instance, uuid)

    # We'll need to return all the properties including those
    # generated by `on_read_trigger` to have a complete result
    # E.g., the 'previous_revision_uuid'
    # Here we skip the 'next_revision_uuid' property becase when the "public" latest revision dataset
    # is not the real latest revision, we don't want the users to see it
    properties_to_skip = [
        'next_revision_uuid'
    ]

    # On entity retrieval, the 'on_read_trigger' doesn't really need a token
    complete_dict = schema_manager.get_complete_entity_result(token, latest_revision_dict, properties_to_skip)

    # Also normalize the result based on schema
    final_result = schema_manager.normalize_entity_result_for_response(complete_dict)
    if user_in_hubmap_read_group(request) and public_entity:
        final_result = schema_manager.exclude_properties_from_response(fields_to_exclude, final_result)
    # Response with the dict
    return jsonify(final_result)


"""
Retrive the calculated revision number of a Dataset

The calculated revision is number is based on the [:REVISION_OF] relationships 
to the oldest dataset in a revision chain. 
Where the oldest dataset = 1 and each newer version is incremented by one (1, 2, 3 ...)

Public/Consortium access rules apply, if is for a non-public dataset 
and no token or a token without membership in HuBMAP-Read group is sent with the request 
then a 403 response should be returned.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 

Returns
-------
int
    The calculated revision number
"""
@app.route('/datasets/<id>/revision', methods = ['GET'])
def get_dataset_revision_number(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset (and publication 2/17/23)
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error("The entity of given id is not a Dataset or Publication")

    # Only published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required = True)

    # By now, either the entity is public accessible or
    # the user token has the correct access level
    revision_number = app_neo4j_queries.get_dataset_revision_number(neo4j_driver_instance, entity_dict['uuid'])

    # Response with the integer
    return jsonify(revision_number)


# """
# Retrieve a list of all multi revisions of a dataset from the id of any dataset in the chain.
# E.g: If there are 5 revisions, and the id for revision 4 is given, a list of revisions
# 1-5 will be returned in reverse order (newest first). Non-public access is only required to
# retrieve information on non-published datasets. Output will be a list of dictionaries. Each dictionary
# contains the dataset revision number and its list of uuids. Optionally, the full dataset can be included for each.
#
# By default, only the revision number and uuids are included. To include the full dataset, the query
# parameter "include_dataset" can be given with the value of "true". If this parameter is not included or
# is set to false, the dataset will not be included. For example, to include the full datasets for each revision,
# use '/datasets/<id>/multi-revisions?include_dataset=true'. To omit the datasets, either set include_dataset=false, or
# simply do not include this parameter.
#
# Parameters
# ----------
# id : str
#     The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target dataset
#
# Returns
# -------
# list
#     The list of revision datasets
# """
# @app.route('/entities/<id>/multi-revisions', methods=['GET'])
# @app.route('/datasets/<id>/multi-revisions', methods=['GET'])
# def get_multi_revisions_list(id):
#     # By default, do not return dataset. Only return dataset if include_dataset is true
#     property_key = 'uuid'
#     if bool(request.args):
#         include_dataset = request.args.get('include_dataset')
#         if (include_dataset is not None) and (include_dataset.lower() == 'true'):
#             property_key = None
#     # Token is not required, but if an invalid token provided,
#     # we need to tell the client with a 401 error
#     validate_token_if_auth_header_exists(request)
#
#     # Use the internal token to query the target entity
#     # since public entities don't require user token
#     token = get_internal_token()
#
#     # Query target entity against uuid-api and neo4j and return as a dict if exists
#     entity_dict = query_target_entity(id, token)
#     normalized_entity_type = entity_dict['entity_type']
#
#     # Only for Dataset
#     if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
#         abort_bad_req("The entity is not a Dataset. Found entity type:" + normalized_entity_type)
#
#     # Only published/public datasets don't require token
#     if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
#         # Token is required and the user must belong to HuBMAP-READ group
#         token = get_user_token(request, non_public_access_required=True)
#
#     # By now, either the entity is public accessible or
#     # the user token has the correct access level
#     # Get the all the sorted (DESC based on creation timestamp) revisions
#     sorted_revisions_list = app_neo4j_queries.get_sorted_multi_revisions(neo4j_driver_instance, entity_dict['uuid'],
#                                                                          fetch_all=user_in_hubmap_read_group(request),
#                                                                          property_key=property_key)
#
#     # Skip some of the properties that are time-consuming to generate via triggers
#     properties_to_skip = [
#         'direct_ancestors',
#         'collections',
#         'upload',
#         'title'
#     ]
#
#     normalized_revisions_list = []
#     sorted_revisions_list_merged = sorted_revisions_list[0] + sorted_revisions_list[1][::-1]
#
#     if property_key is None:
#         for revision in sorted_revisions_list_merged:
#             complete_revision_list = schema_manager.get_complete_entities_list(token, revision, properties_to_skip)
#             normal = schema_manager.normalize_entities_list_for_response(complete_revision_list)
#             normalized_revisions_list.append(normal)
#     else:
#         normalized_revisions_list = sorted_revisions_list_merged
#
#     # Now all we need to do is to compose the result list
#     results = []
#     revision_number = len(normalized_revisions_list)
#     for revision in normalized_revisions_list:
#         result = {
#             'revision_number': revision_number,
#             'uuids': revision
#         }
#         results.append(result)
#         revision_number -= 1
#
#     return jsonify(results)


"""
Retract a published dataset with a retraction reason and sub status

Takes as input a json body with required fields "retracted_reason" and "sub_status".
Authorization handled by gateway. Only token of HuBMAP-Data-Admin group can use this call. 

Technically, the same can be achieved by making a PUT call to the generic entity update endpoint
with using a HuBMAP-Data-Admin group token. But doing this is strongly discouraged because we'll
need to add more validators to ensure when "retracted_reason" is provided, there must be a 
"sub_status" filed and vise versa. So consider this call a special use case of entity update.

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target dataset 

Returns
-------
dict
    The updated dataset details
"""
@app.route('/datasets/<id>/retract', methods=['PUT'])
def retract_dataset(id):
    if READ_ONLY_MODE:
        forbidden_error("Access not granted when entity-api in READ-ONLY mode")
        
    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Normalize user provided status
    if "sub_status" in json_data_dict:
        normalized_status = schema_manager.normalize_status(json_data_dict["sub_status"])
        json_data_dict["sub_status"] = normalized_status

    # Use beblow application-level validations to avoid complicating schema validators
    # The 'retraction_reason' and `sub_status` are the only required/allowed fields. No other fields allowed.
    # Must enforce this rule otherwise we'll need to run after update triggers if any other fields
    # get passed in (which should be done using the generic entity update call)
    if 'retraction_reason' not in json_data_dict:
        bad_request_error("Missing required field: retraction_reason")

    if 'sub_status' not in json_data_dict:
        bad_request_error("Missing required field: sub_status")

    if len(json_data_dict) > 2:
        bad_request_error("Only retraction_reason and sub_status are allowed fields")

    # Must be a HuBMAP-Data-Admin group token
    token = get_user_token(request)

    # Retrieves the neo4j data for a given entity based on the id supplied.
    # The normalized entity-type from this entity is checked to be a dataset
    # If the entity is not a dataset and the dataset is not published, cannot retract
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # A bit more application-level validation
    # Adding publication to validation 2/17/23 ~Derek Furst
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error("The entity of given id is not a Dataset or Publication")

    # Validate request json against the yaml schema
    # The given value of `sub_status` is being validated at this step
    try:
        schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_type, existing_entity_dict = entity_dict)
    except schema_errors.SchemaValidationException as e:
        # No need to log the validation errors
        bad_request_error(str(e))

    # Execute property level validators defined in schema yaml before entity property update
    try:
        schema_manager.execute_property_level_validators('before_property_update_validators', normalized_entity_type, request, entity_dict, json_data_dict)
    except (schema_errors.MissingApplicationHeaderException,
            schema_errors.InvalidApplicationHeaderException,
            KeyError,
            ValueError) as e:
        bad_request_error(e)

    # No need to call after_update() afterwards because retraction doesn't call any after_update_trigger methods
    merged_updated_dict = update_entity_details(request, normalized_entity_type, token, json_data_dict, entity_dict)

    complete_dict = schema_manager.get_complete_entity_result(token, merged_updated_dict)

    # Will also filter the result based on schema
    normalized_complete_dict = schema_manager.normalize_entity_result_for_response(complete_dict)

    # Also reindex the updated entity node in elasticsearch via search-api
    reindex_entity(entity_dict['uuid'], token)

    return jsonify(normalized_complete_dict)


"""
Retrieve a list of all revisions of a dataset from the id of any dataset in the chain. 
E.g: If there are 5 revisions, and the id for revision 4 is given, a list of revisions
1-5 will be returned in reverse order (newest first). Non-public access is only required to 
retrieve information on non-published datasets. Output will be a list of dictionaries. Each dictionary
contains the dataset revision number and its uuid. Optionally, the full dataset can be included for each.

By default, only the revision number and uuid is included. To include the full dataset, the query 
parameter "include_dataset" can be given with the value of "true". If this parameter is not included or 
is set to false, the dataset will not be included. For example, to include the full datasets for each revision,
use '/datasets/<id>/revisions?include_dataset=true'. To omit the datasets, either set include_dataset=false, or
simply do not include this parameter. 

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target dataset 

Returns
-------
list
    The list of revision datasets
"""
@app.route('/entities/<id>/revisions', methods=['GET'])
@app.route('/datasets/<id>/revisions', methods=['GET'])
def get_revisions_list(id):
    # By default, do not return dataset. Only return dataset if return_dataset is true
    show_dataset = False
    if bool(request.args):
        include_dataset = request.args.get('include_dataset')
        if (include_dataset is not None) and (include_dataset.lower() == 'true'):
            show_dataset = True
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error("The entity is not a Dataset. Found entity type:" + normalized_entity_type)

    # Only published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or
    # the user token has the correct access level
    # Get the all the sorted (DESC based on creation timestamp) revisions
    sorted_revisions_list = app_neo4j_queries.get_sorted_revisions(neo4j_driver_instance, entity_dict['uuid'])

    # Skip some of the properties that are time-consuming to generate via triggers
    properties_to_skip = [
        'direct_ancestors',
        'collections',
        'upload',
        'title'
    ]
    complete_revisions_list = schema_manager.get_complete_entities_list(token, sorted_revisions_list, properties_to_skip)
    normalized_revisions_list = schema_manager.normalize_entities_list_for_response(complete_revisions_list)
    fields_to_exclude = schema_manager.get_fields_to_exclude(normalized_entity_type)
    # Only check the very last revision (the first revision dict since normalized_revisions_list is already sorted DESC)
    # to determine if send it back or not
    is_in_read_group = True
    if not user_in_hubmap_read_group(request):
        is_in_read_group = False
        latest_revision = normalized_revisions_list[0]

        if latest_revision['status'].lower() != DATASET_STATUS_PUBLISHED:
            normalized_revisions_list.pop(0)

            # Also hide the 'next_revision_uuid' of the second last revision from response
            if 'next_revision_uuid' in normalized_revisions_list[0]:
                normalized_revisions_list[0].pop('next_revision_uuid')

    # Now all we need to do is to compose the result list
    results = []
    revision_number = len(normalized_revisions_list)
    for revision in normalized_revisions_list:
        result = {
            'revision_number': revision_number,
            'uuid': revision['uuid']
        }
        if show_dataset:
            result['dataset'] = revision
            if not is_in_read_group:
                result['dataset'] = schema_manager.exclude_properties_from_response(fields_to_exclude, revision)
        results.append(result)
        revision_number -= 1

    return jsonify(results)


"""
Get all organs associated with a given dataset

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    a list of all the organs associated with the target dataset
"""
@app.route('/datasets/<id>/organs', methods=['GET'])
def get_associated_organs_from_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error("The entity of given id is not a Dataset or Publication")
    excluded_fields = schema_manager.get_fields_to_exclude('Sample')
    public_entity = True
    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        public_entity = False
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or
    # the user token has the correct access level
    associated_organs = app_neo4j_queries.get_associated_organs_from_dataset(neo4j_driver_instance, entity_dict['uuid'])

    # If there are zero items in the list associated_organs, then there are no associated
    # Organs and a 404 will be returned.
    if len(associated_organs) < 1:
        not_found_error("the dataset does not have any associated organs")

    complete_entities_list = schema_manager.get_complete_entities_list(token, associated_organs)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
    if public_entity and not user_in_hubmap_read_group(request):
        filtered_organs_list = []
        for organ in final_result:
            filtered_organs_list.append(schema_manager.exclude_properties_from_response(excluded_fields, organ))
        final_result = filtered_organs_list

    return jsonify(final_result)


"""
Get all samples associated with a given dataset

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    a list of all the samples associated with the target dataset
"""
@app.route('/datasets/<id>/samples', methods=['GET'])
def get_associated_samples_from_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    excluded_fields = schema_manager.get_fields_to_exclude('Sample')
    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error("The entity of given id is not a Dataset or Publication")
    public_entity = True
    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to HuBMAP-READ group
        public_entity = False
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or the user token has the correct access level
    associated_samples = app_neo4j_queries.get_associated_samples_from_dataset(neo4j_driver_instance, entity_dict['uuid'])

    # If there are zero items in the list associated_samples, then there are no associated
    # samples and a 404 will be returned.
    if len(associated_samples) < 1:
        not_found_error("the dataset does not have any associated samples")

    complete_entities_list = schema_manager.get_complete_entities_list(token, associated_samples)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
    if public_entity and not user_in_hubmap_read_group(request):
        filtered_sample_list = []
        for sample in final_result:
            filtered_sample_list.append(schema_manager.exclude_properties_from_response(excluded_fields, sample))
        final_result = filtered_sample_list
    return jsonify(final_result)


"""
Get all donors associated with a given dataset

The gateway treats this endpoint as public accessible

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity

Returns
-------
json
    a list of all the donors associated with the target dataset
"""
@app.route('/datasets/<id>/donors', methods=['GET'])
def get_associated_donors_from_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']
    excluded_fields = schema_manager.get_fields_to_exclude('Donor')

    # Only for Dataset
    if not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
        bad_request_error("The entity of given id is not a Dataset or Publication")
    public_entity = True
    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        public_entity = False
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required=True)

    # By now, either the entity is public accessible or the user token has the correct access level
    associated_donors = app_neo4j_queries.get_associated_donors_from_dataset(neo4j_driver_instance, entity_dict['uuid'])

    # If there are zero items in the list associated_donors, then there are no associated
    # donors and a 404 will be returned.
    if len(associated_donors) < 1:
        not_found_error("the dataset does not have any associated donors")

    complete_entities_list = schema_manager.get_complete_entities_list(token, associated_donors)

    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(complete_entities_list)
    if public_entity and not user_in_hubmap_read_group(request):
        filtered_donor_list = []
        for donor in final_result:
            filtered_donor_list.append(schema_manager.exclude_properties_from_response(excluded_fields, donor))
        final_result = filtered_donor_list
    return jsonify(final_result)


"""
Get the complete provenance info for a given dataset

Authentication
-------
No token is required, however if a token is given it must be valid or an error will be raised. If no token with HuBMAP
Read Group access is given, only datasets designated as "published" will be returned

Query Parameters
-------
format : string
        Designates the output format of the returned data. Accepted values are "json" and "tsv". If none provided, by 
        default will return a tsv.

Path Parameters
-------
id : string
    A HuBMAP_ID or UUID for a dataset. If an invalid dataset id is given, an error will be raised    

Returns
-------
If the response is small enough to be returned directly through the gateway, an HTTP 200 response code will be
returned.  If the response is too large to pass through the gateway, and HTTP 303 response code will be returned, and
the response body will contain a URL to an AWS S3 Object.  The Object must be retrieved by following the URL before
it expires.

json
    A dictionary of the Datatset's provenance info
tsv
    A text file of tab separated prov info values for the Dataset, including a row of column headings.
"""
@app.route('/datasets/<id>/prov-info', methods=['GET'])
def get_prov_info_for_dataset(id):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)
    organ_types_dict = schema_manager.get_organ_types()
    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    entity_dict = query_target_entity(id, token)
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset
    if normalized_entity_type != 'Dataset':
        bad_request_error("The entity of given id is not a Dataset")

    # published/public datasets don't require token
    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        # Token is required and the user must belong to HuBMAP-READ group
        token = get_user_token(request, non_public_access_required=True)

    return_json = False
    dataset_prov_list = []
    include_samples = []
    if bool(request.args):
        return_format = request.args.get('format')
        if (return_format is not None) and (return_format.lower() == 'json'):
            return_json = True
        include_samples_req = request.args.get('include_samples')
        if (include_samples_req is not None):
            include_samples = include_samples_req.lower().split(',')

    HEADER_DATASET_UUID = 'dataset_uuid'
    HEADER_DATASET_HUBMAP_ID = 'dataset_hubmap_id'
    HEADER_DATASET_STATUS = 'dataset_status'
    HEADER_DATASET_GROUP_NAME = 'dataset_group_name'
    HEADER_DATASET_GROUP_UUID = 'dataset_group_uuid'
    HEADER_DATASET_DATE_TIME_CREATED = 'dataset_date_time_created'
    HEADER_DATASET_CREATED_BY_EMAIL = 'dataset_created_by_email'
    HEADER_DATASET_DATE_TIME_MODIFIED = 'dataset_date_time_modified'
    HEADER_DATASET_MODIFIED_BY_EMAIL = 'dataset_modified_by_email'
    HEADER_DATASET_LAB_ID = 'lab_id_or_name'
    HEADER_DATASET_DATASET_TYPE = 'dataset_dataset_type'
    HEADER_DATASET_PORTAL_URL = 'dataset_portal_url'
    HEADER_DATASET_SAMPLES = 'dataset_samples'
    HEADER_FIRST_SAMPLE_HUBMAP_ID = 'first_sample_hubmap_id'
    HEADER_FIRST_SAMPLE_SUBMISSION_ID = 'first_sample_submission_id'
    HEADER_FIRST_SAMPLE_UUID = 'first_sample_uuid'
    HEADER_FIRST_SAMPLE_TYPE = 'first_sample_type'
    HEADER_FIRST_SAMPLE_PORTAL_URL = 'first_sample_portal_url'
    HEADER_ORGAN_HUBMAP_ID = 'organ_hubmap_id'
    HEADER_ORGAN_SUBMISSION_ID = 'organ_submission_id'
    HEADER_ORGAN_UUID = 'organ_uuid'
    HEADER_ORGAN_TYPE = 'organ_type'
    HEADER_DONOR_HUBMAP_ID = 'donor_hubmap_id'
    HEADER_DONOR_SUBMISSION_ID = 'donor_submission_id'
    HEADER_DONOR_UUID = 'donor_uuid'
    HEADER_DONOR_GROUP_NAME = 'donor_group_name'
    HEADER_RUI_LOCATION_HUBMAP_ID = 'rui_location_hubmap_id'
    HEADER_RUI_LOCATION_SUBMISSION_ID = 'rui_location_submission_id'
    HEADER_RUI_LOCATION_UUID = 'rui_location_uuid'
    HEADER_SAMPLE_METADATA_HUBMAP_ID = 'sample_metadata_hubmap_id'
    HEADER_SAMPLE_METADATA_SUBMISSION_ID = 'sample_metadata_submission_id'
    HEADER_SAMPLE_METADATA_UUID = 'sample_metadata_uuid'
    HEADER_PROCESSED_DATASET_UUID = 'processed_dataset_uuid'
    HEADER_PROCESSED_DATASET_HUBMAP_ID = 'processed_dataset_hubmap_id'
    HEADER_PROCESSED_DATASET_STATUS = 'processed_dataset_status'
    HEADER_PROCESSED_DATASET_PORTAL_URL = 'processed_dataset_portal_url'

    headers = [
        HEADER_DATASET_UUID, HEADER_DATASET_HUBMAP_ID, HEADER_DATASET_STATUS, HEADER_DATASET_GROUP_NAME,
        HEADER_DATASET_GROUP_UUID, HEADER_DATASET_DATE_TIME_CREATED, HEADER_DATASET_CREATED_BY_EMAIL,
        HEADER_DATASET_DATE_TIME_MODIFIED, HEADER_DATASET_MODIFIED_BY_EMAIL, HEADER_DATASET_LAB_ID,
        HEADER_DATASET_DATASET_TYPE, HEADER_DATASET_PORTAL_URL, HEADER_FIRST_SAMPLE_HUBMAP_ID,
        HEADER_FIRST_SAMPLE_SUBMISSION_ID, HEADER_FIRST_SAMPLE_UUID, HEADER_FIRST_SAMPLE_TYPE,
        HEADER_FIRST_SAMPLE_PORTAL_URL, HEADER_ORGAN_HUBMAP_ID, HEADER_ORGAN_SUBMISSION_ID, HEADER_ORGAN_UUID,
        HEADER_ORGAN_TYPE, HEADER_DONOR_HUBMAP_ID, HEADER_DONOR_SUBMISSION_ID, HEADER_DONOR_UUID,
        HEADER_DONOR_GROUP_NAME, HEADER_RUI_LOCATION_HUBMAP_ID, HEADER_RUI_LOCATION_SUBMISSION_ID,
        HEADER_RUI_LOCATION_UUID, HEADER_SAMPLE_METADATA_HUBMAP_ID, HEADER_SAMPLE_METADATA_SUBMISSION_ID,
        HEADER_SAMPLE_METADATA_UUID, HEADER_PROCESSED_DATASET_UUID, HEADER_PROCESSED_DATASET_HUBMAP_ID,
        HEADER_PROCESSED_DATASET_STATUS, HEADER_PROCESSED_DATASET_PORTAL_URL, HEADER_DATASET_SAMPLES
    ]

    hubmap_ids = schema_manager.get_hubmap_ids(id)

    # Get the target uuid if all good
    uuid = hubmap_ids['hm_uuid']
    dataset = app_neo4j_queries.get_individual_prov_info(neo4j_driver_instance, uuid)
    if dataset is None:
        bad_request_error("Query For this Dataset Returned no Records. Make sure this is a Primary Dataset")
    internal_dict = collections.OrderedDict()
    internal_dict[HEADER_DATASET_HUBMAP_ID] = dataset['hubmap_id']
    internal_dict[HEADER_DATASET_UUID] = dataset['uuid']
    internal_dict[HEADER_DATASET_STATUS] = dataset['status']
    internal_dict[HEADER_DATASET_GROUP_NAME] = dataset['group_name']
    internal_dict[HEADER_DATASET_GROUP_UUID] = dataset['group_uuid']
    internal_dict[HEADER_DATASET_DATE_TIME_CREATED] = str(datetime.fromtimestamp(int(dataset['created_timestamp'] / 1000.0)))
    internal_dict[HEADER_DATASET_CREATED_BY_EMAIL] = dataset['created_by_user_email']
    internal_dict[HEADER_DATASET_DATE_TIME_MODIFIED] = str(datetime.fromtimestamp(int(dataset['last_modified_timestamp'] / 1000.0)))
    internal_dict[HEADER_DATASET_MODIFIED_BY_EMAIL] = dataset['last_modified_user_email']
    internal_dict[HEADER_DATASET_LAB_ID] = dataset['lab_dataset_id']
    internal_dict[HEADER_DATASET_DATASET_TYPE] = dataset['dataset_dataset_type']

    internal_dict[HEADER_DATASET_PORTAL_URL] = app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'dataset').replace(
        '<identifier>', dataset['uuid'])
    if dataset['first_sample'] is not None:
        first_sample_hubmap_id_list = []
        first_sample_submission_id_list = []
        first_sample_uuid_list = []
        first_sample_type_list = []
        first_sample_portal_url_list = []
        for item in dataset['first_sample']:
            first_sample_hubmap_id_list.append(item['hubmap_id'])
            first_sample_submission_id_list.append(item['submission_id'])
            first_sample_uuid_list.append(item['uuid'])
            first_sample_type_list.append(item['sample_category'])

            first_sample_portal_url_list.append(
                app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'sample').replace('<identifier>', item['uuid']))
        internal_dict[HEADER_FIRST_SAMPLE_HUBMAP_ID] = first_sample_hubmap_id_list
        internal_dict[HEADER_FIRST_SAMPLE_SUBMISSION_ID] = first_sample_submission_id_list
        internal_dict[HEADER_FIRST_SAMPLE_UUID] = first_sample_uuid_list
        internal_dict[HEADER_FIRST_SAMPLE_TYPE] = first_sample_type_list
        internal_dict[HEADER_FIRST_SAMPLE_PORTAL_URL] = first_sample_portal_url_list
        if return_json is False:
            internal_dict[HEADER_FIRST_SAMPLE_HUBMAP_ID] = ",".join(first_sample_hubmap_id_list)
            internal_dict[HEADER_FIRST_SAMPLE_SUBMISSION_ID] = ",".join(first_sample_submission_id_list)
            internal_dict[HEADER_FIRST_SAMPLE_UUID] = ",".join(first_sample_uuid_list)
            internal_dict[HEADER_FIRST_SAMPLE_TYPE] = ",".join(first_sample_type_list)
            internal_dict[HEADER_FIRST_SAMPLE_PORTAL_URL] = ",".join(first_sample_portal_url_list)
    if dataset['distinct_organ'] is not None:
        distinct_organ_hubmap_id_list = []
        distinct_organ_submission_id_list = []
        distinct_organ_uuid_list = []
        distinct_organ_type_list = []
        for item in dataset['distinct_organ']:
            distinct_organ_hubmap_id_list.append(item['hubmap_id'])
            distinct_organ_submission_id_list.append(item['submission_id'])
            distinct_organ_uuid_list.append(item['uuid'])

            organ_code = item['organ'].upper()
            validate_organ_code(organ_code)

            distinct_organ_type_list.append(organ_types_dict[organ_code].lower())
        internal_dict[HEADER_ORGAN_HUBMAP_ID] = distinct_organ_hubmap_id_list
        internal_dict[HEADER_ORGAN_SUBMISSION_ID] = distinct_organ_submission_id_list
        internal_dict[HEADER_ORGAN_UUID] = distinct_organ_uuid_list
        internal_dict[HEADER_ORGAN_TYPE] = distinct_organ_type_list
        if return_json is False:
            internal_dict[HEADER_ORGAN_HUBMAP_ID] = ",".join(distinct_organ_hubmap_id_list)
            internal_dict[HEADER_ORGAN_SUBMISSION_ID] = ",".join(distinct_organ_submission_id_list)
            internal_dict[HEADER_ORGAN_UUID] = ",".join(distinct_organ_uuid_list)
            internal_dict[HEADER_ORGAN_TYPE] = ",".join(distinct_organ_type_list)
    if dataset['distinct_donor'] is not None:
        distinct_donor_hubmap_id_list = []
        distinct_donor_submission_id_list = []
        distinct_donor_uuid_list = []
        distinct_donor_group_name_list = []
        for item in dataset['distinct_donor']:
            distinct_donor_hubmap_id_list.append(item['hubmap_id'])
            distinct_donor_submission_id_list.append(item['submission_id'])
            distinct_donor_uuid_list.append(item['uuid'])
            distinct_donor_group_name_list.append(item['group_name'])
        internal_dict[HEADER_DONOR_HUBMAP_ID] = distinct_donor_hubmap_id_list
        internal_dict[HEADER_DONOR_SUBMISSION_ID] = distinct_donor_submission_id_list
        internal_dict[HEADER_DONOR_UUID] = distinct_donor_uuid_list
        internal_dict[HEADER_DONOR_GROUP_NAME] = distinct_donor_group_name_list
        if return_json is False:
            internal_dict[HEADER_DONOR_HUBMAP_ID] = ",".join(distinct_donor_hubmap_id_list)
            internal_dict[HEADER_DONOR_SUBMISSION_ID] = ",".join(distinct_donor_submission_id_list)
            internal_dict[HEADER_DONOR_UUID] = ",".join(distinct_donor_uuid_list)
            internal_dict[HEADER_DONOR_GROUP_NAME] = ",".join(distinct_donor_group_name_list)
    if dataset['distinct_rui_sample'] is not None:
        rui_location_hubmap_id_list = []
        rui_location_submission_id_list = []
        rui_location_uuid_list = []
        for item in dataset['distinct_rui_sample']:
            rui_location_hubmap_id_list.append(item['hubmap_id'])
            rui_location_submission_id_list.append(item['submission_id'])
            rui_location_uuid_list.append(item['uuid'])
        internal_dict[HEADER_RUI_LOCATION_HUBMAP_ID] = rui_location_hubmap_id_list
        internal_dict[HEADER_RUI_LOCATION_SUBMISSION_ID] = rui_location_submission_id_list
        internal_dict[HEADER_RUI_LOCATION_UUID] = rui_location_uuid_list
        if return_json is False:
            internal_dict[HEADER_RUI_LOCATION_HUBMAP_ID] = ",".join(rui_location_hubmap_id_list)
            internal_dict[HEADER_RUI_LOCATION_SUBMISSION_ID] = ",".join(rui_location_submission_id_list)
            internal_dict[HEADER_RUI_LOCATION_UUID] = ",".join(rui_location_uuid_list)
    if dataset['distinct_metasample'] is not None:
        metasample_hubmap_id_list = []
        metasample_submission_id_list = []
        metasample_uuid_list = []
        for item in dataset['distinct_metasample']:
            metasample_hubmap_id_list.append(item['hubmap_id'])
            metasample_submission_id_list.append(item['submission_id'])
            metasample_uuid_list.append(item['uuid'])
        internal_dict[HEADER_SAMPLE_METADATA_HUBMAP_ID] = metasample_hubmap_id_list
        internal_dict[HEADER_SAMPLE_METADATA_SUBMISSION_ID] = metasample_submission_id_list
        internal_dict[HEADER_SAMPLE_METADATA_UUID] = metasample_uuid_list
        if return_json is False:
            internal_dict[HEADER_SAMPLE_METADATA_HUBMAP_ID] = ",".join(metasample_hubmap_id_list)
            internal_dict[HEADER_SAMPLE_METADATA_SUBMISSION_ID] = ",".join(metasample_submission_id_list)
            internal_dict[HEADER_SAMPLE_METADATA_UUID] = ",".join(metasample_uuid_list)

    # processed_dataset properties are retrived from its own dictionary
    if dataset['processed_dataset'] is not None:
        processed_dataset_uuid_list = []
        processed_dataset_hubmap_id_list = []
        processed_dataset_status_list = []
        processed_dataset_portal_url_list = []
        for item in dataset['processed_dataset']:
            processed_dataset_uuid_list.append(item['uuid'])
            processed_dataset_hubmap_id_list.append(item['hubmap_id'])
            processed_dataset_status_list.append(item['status'])
            processed_dataset_portal_url_list.append(
                app.config['DOI_REDIRECT_URL'].replace('<entity_type>', 'dataset').replace('<identifier>',
                                                                                           item['uuid']))
        internal_dict[HEADER_PROCESSED_DATASET_UUID] = processed_dataset_uuid_list
        internal_dict[HEADER_PROCESSED_DATASET_HUBMAP_ID] = processed_dataset_hubmap_id_list
        internal_dict[HEADER_PROCESSED_DATASET_STATUS] = processed_dataset_status_list
        internal_dict[HEADER_PROCESSED_DATASET_PORTAL_URL] = processed_dataset_portal_url_list
        if return_json is False:
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_uuid_list)
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_hubmap_id_list)
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_status_list)
            internal_dict[HEADER_PROCESSED_DATASET_UUID] = ",".join(processed_dataset_portal_url_list)

    if include_samples:
        # Get provenance non-organ Samples for the Dataset all the way back to each Donor, to supplement
        # the "first sample" data stashed in internal_dict in the previous section.
        dataset_samples = app_neo4j_queries.get_all_dataset_samples(neo4j_driver_instance, uuid)

        if 'all' in include_samples:
            internal_dict[HEADER_DATASET_SAMPLES] = dataset_samples
        else:
            requested_samples = {}
            for uuid in dataset_samples.keys():
                if dataset_samples[uuid]['sample_category'] in include_samples:
                    requested_samples[uuid] = dataset_samples[uuid]
            internal_dict[HEADER_DATASET_SAMPLES] = requested_samples

    dataset_prov_list.append(internal_dict)

    # Establish a string for the Response which can be checked to
    # see if it is small enough to return directly or must be stashed in S3.
    if return_json:
        resp_body = json.dumps(dataset_prov_list).encode('utf-8')
    else:
        # If return_json is false, convert the data to a TSV
        new_tsv_file = StringIO()
        writer = csv.DictWriter(new_tsv_file, fieldnames=headers, delimiter='\t')
        writer.writeheader()
        writer.writerows(dataset_prov_list)
        new_tsv_file.seek(0)
        resp_body = new_tsv_file.read()

    # Check the size of what is to be returned through the AWS Gateway, and replace it with
    # a response that links to an Object in the AWS S3 Bucket, if appropriate.
    try:
        s3_url = anS3Worker.stash_response_body_if_big(resp_body)
        if s3_url is not None:
            return Response(response=s3_url
                            , status=303)  # See Other
    except Exception as s3exception:
        logger.error(f"Error using anS3Worker to handle len(resp_body)="
                     f"{len(resp_body)}.")
        logger.error(s3exception, exc_info=True)
        return Response(response=f"Unexpected error storing large results in S3. See logs."
                        , status=500)

    # Return a regular response through the AWS Gateway
    if return_json:
        return jsonify(dataset_prov_list[0])
    else:
        # Return the TSV as an attachment, since it will is small enough to fit through the AWS Gateway.
        new_tsv_file.seek(0)
        output = Response(new_tsv_file, mimetype='text/tsv')
        output.headers['Content-Disposition'] = 'attachment; filename=prov-info.tsv'
        return output


"""
Get the information needed to generate the sankey on software-docs as a json.

Authentication
-------
No token is required or checked. The information returned is what is displayed in the public sankey

Query Parameters
-------
N/A

Path Parameters
-------
N/A

Returns
-------
json
    a json array. Each item in the array corresponds to a dataset. Each dataset has the values: dataset_group_name, 
    organ_type, dataset_data_types, and dataset_status, each of which is a string. # TODO-integrate dataset_dataset_type to documentation.

"""
@app.route('/datasets/sankey_data', methods=['GET'])
def sankey_data():
    # String constants
    HEADER_DATASET_GROUP_NAME = 'dataset_group_name'
    HEADER_ORGAN_TYPE = 'organ_type'
    HEADER_DATASET_DATASET_TYPE = 'dataset_dataset_type'
    HEADER_DATASET_STATUS = 'dataset_status'

    public_only = False

    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)
    try:
        token = get_user_token(request, non_public_access_required=True)
    except Exception:
        public_only = True

    # Parsing the organ types yaml has to be done here rather than calling schema.schema_triggers.get_organ_description
    # because that would require using a urllib request for each dataset
    organ_types_dict = schema_manager.get_organ_types()

    # Instantiation of the list dataset_sankey_list
    dataset_sankey_list = []

    cache_key = f'{MEMCACHED_PREFIX}sankey'

    if MEMCACHED_MODE:
        if memcached_client_instance.get(cache_key) is not None:
            dataset_sankey_list = memcached_client_instance.get(cache_key)

    if not dataset_sankey_list:
        if MEMCACHED_MODE:
            logger.info(f'Sankey data cache not found or expired. Making a new data fetch at time {datetime.now()}')

        # Call to app_neo4j_queries to prepare and execute the database query
        sankey_info = app_neo4j_queries.get_sankey_info(neo4j_driver_instance, public_only)
        for dataset in sankey_info:
            internal_dict = collections.OrderedDict()
            internal_dict[HEADER_DATASET_GROUP_NAME] = dataset[HEADER_DATASET_GROUP_NAME]
            organ_list = []
            for organ in dataset[HEADER_ORGAN_TYPE]:
                organ_code = organ.upper()
                validate_organ_code(organ_code)
                organ_type = organ_types_dict[organ_code].lower()
                organ_list.append(organ_type)
            internal_dict[HEADER_ORGAN_TYPE] = organ_list

            internal_dict[HEADER_DATASET_DATASET_TYPE] = dataset[HEADER_DATASET_DATASET_TYPE]

            # Replace applicable Group Name and Data type with the value needed for the sankey via the mapping_dict
            internal_dict[HEADER_DATASET_STATUS] = dataset['dataset_status']
            # if internal_dict[HEADER_DATASET_GROUP_NAME] in mapping_dict.keys():
            #     internal_dict[HEADER_DATASET_GROUP_NAME] = mapping_dict[internal_dict[HEADER_DATASET_GROUP_NAME]]

            # Each dataset's dictionary is added to the list to be returned
            dataset_sankey_list.append(internal_dict)

        if MEMCACHED_MODE:
            # Cache the result
            memcached_client_instance.set(cache_key, dataset_sankey_list, expire = SchemaConstants.MEMCACHED_TTL)
    else:
        logger.info(f'Using the cached sankey data at time {datetime.now()}')

    return jsonify(dataset_sankey_list)


"""
Retrieve all unpublished datasets (datasets with status value other than 'Published' or 'Hold')

Authentication
-------
Requires HuBMAP Read-Group access. Authenticated in the gateway 

Query Parameters
-------
    format : string
        Determines the output format of the data. Allowable values are ("tsv"|"json")

Returns
-------
json
    an array of each unpublished dataset.
    fields: ("data_types", "donor_hubmap_id", "donor_submission_id", "hubmap_id", "organ", "organization", 
             "provider_experiment_id", "uuid")  # TODO-integrate dataset_dataset_type to documentation.
tsv
    a text/tab-seperated-value document including each unpublished dataset.
    fields: ("data_types", "donor_hubmap_id", "donor_submission_id", "hubmap_id", "organ", "organization", 
             "provider_experiment_id", "uuid")  # TODO-integrate dataset_dataset_type to documentation.
"""
@app.route('/datasets/unpublished', methods=['GET'])
def unpublished():
    # String constraints
    HEADER_DATA_TYPES = "data_types" # TODO-eliminate when HEADER_DATASET_TYPE is required
    HEADER_DATASET_TYPE = 'dataset_type'
    HEADER_ORGANIZATION = "organization"
    HEADER_UUID = "uuid"
    HEADER_HUBMAP_ID = "hubmap_id"
    HEADER_ORGAN = "organ"
    HEADER_DONOR_HUBMAP_ID = "donor_hubmap_id"
    HEADER_SUBMISSION_ID = "donor_submission_id"
    HEADER_PROVIDER_EXPERIMENT_ID = "provider_experiment_id"

    # TODO-Eliminate HEADER_DATA_TYPES once HEADER_DATASET_TYPE is required.
    headers = [
        HEADER_DATA_TYPES, HEADER_DATASET_TYPE, HEADER_ORGANIZATION, HEADER_UUID, HEADER_HUBMAP_ID, HEADER_ORGAN, HEADER_DONOR_HUBMAP_ID,
        HEADER_SUBMISSION_ID, HEADER_PROVIDER_EXPERIMENT_ID
    ]

    # Processing and validating query parameters
    accepted_arguments = ['format']
    return_tsv = False
    if bool(request.args):
        for argument in request.args:
            if argument not in accepted_arguments:
                bad_request_error(f"{argument} is an unrecognized argument.")
        return_format = request.args.get('format')
        if return_format is not None:
            if return_format.lower() not in ['json', 'tsv']:
                bad_request_error(
                    "Invalid Format. Accepted formats are JSON and TSV. If no format is given, JSON will be the default")
            if return_format.lower() == 'tsv':
                return_tsv = True
    unpublished_info = app_neo4j_queries.get_unpublished(neo4j_driver_instance)
    if return_tsv:
        new_tsv_file = StringIO()
        writer = csv.DictWriter(new_tsv_file, fieldnames=headers, delimiter='\t')
        writer.writeheader()
        writer.writerows(unpublished_info)
        new_tsv_file.seek(0)
        output = Response(new_tsv_file, mimetype='text/tsv')
        output.headers['Content-Disposition'] = 'attachment; filename=unpublished-datasets.tsv'
        return output

    # if return_json is false, the data must be converted to be returned as a tsv
    else:
        return jsonify(unpublished_info)


"""
Retrieve uuids for associated dataset of given data_type which 
shares a sample ancestor of given dataset id

Returns
--------
json array
    List of uuids of all datasets (if any) of the specified data_type
     who share a sample ancestor with the dataset with the given id

Authorization
-------------
This endpoint is publicly accessible, however if a token is provided, 
it must be valid. If the given dataset uuid is for an unpublished dataset,
the user must be part of the HuBMAP-Read-Group. If not, a 403 will be raised.

Path Parameters
---------------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target dataset

Required Query Paramters
------------------------
data_type : str
    The data type to be searched for.
    
Optional Query Paramters
------------------------
search_depth : int
    The max number of generations of datasets to search for associated paired 
    dataset. This number is the number of generations between the shared sample
    ancestor and the target dataset (if any) rather than the starting dataset. 
    This number counts dataset generations and not activity nodes or any other 
    intermediate steps between 2 datasets. If no search_depth is given, the 
    search will traverse all descendants of the sample ancestor.  

If the associated datasets (if any exist) returned are unpublished, they    
"""
@app.route('/datasets/<id>/paired-dataset', methods=['GET'])
def paired_dataset(id):
    if request.headers.get('Authorization') is not None:
        try:
            user_token = auth_helper_instance.getAuthorizationTokens(request.headers)
        except Exception:
            msg = "Failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
            # Log the full stack trace, prepend a line with our message
            logger.exception(msg)
            internal_server_error(msg)
        # When the Authoriztion header provided but the user_token is a flask.Response instance,
        # it MUST be a 401 error with message.
        # That's how commons.auth_helper.getAuthorizationTokens() was designed
        if isinstance(user_token, Response):
            # We wrap the message in a json and send back to requester as 401 too
            # The Response.data returns binary string, need to decode
            unauthorized_error(user_token.get_data().decode())
        # Also check if the parased token is invalid or expired
        # Set the second paremeter as False to skip group check
        user_info = auth_helper_instance.getUserInfo(user_token, False)
        if isinstance(user_info, Response):
            unauthorized_error(user_info.get_data().decode())

    accepted_arguments = ['data_type', 'search_depth']
    if not bool(request.args):
        bad_request_error(f"'data_type' is a required argument")
    else:
        for argument in request.args:
            if argument not in accepted_arguments:
                bad_request_error(f"{argument} is an unrecognized argument.")
        if 'data_type' not in request.args:
            bad_request_error(f"'data_type' is a required argument")
        else:
            data_type = request.args.get('data_type')
        if 'search_depth' in request.args:
            try:
                search_depth = int(request.args.get('search_depth'))
            except ValueError:
                bad_request_error(f"'search_depth' must be an integer")
        else:
            search_depth = None
    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Query target entity against uuid-api and neo4j and return as a dict if exists
    # Then retrieve the allowable data access level (public, protected or consortium)
    # for the dataset and HuBMAP Component ID that the dataset belongs to
    entity_dict = query_target_entity(id, token)
    uuid = entity_dict['uuid']
    normalized_entity_type = entity_dict['entity_type']

    # Only for Dataset and Upload
    if normalized_entity_type != 'Dataset':
        bad_request_error("The target entity of the specified id is not a Dataset")

    if entity_dict['status'].lower() != DATASET_STATUS_PUBLISHED:
        if not user_in_hubmap_read_group(request):
            forbidden_error("Access not granted")

    paired_dataset = app_neo4j_queries.get_paired_dataset(neo4j_driver_instance, uuid, data_type, search_depth)
    out_list = []
    for result in paired_dataset:
        if user_in_hubmap_read_group(request) or result['status'].lower() == 'published':
            out_list.append(result['uuid'])
    if len(out_list) < 1:
        not_found_error(f"Search for paired datasets of type {data_type} for dataset with id {uuid} returned no results")
    else:
        return jsonify(out_list), 200


"""
Create multiple component datasets from a single Multi-Assay ancestor

Input
-----
json
    A json object with the fields: 
        creation_action
         - type: str
         - description: the action event that will describe the activity node. Allowed valuese are: "Multi-Assay Split"
        group_uuid
         - type: str
         - description: the group uuid for the new component datasets
        direct_ancestor_uuid
         - type: str
         - description: the uuid for the parent multi assay dataset
        datasets
         - type: list
         - description: the datasets to be created. Only difference between these and normal datasets are the field "dataset_link_abs_dir"

Returns
--------
json array
    List of the newly created datasets represented as dictionaries. 
"""
@app.route('/datasets/components', methods=['POST'])
def multiple_components():
    if READ_ONLY_MODE:
        forbidden_error("Access not granted when entity-api in READ-ONLY mode")
    # If an invalid token provided, we need to tell the client with a 401 error, rather
    # than a 500 error later if the token is not good.
    validate_token_if_auth_header_exists(request)
    # Get user token from Authorization header
    user_token = get_user_token(request)
    # Create a dictionary as required to use an entity validator. Ignore the
    # options_dict['existing_entity_dict'] support for PUT requests, since this
    # @app.route() only supports POST.
    options_dict = {'http_request': request}
    try:
        schema_validators.validate_application_header_before_entity_create(options_dict=options_dict)
    except Exception as e:
        bad_request_error(str(e))
    require_json(request)

    ######### validate top level properties ########

    # Verify that each required field is in the json_data_dict, and that there are no other fields
    json_data_dict = request.get_json()
    required_fields = ['creation_action', 'group_uuid', 'direct_ancestor_uuids', 'datasets']
    for field in required_fields:
        if field not in json_data_dict:
            raise bad_request_error(f"Missing required field {field}")
    for field in json_data_dict:
        if field not in required_fields:
            raise bad_request_error(f"Request body contained unexpected field {field}")

    # validate creation_action
    allowable_creation_actions = ['Multi-Assay Split']
    if json_data_dict.get('creation_action') not in allowable_creation_actions:
        bad_request_error(f"creation_action {json_data_dict.get('creation_action')} not recognized. Allowed values are: {COMMA_SEPARATOR.join(allowable_creation_actions)}")

    # While we accept a list of direct_ancestor_uuids, we currently only allow a single direct ancestor so verify that there is only 1
    direct_ancestor_uuids = json_data_dict.get('direct_ancestor_uuids')
    if direct_ancestor_uuids is None or not isinstance(direct_ancestor_uuids, list) or len(direct_ancestor_uuids) !=1:
        bad_request_error(f"Required field 'direct_ancestor_uuids' must be a list. This list may only contain 1 item: a string representing the uuid of the direct ancestor")
    # validate existence of direct ancestors.
    for direct_ancestor_uuid in direct_ancestor_uuids:
        direct_ancestor_dict = query_target_entity(direct_ancestor_uuid, user_token)
        if direct_ancestor_dict.get('entity_type').lower() != "dataset":
            bad_request_error(f"Direct ancestor is of type: {direct_ancestor_dict.get('entity_type')}. Must be of type 'dataset'.")
        dataset_has_component_children = app_neo4j_queries.dataset_has_component_children(neo4j_driver_instance, direct_ancestor_uuid)
        if dataset_has_component_children:
            bad_request_error(f"The dataset with uuid {direct_ancestor_uuid} already has component children dataset(s)")
    # validate that there is at least one component dataset
    if len(json_data_dict.get('datasets')) < 1:
        bad_request_error(f"'datasets' field must contain at least 1 dataset.")

    # Validate all datasets using existing schema with triggers and validators
    for dataset in json_data_dict.get('datasets'):
        # dataset_link_abs_dir is not part of the entity creation, will not be stored in neo4j and does not require
        # validation. Remove it here and add it back after validation. We do the same for creating the entities. Doing
        # this makes it easier to keep the dataset_link_abs_dir with the associated dataset instead of adding additional lists and keeping track of which value is tied to which dataset
        dataset_link_abs_dir = dataset.pop('dataset_link_abs_dir', None)
        if not dataset_link_abs_dir:
            bad_request_error(f"Missing required field in datasets: dataset_link_abs_dir")
        dataset['group_uuid'] = json_data_dict.get('group_uuid')
        dataset['direct_ancestor_uuids'] = direct_ancestor_uuids
        try:
            schema_manager.validate_json_data_against_schema(dataset, 'Dataset')
        except schema_errors.SchemaValidationException as e:
            # No need to log validation errors
            bad_request_error(str(e))
        # Execute property level validators defined in the schema yaml before entity property creation
        # Use empty dict {} to indicate there's no existing_data_dict
        try:
            schema_manager.execute_property_level_validators('before_property_create_validators', "Dataset", request, {}, dataset)
        # Currently only ValueError
        except ValueError as e:
            bad_request_error(e)

        # Add back in dataset_link_abs_dir
        dataset['dataset_link_abs_dir'] = dataset_link_abs_dir

    # Check URL parameters before proceeding to any CRUD operations, halting on validation failures.
    #
    # Check if re-indexing is to be suppressed after entity creation.
    try:
        suppress_reindex = _suppress_reindex()
    except Exception as e:
        bad_request_error(e)

    dataset_list = create_multiple_component_details(request, "Dataset", user_token, json_data_dict.get('datasets'), json_data_dict.get('creation_action'))

    # We wait until after the new datasets are linked to their ancestor before performing the remaining post-creation
    # linkeages. This way, in the event of unforseen errors, we don't have orphaned nodes.
    for dataset in dataset_list:
        schema_triggers.set_status_history('status', 'Dataset', user_token, dataset, {})

    properties_to_skip = [
        'direct_ancestors',
        'collections',
        'upload',
        'title',
        'previous_revision_uuid',
        'next_revision_uuid'
    ]

    if bool(request.args):
        # The parsed query string value is a string 'true'
        return_all_properties = request.args.get('return_all_properties')

        if (return_all_properties is not None) and (return_all_properties.lower() == 'true'):
            properties_to_skip = []

    normalized_complete_entity_list = []
    for dataset in dataset_list:
        # Remove dataset_link_abs_dir once more before entity creation
        dataset_link_abs_dir = dataset.pop('dataset_link_abs_dir', None)
        # Generate the filtered or complete entity dict to send back
        complete_dict = schema_manager.get_complete_entity_result(user_token, dataset, properties_to_skip)

        # Will also filter the result based on schema
        normalized_complete_dict = schema_manager.normalize_entity_result_for_response(complete_dict)

        if suppress_reindex:
            logger.log(level=logging.INFO
                       , msg=f"Re-indexing suppressed during multiple component creation of {complete_dict['entity_type']}"
                             f" with UUID {complete_dict['uuid']}")
        else:
            # Also index the new entity node in elasticsearch via search-api
            logger.log(level=logging.INFO
                       , msg=f"Re-indexing for multiple component creation of {complete_dict['entity_type']}"
                             f" with UUID {complete_dict['uuid']}")
            reindex_entity(complete_dict['uuid'], user_token)
        # Add back in dataset_link_abs_dir one last time
        normalized_complete_dict['dataset_link_abs_dir'] = dataset_link_abs_dir
        normalized_complete_entity_list.append(normalized_complete_dict)

    return jsonify(normalized_complete_entity_list)

"""
New endpoints (PUT /datasets and PUT /uploads) to handle the bulk updating of entities see Issue: #698
https://github.com/hubmapconsortium/entity-api/issues/698

This is used by Data Ingest Board application for now.

Shirey: With this use case we're not worried about a lot of concurrent calls to this endpoint (only one user,
Brendan, will be ever using it). Just start a thread on request and loop through the Datasets/Uploads to change
with a 5 second delay or so between them to allow some time for reindexing.

Example call
1) pick Dataset entities to change by querying Neo4J...
URL: http://18.205.215.12:7474/browser/
query: MATCH (e:Dataset {entity_type: 'Dataset'}) RETURN e.uuid, e.status, e.ingest_task, e.assigned_to_group_name LIMIT 100

curl --request PUT \
 --url ${ENTITY_API}/datasets \
 --header "Content-Type: application/json" \
 --header "Authorization: Bearer ${TOKEN}" \
 --header "X-Hubmap-Application: entity-api" \
 --data '[{"uuid":"f22a9ba97b79eefe6b152b4315e43c76", "status":"Error", "assigned_to_group_name":"TMC - Cal Tech"}, {"uuid":"e4b371ea3ed4c3ca77791b34b829803f", "status":"Error", "assigned_to_group_name":"TMC - Cal Tech"}]'
"""
@app.route('/datasets', methods=['PUT'])
@app.route('/uploads', methods=['PUT'])
def entity_bulk_update():
    # Only in the PUT call: `assigned_to_group_name` is allowed to use an empty string value to reser/clear existing values
    ENTITY_BULK_UPDATE_FIELDS_ACCEPTED = ['uuid', 'status', 'ingest_task', 'assigned_to_group_name']

    entity_type: str = 'dataset'
    if request.path == "/uploads":
        entity_type = "upload"

    validate_token_if_auth_header_exists(request)
    require_json(request)

    entities = request.get_json()
    if entities is None or not isinstance(entities, list) or len(entities) == 0:
        bad_request_error("Request object field 'entities' is either missing, "
                          "does not contain a list, or contains an empty list")

    user_token: str = get_user_token(request)
    for entity in entities:
        validate_user_update_privilege(entity, user_token)

    uuids = [e.get("uuid") for e in entities]

    logger.debug(f"Bulk updating the following {entity_type} uuids:")
    logger.debug(uuids)

    if None in uuids:
        bad_request_error(f"All {entity_type}s must have a 'uuid' field")
    if len(set(uuids)) != len(uuids):
        bad_request_error(f"{entity_type}s must have unique 'uuid' fields")

    if not all(set(e.keys()).issubset(ENTITY_BULK_UPDATE_FIELDS_ACCEPTED) for e in entities):
        bad_request_error(
            f"Some {entity_type}s have invalid fields. Acceptable fields are: " +
            ", ".join(ENTITY_BULK_UPDATE_FIELDS_ACCEPTED)
        )

    uuids = set([e["uuid"] for e in entities])
    try:
        fields = {"uuid", "entity_type"}
        db_entities = app_neo4j_queries.get_entities_by_uuid(neo4j_driver_instance, uuids, fields)
    except Exception as e:
        logger.error(f"Error while submitting datasets: {str(e)}")
        bad_request_error(str(e))

    diff = uuids.difference({e["uuid"] for e in db_entities if e["entity_type"].lower() == entity_type})
    if len(diff) > 0:
        bad_request_error(f"No {entity_type} found with the following uuids: {', '.join(diff)}")

    logger.info(f"Bulk update {len(entities)} {entity_type} in a separate thread...")

    thread_instance =\
        threading.Thread(target=update_datasets_uploads,
                         args=(entities, user_token, app.config["ENTITY_API_URL"]))
    thread_instance.start()

    return jsonify(list(uuids)), 202


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
Parse the token from Authorization header

Parameters
----------
request : falsk.request
    The flask http request object
non_public_access_required : bool
    If a non-public access token is required by the request, default to False

Returns
-------
str
    The token string if valid
"""
def get_user_token(request, non_public_access_required = False):
    # Get user token from Authorization header
    # getAuthorizationTokens() also handles MAuthorization header but we are not using that here
    try:
        user_token = auth_helper_instance.getAuthorizationTokens(request.headers)
    except Exception:
        msg = "Failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        internal_server_error(msg)

    # Further check the validity of the token if required non-public access
    if non_public_access_required:
        # When the token is a flask.Response instance,
        # it MUST be a 401 error with message.
        # That's how commons.auth_helper.getAuthorizationTokens() was designed
        if isinstance(user_token, Response):
            # We wrap the message in a json and send back to requester as 401 too
            # The Response.data returns binary string, need to decode
            unauthorized_error(user_token.get_data().decode())

        # By now the token is already a valid token
        # But we also need to ensure the user belongs to HuBMAP-Read group
        # in order to access the non-public entity
        # Return a 403 response if the user doesn't belong to HuBMAP-READ group
        if not user_in_hubmap_read_group(request):
            forbidden_error("Access not granted")

    return user_token


"""
Check if the user with token is in the HuBMAP-READ group

Parameters
----------
request : falsk.request
    The flask http request object that containing the Authorization header
    with a valid Globus groups token for checking group information

Returns
-------
bool
    True if the user belongs to HuBMAP-READ group, otherwise False
"""
def user_in_hubmap_read_group(request):
    if 'Authorization' not in request.headers:
        return False

    try:
        # The property 'hmgroupids' is ALWASYS in the output with using schema_manager.get_user_info()
        # when the token in request is a groups token
        user_info = schema_manager.get_user_info(request)
        hubmap_read_group_uuid = auth_helper_instance.groupNameToId('HuBMAP-READ')['uuid']
    except Exception as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)

        # If the token is not a groups token, no group information available
        # The commons.hm_auth.AuthCache would return a Response with 500 error message
        # We treat such cases as the user not in the HuBMAP-READ group
        return False


    return (hubmap_read_group_uuid in user_info['hmgroupids'])


"""
Check if a user has valid access to update a given entity

Parameters
----------
entity : dict
    The entity that is attempting to be updated
user_token : str 
    The token passed in via the request header that will be used to authenticate
"""
def validate_user_update_privilege(entity, user_token):
    # A user has update privileges if they are a data admin or are in the same group that registered the entity
    is_admin = auth_helper_instance.has_data_admin_privs(user_token)

    if isinstance(is_admin, Response):
        abort(is_admin)

    user_write_groups: List[dict] = auth_helper_instance.get_user_write_groups(user_token)

    if isinstance(user_write_groups, Response):
        abort(user_write_groups)

    user_group_uuids = [d['uuid'] for d in user_write_groups]
    if entity.get('group_uuid') not in user_group_uuids and is_admin is False:
        forbidden_error(f"User does not have write privileges for this entity. "
                        "Please reach out to the help desk (help@hubmapconsortium.org) to request access.")


"""
Validate the provided token when Authorization header presents

Parameters
----------
request : flask.request object
    The Flask http request object
"""
def validate_token_if_auth_header_exists(request):
    # No matter if token is required or not, when an invalid token provided,
    # we need to tell the client with a 401 error
    # HTTP header names are case-insensitive
    # request.headers.get('Authorization') returns None if the header doesn't exist
    if request.headers.get('Authorization') is not None:
        user_token = get_user_token(request)

        # When the Authoriztion header provided but the user_token is a flask.Response instance,
        # it MUST be a 401 error with message.
        # That's how commons.auth_helper.getAuthorizationTokens() was designed
        if isinstance(user_token, Response):
            # We wrap the message in a json and send back to requester as 401 too
            # The Response.data returns binary string, need to decode
            unauthorized_error(user_token.get_data().decode())

        # Also check if the parased token is invalid or expired
        # Set the second paremeter as False to skip group check
        user_info = auth_helper_instance.getUserInfo(user_token, False)

        if isinstance(user_info, Response):
            unauthorized_error(user_info.get_data().decode())


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
Return the "visibility" of an entity as DataVisibilityEnum value.  Determination of
"public" or "non-public" is specific to entity type.

Parameters
----------
entity_dict : dict
    A Python dictionary retrieved for the entity 
normalized_entity_type : str
    One of the normalized entity types: Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication

Returns
-------
DataVisibilityEnum
    A value identifying if the entity is public or non-public
"""
def _get_entity_visibility(normalized_entity_type, entity_dict):
    if normalized_entity_type not in schema_manager.get_all_entity_types():
        logger.log( logging.ERROR
                    ,f"normalized_entity_type={normalized_entity_type}"
                     f" not recognized by schema_manager.get_all_entity_types().")
        bad_request_error(f"'{normalized_entity_type}' is not a recognized entity type.")

    # Use the characteristics of the entity's data to classify the entity's visibility, so
    # it can be used along with the user's authorization to determine access.
    entity_visibility=DataVisibilityEnum.NONPUBLIC
    if schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset') and \
       entity_dict['status'].lower() == DATASET_STATUS_PUBLISHED:
        entity_visibility=DataVisibilityEnum.PUBLIC
    elif schema_manager.entity_type_instanceof(normalized_entity_type, 'Collection') and \
        'registered_doi' in entity_dict and \
        'doi_url' in entity_dict and \
        'contacts' in entity_dict and \
        'contributors' in entity_dict and \
        len(entity_dict['contacts']) > 0 and \
        len(entity_dict['contributors']) > 0:
            # Get the data_access_level for each Dataset in the Collection from Neo4j
            collection_dataset_statuses = schema_neo4j_queries.get_collection_datasets_statuses(neo4j_driver_instance
                                                                                                ,entity_dict['uuid'])

            # If the list of distinct statuses for Datasets in the Collection only has one entry, and
            # it is 'published', the Collection is public
            if len(collection_dataset_statuses) == 1 and \
                collection_dataset_statuses[0].lower() == SchemaConstants.DATASET_STATUS_PUBLISHED:
                entity_visibility=DataVisibilityEnum.PUBLIC
    elif normalized_entity_type == 'Upload':
        # Upload entities require authorization to access, so keep the
        # entity_visibility as non-public, as initialized outside block.
        pass
    elif normalized_entity_type in ['Donor','Sample'] and \
         entity_dict['data_access_level'] == ACCESS_LEVEL_PUBLIC:
        entity_visibility = DataVisibilityEnum.PUBLIC
    return entity_visibility


"""
Retrieve the organ, donor, or sample metadata information associated with a Dataset, based
up the user's authorization to access the Dataset.

Parameters
----------
dataset_dict : dict
    A dictionary containing all the properties the target entity.
dataset_visibility : DataVisibilityEnum
    An indication of the entity itself is public or not, so the associated data can
    be filtered to match the entity dictionary before being returned.
valid_user_token : str
    Either the valid current token for an authenticated user or None.
user_info : dict
    Information for the logged-in user to be used for authorization accessing non-public entities.
associated_data : str
    A string indicating the associated property to be retrieved, which must be from
    the values supported by this method.

Returns
-------
list
    A dictionary containing the metadata properties the Dataset associated data.
"""
def _get_dataset_associated_metadata(dataset_dict, dataset_visibility, valid_user_token, request, associated_data: str):

    # Confirm the associated data requested is supported by this method.
    retrievable_associations = ['organs', 'samples', 'donors']
    if associated_data.lower() not in retrievable_associations:
        bad_request_error(  f"Dataset associated data cannot be retrieved for"
                            f" {associated_data}, only"
                            f" {COMMA_SEPARATOR.join(retrievable_associations)}.")

    # Confirm the dictionary passed in is for a Dataset entity.
    if not schema_manager.entity_type_instanceof(dataset_dict['entity_type'], 'Dataset'):
        bad_request_error(  f"'{dataset_dict['entity_type']}' for"
                            f" uuid={dataset_dict['uuid']} is not a Dataset or Publication,"
                            f" so '{associated_data}' can not be retrieved for it.")
    # Set up fields to be excluded when retrieving the entities associated with
    # the Dataset.  Organs are one kind of Sample.
    if associated_data.lower() in ['organs', 'samples']:
        fields_to_exclude = schema_manager.get_fields_to_exclude('Sample')
    elif associated_data.lower() in ['donors']:
        fields_to_exclude = schema_manager.get_fields_to_exclude('Donor')
    else:
        logger.error(   f"Expected associated data type to be verified, but got"
                        f" associated_data.lower()={associated_data.lower()}.")
        internal_server_error(f"Unexpected error retrieving '{associated_data}' for a Dataset")

    public_entity = (dataset_visibility is DataVisibilityEnum.PUBLIC)

    # Set a variable reflecting the user's authorization by being in the HuBMAP-READ Globus Group
    user_authorized = user_in_hubmap_read_group(request=request)

    # For non-public documents, reject the request if the user is not authorized
    if not public_entity:
        if valid_user_token is None:
            forbidden_error(    f"{dataset_dict['entity_type']} for"
                                f" {dataset_dict['uuid']} is not"
                                f" accessible without presenting a token.")
        if not user_authorized:
            forbidden_error(    f"The requested Dataset has non-public data."
                                f"  A Globus token with access permission is required.")

    # By now, either the entity is public accessible or the user has the correct access level
    if associated_data.lower() == 'organs':
        associated_entities = app_neo4j_queries.get_associated_organs_from_dataset(neo4j_driver_instance,
                                                                                   dataset_dict['uuid'])
    elif associated_data.lower() == 'samples':
        associated_entities = app_neo4j_queries.get_associated_samples_from_dataset(neo4j_driver_instance,
                                                                                    dataset_dict['uuid'])
    elif associated_data.lower() == 'donors':
        associated_entities = app_neo4j_queries.get_associated_donors_from_dataset(neo4j_driver_instance,
                                                                                   dataset_dict['uuid'])
    else:
        logger.error(   f"Expected associated data type to be verified, but got"
                        f" associated_data.lower()={associated_data.lower()} while retrieving from Neo4j.")
        internal_server_error(f"Unexpected error retrieving '{associated_data}' from the data store")

    # If there are zero items in the list of associated_entities, return an empty list rather than retrieving.
    if len(associated_entities) < 1:
        return []

    # Use the internal token to query the target entity to assure it is returned. This way public
    # entities can be accessed even if valid_user_token is None.
    internal_token = auth_helper_instance.getProcessSecret()
    complete_entities_list = schema_manager.get_complete_entities_list( token=internal_token
                                                                        , entities_list=associated_entities)
    # Final result after normalization
    final_result = schema_manager.normalize_entities_list_for_response(entities_list=complete_entities_list)

    # For public entities, limit the fields in the response unless the authorization presented in the
    # Request allows the user to see all properties.
    # Remove fields which do not belong in provenance metadata, regardless of
    # entity scope or user authorization.
    filtered_entities_list = []
    for entity in final_result:
        final_entity_dict = schema_manager.exclude_properties_from_response(excluded_fields=fields_to_exclude
                                                                            , output_dict=entity)
        filtered_entities_list.append(final_entity_dict)
    final_result = filtered_entities_list

    return final_result

# Use the Flask request.args MultiDict to see if 'reindex' is a URL parameter passed in with the
# request and if it indicates reindexing should be supressed. Default to reindexing in all other cases.
def _suppress_reindex() -> bool:
    if 'reindex' not in request.args:
        return False
    reindex_str = request.args.get('reindex').lower()
    if reindex_str == 'false':
        return True
    elif reindex_str == 'true':
        return False
    raise Exception(f"The value of the 'reindex' parameter must be True or False (case-insensitive)."
                    f" '{request.args.get('reindex')}' is not recognized.")

"""
Generate 'before_create_triiger' data and create the entity details in Neo4j

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication
user_token: str
    The user's globus groups token
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
        msg = "The user does not belong to the given Globus group, can't create the entity"
        logger.exception(msg)
        forbidden_error(msg)
    except schema_errors.MultipleDataProviderGroupException:
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        bad_request_error(msg)
    except KeyError as e:
        logger.exception(e)
        bad_request_error(e)
    except requests.exceptions.RequestException as e:
        msg = f"Failed to create new HuBMAP ids via the uuid-api service"
        logger.exception(msg)

        # Due to the use of response.raise_for_status() in schema_manager.create_hubmap_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            bad_request_error(e.response.text)
        if status_code == 404:
            not_found_error(e.response.text)
        else:
            internal_server_error(e.response.text)

    # Merge all the above dictionaries and pass to the trigger methods
    new_data_dict = {**json_data_dict, **user_info_dict, **new_ids_dict}

    try:
        # Use {} since no existing dict
        generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data( trigger_type=TriggerTypeEnum.BEFORE_CREATE
                                                                                            , normalized_class=normalized_entity_type
                                                                                            , user_token=user_token
                                                                                            , existing_data_dict={}
                                                                                            , new_data_dict=new_data_dict)
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
        forbidden_error(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        bad_request_error(msg)
    # If something wrong with file upload
    except schema_errors.FileUploadException as e:
        logger.exception(e)
        internal_server_error(e)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        bad_request_error(e)
    except Exception as e:
        logger.exception(e)
        internal_server_error(e)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_before_create_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
    # and properties with None value
    # Meaning the returned target property key is different from the original key
    # in the trigger method, e.g., Donor.image_files_to_add
    filtered_merged_dict = schema_manager.remove_transient_and_none_values(merged_dict, normalized_entity_type)
    # Create new entity
    try:
        # Check if the optional `superclass` property is defined, None otherwise
        superclass = schema_manager.get_entity_superclass(normalized_entity_type)

        # Important: `entity_dict` is the resulting neo4j dict, Python list and dicts are stored
        # as string expression literals in it. That's why properties like entity_dict['direct_ancestor_uuids']
        # will need to use ast.literal_eval() in the schema_triggers.py
        entity_dict = schema_neo4j_queries.create_entity(neo4j_driver_instance, normalized_entity_type, filtered_merged_dict, superclass)
    except (TransactionError, ValueError):
        msg = "Failed to create the new " + normalized_entity_type
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)


    # Important: use `entity_dict` instead of `filtered_merged_dict` to keep consistent with the stored
    # string expression literals of Python list/dict being used with entity update, e.g., `image_files`
    # Important: the same property keys in entity_dict will overwrite the same key in json_data_dict
    # and this is what we wanted. Adding json_data_dict back is to include those `transient` properties
    # provided in the JSON input but not stored in neo4j, and will be needed for after_create_trigger/after_update_trigger,
    # e.g., `previous_revision_uuid`, `direct_ancestor_uuids`
    # Add user_info_dict because it may be used by after_update_trigger methods
    merged_final_dict = {**json_data_dict, **entity_dict, **user_info_dict}

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
    Must be "Sample" in this case
user_token: str
    The user's globus groups token
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
        forbidden_error(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        bad_request_error(msg)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        bad_request_error(e)
    except requests.exceptions.RequestException as e:
        msg = f"Failed to create new HuBMAP ids via the uuid-api service"
        logger.exception(msg)

        # Due to the use of response.raise_for_status() in schema_manager.create_hubmap_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            bad_request_error(e.response.text)
        if status_code == 404:
            not_found_error(e.response.text)
        else:
            internal_server_error(e.response.text)

    # Use the same json_data_dict and user_info_dict for each sample
    # Only difference is the `uuid` and `hubmap_id` that are generated
    # Merge all the dictionaries and pass to the trigger methods
    new_data_dict = {**json_data_dict, **user_info_dict, **new_ids_dict_list[0]}

    # Instead of calling generate_triggered_data() for each sample, we'll just call it on the first sample
    # since all other samples will share the same generated data except `uuid` and `hubmap_id`
    # A bit performance improvement
    try:
        # Use {} since no existing dict
        generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data( trigger_type=TriggerTypeEnum.BEFORE_CREATE
                                                                                            , normalized_class=normalized_entity_type
                                                                                            , user_token=user_token
                                                                                            , existing_data_dict={}
                                                                                            , new_data_dict=new_data_dict)
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
        forbidden_error(msg)
    except schema_errors.MultipleDataProviderGroupException:
        # Log the full stack trace, prepend a line with our message
        msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
        logger.exception(msg)
        bad_request_error(msg)
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        bad_request_error(e)
    except Exception as e:
        logger.exception(e)
        internal_server_error(e)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_before_create_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
    # and properties with None value
    # Meaning the returned target property key is different from the original key
    # in the trigger method, e.g., Donor.image_files_to_add
    filtered_merged_dict = schema_manager.remove_transient_and_none_values(merged_dict, normalized_entity_type)

    samples_dict_list = []
    for new_ids_dict in new_ids_dict_list:
        # Just overwrite the `uuid` and `hubmap_id` that are generated
        # All other generated properties will stay the same across all samples
        sample_dict = {**filtered_merged_dict, **new_ids_dict}
        # Add to the list
        samples_dict_list.append(sample_dict)

    # Generate property values for the only one Activity node
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
Create multiple dataset nodes and relationships with the source entity node

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    Must be "Dataset" in this case
user_token: str
    The user's globus groups token
json_data_dict_list: list
    List of datasets objects as dictionaries
creation_action : str
    The creation action for the new activity node.

Returns
-------
list
    A list of all the newly created datasets with generated fields represented as dictionaries
"""
def create_multiple_component_details(request, normalized_entity_type, user_token, json_data_dict_list, creation_action):
    # Get user info based on request
    user_info_dict = schema_manager.get_user_info(request)
    direct_ancestor = json_data_dict_list[0].get('direct_ancestor_uuids')[0]
    # Create new ids for the new entity
    try:
        # we only need the json data from one of the datasets. The info will be the same for both, so we just grab the first in the list
        new_ids_dict_list = schema_manager.create_hubmap_ids(normalized_entity_type, json_data_dict_list[0], user_token, user_info_dict, len(json_data_dict_list))
    # When group_uuid is provided by user, it can be invalid
    except KeyError as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)
        bad_request_error(e)
    except requests.exceptions.RequestException as e:
        msg = f"Failed to create new HuBMAP ids via the uuid-api service"
        logger.exception(msg)

        # Due to the use of response.raise_for_status() in schema_manager.create_hubmap_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            bad_request_error(e.response.text)
        if status_code == 404:
            not_found_error(e.response.text)
        else:
            internal_server_error(e.response.text)
    datasets_dict_list = []
    for i in range(len(json_data_dict_list)):
        # Remove dataset_link_abs_dir once more before entity creation
        dataset_link_abs_dir = json_data_dict_list[i].pop('dataset_link_abs_dir', None)
        # Combine each id dict into each dataset in json_data_dict_list
        new_data_dict = {**json_data_dict_list[i], **user_info_dict, **new_ids_dict_list[i]}
        try:
            # Use {} since no existing dict
            generated_before_create_trigger_data_dict = schema_manager.generate_triggered_data( trigger_type=TriggerTypeEnum.BEFORE_CREATE
                                                                                                , normalized_class=normalized_entity_type
                                                                                                , user_token=user_token
                                                                                                , existing_data_dict={}
                                                                                                , new_data_dict=new_data_dict)
            # If one of the before_create_trigger methods fails, we can't create the entity
        except schema_errors.BeforeCreateTriggerException:
            # Log the full stack trace, prepend a line with our message
            msg = "Failed to execute one of the 'before_create_trigger' methods, can't create the entity"
            logger.exception(msg)
            internal_server_error(msg)
        except schema_errors.NoDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            if 'group_uuid' in json_data_dict_list[i]:
                msg = "Invalid 'group_uuid' value, can't create the entity"
            else:
                msg = "The user does not have the correct Globus group associated with, can't create the entity"

            logger.exception(msg)
            bad_request_error(msg)
        except schema_errors.UnmatchedDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            msg = "The user does not belong to the given Globus group, can't create the entity"
            logger.exception(msg)
            forbidden_error(msg)
        except schema_errors.MultipleDataProviderGroupException:
            # Log the full stack trace, prepend a line with our message
            msg = "The user has mutiple Globus groups associated with, please specify one using 'group_uuid'"
            logger.exception(msg)
            bad_request_error(msg)
        except KeyError as e:
            # Log the full stack trace, prepend a line with our message
            logger.exception(e)
            bad_request_error(e)
        except Exception as e:
            logger.exception(e)
            internal_server_error(e)
        merged_dict = {**json_data_dict_list[i], **generated_before_create_trigger_data_dict}

        # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
        # and properties with None value
        # Meaning the returned target property key is different from the original key
        # in the trigger method, e.g., Donor.image_files_to_add
        filtered_merged_dict = schema_manager.remove_transient_and_none_values(merged_dict, normalized_entity_type)
        dataset_dict = {**filtered_merged_dict, **new_ids_dict_list[i]}
        dataset_dict['dataset_link_abs_dir'] = dataset_link_abs_dir
        datasets_dict_list.append(dataset_dict)

    activity_data_dict = schema_manager.generate_activity_data(normalized_entity_type, user_token, user_info_dict)
    activity_data_dict['creation_action'] = creation_action
    try:
        created_datasets = app_neo4j_queries.create_multiple_datasets(neo4j_driver_instance, datasets_dict_list, activity_data_dict, direct_ancestor)
    except TransactionError:
        msg = "Failed to create multiple samples"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

    return created_datasets


"""
Execute 'after_create_triiger' methods

Parameters
----------
normalized_entity_type : str
    One of the normalized entity types: Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication
user_token: str
    The user's globus groups token
merged_data_dict: dict
    The merged dict that contains the entity dict newly created and 
    information from user request json that are not stored in Neo4j
"""
def after_create(normalized_entity_type, user_token, merged_data_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        # Use {} since no new dict
        schema_manager.generate_triggered_data( trigger_type=TriggerTypeEnum.AFTER_CREATE
                                                , normalized_class=normalized_entity_type
                                                , user_token=user_token
                                                , existing_data_dict=merged_data_dict
                                                , new_data_dict={})
    except schema_errors.AfterCreateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "The entity has been created, but failed to execute one of the 'after_create_trigger' methods"
        logger.exception(msg)
        internal_server_error(msg)
    except Exception as e:
        logger.exception(e)
        internal_server_error(e)


"""
Generate 'before_create_triiger' data and create the entity details in Neo4j

Parameters
----------
request : flask.Request object
    The incoming request
normalized_entity_type : str
    One of the normalized entity types: Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication
user_token: str
    The user's globus groups token
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
        generated_before_update_trigger_data_dict = schema_manager.generate_triggered_data( trigger_type=TriggerTypeEnum.BEFORE_UPDATE
                                                                                            , normalized_class=normalized_entity_type
                                                                                            , user_token=user_token
                                                                                            , existing_data_dict=existing_entity_dict
                                                                                            , new_data_dict=new_data_dict)
    # If something wrong with file upload
    except schema_errors.FileUploadException as e:
        logger.exception(e)
        internal_server_error(e)
    # If one of the before_update_trigger methods fails, we can't update the entity
    except schema_errors.BeforeUpdateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "Failed to execute one of the 'before_update_trigger' methods, can't update the entity"
        logger.exception(msg)
        internal_server_error(msg)
    except Exception as e:
        logger.exception(e)
        internal_server_error(e)

    # Merge dictionaries
    merged_dict = {**json_data_dict, **generated_before_update_trigger_data_dict}

    # Filter out the merged_dict by getting rid of the transitent properties (not to be stored)
    # and properties with None value
    # Meaning the returned target property key is different from the original key
    # in the trigger method, e.g., Donor.image_files_to_add
    filtered_merged_dict = schema_manager.remove_transient_and_none_values(merged_dict, normalized_entity_type)

    # By now the filtered_merged_dict contains all user updates and all triggered data to be added to the entity node
    # Any properties in filtered_merged_dict that are not on the node will be added.
    # Any properties not in filtered_merged_dict that are on the node will be left as is.
    # Any properties that are in both filtered_merged_dict and the node will be replaced in the node. However, if any property in the map is null, it will be removed from the node.

    # Update the exisiting entity
    try:
        updated_entity_dict = schema_neo4j_queries.update_entity(neo4j_driver_instance, normalized_entity_type, filtered_merged_dict, existing_entity_dict['uuid'])
    except TransactionError:
        msg = "Failed to update the entity with id " + id
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)
        # Terminate and let the users know
        internal_server_error(msg)

    # Important: use `updated_entity_dict` instead of `filtered_merged_dict` to keep consistent with the stored
    # string expression literals of Python list/dict being used with entity update, e.g., `image_files`
    # Important: the same property keys in entity_dict will overwrite the same key in json_data_dict
    # and this is what we wanted. Adding json_data_dict back is to include those `transient` properties
    # provided in the JSON input but not stored in neo4j, and will be needed for after_create_trigger/after_update_trigger,
    # e.g., `previous_revision_uuid`, `direct_ancestor_uuids`
    # Add user_info_dict because it may be used by after_update_trigger methods
    merged_final_dict = {**json_data_dict, **updated_entity_dict, **user_info_dict}

    # Use merged_final_dict instead of merged_dict because
    # merged_dict only contains properties to be updated, not all properties
    return merged_final_dict


"""
Execute 'after_update_trigger' methods

Parameters
----------
normalized_entity_type : str
    One of the normalized entity types: Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication
user_token: str
    The user's globus groups token
entity_dict: dict
    The entity dict newly updated
"""
def after_update(normalized_entity_type, user_token, entity_dict):
    try:
        # 'after_create_trigger' and 'after_update_trigger' don't generate property values
        # It just returns the empty dict, no need to assign value
        # Use {} sicne no new dict
        schema_manager.generate_triggered_data( trigger_type=TriggerTypeEnum.AFTER_UPDATE
                                                , normalized_class=normalized_entity_type
                                                , user_token=user_token
                                                , existing_data_dict=entity_dict
                                                , new_data_dict={})
    except schema_errors.AfterUpdateTriggerException:
        # Log the full stack trace, prepend a line with our message
        msg = "The entity information has been updated, but failed to execute one of the 'after_update_trigger' methods"
        logger.exception(msg)
        internal_server_error(msg)
    except Exception as e:
        logger.exception(e)
        internal_server_error(e)


"""
Get target entity dict from Neo4j query for the given id

Parameters
----------
id : str
    The uuid or hubmap_id of target entity
user_token: str
    The user's globus groups token from the incoming request

Returns
-------
dict
    A dictionary of entity details either from cache or new neo4j lookup
"""
def query_target_entity(id, user_token):
    entity_dict = None
    cache_result = None

    try:
        # Get cached ids if exist otherwise retrieve from UUID-API
        hubmap_ids = schema_manager.get_hubmap_ids(id.strip())

        # Get the target uuid if all good
        uuid = hubmap_ids['hm_uuid']

        # Look up the cache again by the uuid since we only use uuid in the cache key
        if MEMCACHED_MODE and MEMCACHED_PREFIX and memcached_client_instance:
            cache_key = f'{MEMCACHED_PREFIX}_neo4j_{uuid}'
            cache_result = memcached_client_instance.get(cache_key)

        if cache_result is None:
            logger.info(f'Neo4j entity cache of {uuid} not found or expired at time {datetime.now()}')

            # Make a new query against neo4j
            entity_dict = schema_neo4j_queries.get_entity(neo4j_driver_instance, uuid)

            # The uuid exists via uuid-api doesn't mean it also exists in Neo4j
            if not entity_dict:
                logger.info(f"Entity of uuid: {uuid} not found in Neo4j")

                # Still use the user provided id, especially when it's a hubmap_id, for error message
                not_found_error(f"Entity of id: {id} not found in Neo4j")
            
            # Save to cache
            if MEMCACHED_MODE and MEMCACHED_PREFIX and memcached_client_instance:
                logger.info(f'Creating neo4j entity result cache of {uuid} at time {datetime.now()}')

                cache_key = f'{MEMCACHED_PREFIX}_neo4j_{uuid}'
                memcached_client_instance.set(cache_key, entity_dict, expire = SchemaConstants.MEMCACHED_TTL)
        else:
            logger.info(f'Using neo4j entity cache of UUID {uuid} at time {datetime.now()}')

            entity_dict = cache_result
    except requests.exceptions.RequestException as e:
        # Due to the use of response.raise_for_status() in schema_manager.get_hubmap_ids()
        # we can access the status codes from the exception
        status_code = e.response.status_code

        if status_code == 400:
            bad_request_error(e.response.text)
        if status_code == 404:
            not_found_error(e.response.text)
        else:
            internal_server_error(e.response.text)

    # One final return
    return entity_dict


"""
Always expect a json body from user request

request : Flask request object
    The Flask request passed from the API endpoint
"""
def require_json(request):
    if not request.is_json:
        bad_request_error("A json body and appropriate Content-Type header are required")



"""
Delete the cached data of all possible keys used for the given entity_uuid and entity_type
By taking entity_uuid and entity_type as input, it eliminates the need to call query_target_entity()
which is more useful when the input id could be either UUID or HuBMAP ID.

Parameters
----------
entity_uuid : str
    The UUID of target entity Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication
entity_type : str
    One of the normalized entity types: Donor/Dataset/Sample/Upload/Collection/EPICollection/Publication
"""
def delete_cache(entity_uuid, entity_type):
    if MEMCACHED_MODE:
        descendant_uuids = []
        collection_dataset_uuids = []
        upload_dataset_uuids = []
        collection_uuids = []
        dataset_upload_dict = {}
        publication_collection_dict = {}

        # Determine the associated cache keys based on the entity type
        # For Donor/Datasets/Sample/Publication, delete the cache of all the descendants
        if entity_type in ['Donor', 'Sample', 'Dataset', 'Publication']:
            descendant_uuids = schema_neo4j_queries.get_descendants(neo4j_driver_instance, entity_uuid , 'uuid')

        # For Collection/Epicollection, delete the cache for each of its associated datasets (via [:IN_COLLECTION])
        if schema_manager.entity_type_instanceof(entity_type, 'Collection'):
            collection_dataset_uuids = schema_neo4j_queries.get_collection_associated_datasets(neo4j_driver_instance, entity_uuid , 'uuid')

        # For Upload, delete the cache for each of its associated Datasets (via [:IN_UPLOAD])
        if entity_type == 'Upload':
            upload_dataset_uuids = schema_neo4j_queries.get_upload_datasets(neo4j_driver_instance, entity_uuid , 'uuid')

        # For Dataset, also delete the cache of associated Collections and Upload
        if entity_type == 'Dataset':
            collection_uuids = schema_neo4j_queries.get_dataset_collections(neo4j_driver_instance, entity_uuid , 'uuid')
            dataset_upload_dict = schema_neo4j_queries.get_dataset_upload(neo4j_driver_instance, entity_uuid)

        # For Publication, also delete cache of the associated collection
        # NOTE: As of 5/30/2025, the [:USES_DATA] workaround has been deprecated.
        # Still keep it in the code until further decision - Zhou
        if entity_type == 'Publication':
            publication_collection_dict = schema_neo4j_queries.get_publication_associated_collection(neo4j_driver_instance, entity_uuid)
            
        # We only use uuid in the cache key acorss all the cache types
        uuids_list = [entity_uuid] + descendant_uuids + collection_dataset_uuids + upload_dataset_uuids + collection_uuids

        # Add to the list if the target dataset has linked upload
        if dataset_upload_dict:
            uuids_list.append(dataset_upload_dict['uuid'])

        # Add to the list if the target publicaiton has associated collection
        if publication_collection_dict:
            uuids_list.append(publication_collection_dict['uuid'])

        # Final batch delete
        schema_manager.delete_memcached_cache(uuids_list)


"""
Make a call to search-api to trigger reindex of this entity document in elasticsearch

Parameters
----------
uuid : str
    The uuid of the target entity
user_token: str
    The user's globus groups token
"""
def reindex_entity(uuid, user_token):
    headers = {
        'Authorization': f'Bearer {user_token}'
    }

    logger.info(f"Making a call to search-api to reindex uuid: {uuid}")

    response = requests.put(f"{app.config['SEARCH_API_URL']}/reindex/{uuid}", headers = headers)

    # The reindex takes time, so 202 Accepted response status code indicates that
    # the request has been accepted for processing, but the processing has not been completed
    if response.status_code == 202:
        logger.info(f"The search-api has accepted the reindex request for uuid: {uuid}")
    else:
        logger.error(f"The search-api failed to initialize the reindex for uuid: {uuid}")


"""
Ensure the access level dir with leading and trailing slashes

Parameters
----------
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


"""
Ensures that a given organ code is 2-letter alphabetic and can be found int the UBKG ontology-api

Parameters
----------
organ_code : str
"""
def validate_organ_code(organ_code):
    organ_types_dict = schema_manager.get_organ_types()
    if not organ_code.isalpha() or not len(organ_code) == 2:
        internal_server_error(f"Invalid organ code {organ_code}. Must be 2-letter alphabetic code")

    try:
        if organ_code.upper() not in organ_types_dict:
            not_found_error(f"Unable to find organ code {organ_code} via the ontology-api")
    except requests.exceptions.RequestException:
        msg = f"Failed to validate the organ code: {organ_code}"
        # Log the full stack trace, prepend a line with our message
        logger.exception(msg)

        # Terminate and let the users know
        internal_server_error(msg)


"""
Bulk update the entities in the entity-api.

This function supports request throttling and retries.

Parameters
----------
entity_updates : dict
    The dictionary of entity updates. The key is the uuid and the value is the
    update dictionary.
token : str
    The groups token for the request.
entity_api_url : str
    The url of the entity-api.
total_tries : int, optional
    The number of total requests to be made for each update, by default 3.
throttle : float, optional
    The time to wait between requests and retries, by default 5.
after_each_callback : Callable[[int], None], optional
    A callback function to be called after each update, by default None. The index
    of the update is passed as a parameter to the callback.

Returns
-------
dict
    The results of the bulk update. The key is the uuid of the entity. If
    successful, the value is a dictionary with "success" as True and "data" as the
    entity data. If failed, the value is a dictionary with "success" as False and
    "data" as the error message.
"""
def bulk_update_entities(
    entity_updates: dict,
    token: str,
    entity_api_url: str,
    total_tries: int = 3,
    throttle: float = 5,
    after_each_callback: Optional[Callable[[int], None]] = None,
) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        SchemaConstants.HUBMAP_APP_HEADER: SchemaConstants.ENTITY_API_APP,
    }
    # create a session with retries
    session = requests.Session()
    session.headers = headers
    retries = Retry(
        total=total_tries,
        backoff_factor=throttle,
        status_forcelist=[500, 502, 503, 504],
    )
    session.mount(entity_api_url, HTTPAdapter(max_retries=retries))

    results = {}
    with session as s:
        for idx, (uuid, payload) in enumerate(entity_updates.items()):
            try:
                # https://github.com/hubmapconsortium/entity-api/issues/698#issuecomment-2260799700
                # yuanzhou: When you iterate over the target uuids make individual PUT /entities/<uuid> calls.
                # The main reason we use the PUT call rather than direct neo4j query is because the entity update
                # needs to go through the schema trigger methods and generate corresponding values programmatically
                # before sending over to neo4j.
                # The PUT call returns the response immediately while the backend updating may be still going on.
                res = s.put(f"{entity_api_url}/entities/{uuid}", json=payload)
                
                results[uuid] = {
                    "success": res.ok,
                    "data": res.json() if res.ok else res.json().get("error"),
                }

                logger.info(f"Successfully made No.{idx + 1} internal entity-api call to update {uuid}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to update entity {uuid}: {e}")
                results[uuid] = {"success": False, "data": str(e)}

            if after_each_callback:
                after_each_callback(idx)

            if idx < len(entity_updates) - 1:
                time.sleep(throttle)

    logger.debug("Returning bulk_update_entities() resulting data")
    logger.debug(results)

    return results


"""
Bulk update the entities called in a separate thread

Parameters
----------
entity_updates : dict
    The dictionary of entity updates
token : str
    The groups token for the request
entity_api_url : str
    The url of the entity-api
"""
def update_datasets_uploads(entity_updates: list, token: str, entity_api_url: str) -> None:
    update_payload = {ds.pop("uuid"): ds for ds in entity_updates}
    update_res = bulk_update_entities(update_payload, token, entity_api_url)

    for uuid, res in update_res.items():
        if not res["success"]:
            logger.error(f"Failed to update entity {uuid}: {res['data']}")


"""
Retrieve the JSON containing the normalized metadata information for a given entity appropriate for the
scope of metadata requested e.g. complete data for a another service, indexing data for an OpenSearch document, etc.

Parameters
----------
entity_id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity 
metadata_scope:
    A recognized scope from the SchemaConstants, controlling the triggers which are fired and elements
    from Neo4j which are retained.  Default is MetadataScopeEnum.INDEX.
    
Returns
-------
json
    Metadata for the entity appropriate for the metadata_scope argument, and filtered by an additional
    `property` arguments in the HTTP request.
"""
def _get_metadata_by_id(entity_id:str=None, metadata_scope:MetadataScopeEnum=MetadataScopeEnum.INDEX):
    # Token is not required, but if an invalid token provided,
    # we need to tell the client with a 401 error
    validate_token_if_auth_header_exists(request)

    # Use the internal token to query the target entity
    # since public entities don't require user token
    token = get_internal_token()

    # Get the entity dict from cache if exists
    # Otherwise query against uuid-api and neo4j to get the entity dict if the id exists
    entity_dict = query_target_entity(entity_id, token)
    normalized_entity_type = entity_dict['entity_type']
    excluded_fields = schema_manager.get_fields_to_exclude(normalized_entity_type)
    # Get the entity result of the indexable dictionary from cache if exists, otherwise regenerate and cache
    metadata_dict = schema_manager.get_index_metadata(token, entity_dict) \
                    if metadata_scope==MetadataScopeEnum.INDEX \
                    else schema_manager.get_complete_entity_result(token, entity_dict)

    # Determine if the entity is publicly visible base on its data, only.
    # To verify if a Collection is public, it is necessary to have its Datasets, which
    # are populated as triggered data.  So pull back the complete entity for
    # _get_entity_visibility() to check.
    entity_scope = _get_entity_visibility(normalized_entity_type=normalized_entity_type, entity_dict=entity_dict)

    # Initialize the user as authorized if the data is public.  Otherwise, the
    # user is not authorized and credentials must be checked.
    public_entity = False
    has_access = True
    if entity_scope == DataVisibilityEnum.PUBLIC:
        public_entity = True
        user_authorized = True
    else:
        # It's highly possible that there's no token provided
        user_token = get_user_token(request)

        # The user_token is flask.Response on error
        # Without token, the user can only access public collections, modify the collection result
        # by only returning public datasets attached to this collection
        if isinstance(user_token, Response):
            forbidden_error(f"{normalized_entity_type} for {entity_id} is not accessible without presenting a token.")
        else:
            # When the groups token is valid, but the user doesn't belong to HuBMAP-READ group
            # Or the token is valid but doesn't contain group information (auth token or transfer token)
            user_authorized = user_in_hubmap_read_group(request)
    user_token = get_user_token(request)
    if isinstance(user_token, Response):
        has_access = False
    if not user_in_hubmap_read_group(request):
        has_access = False
    # We'll need to return all the properties including those generated by
    # `on_read_trigger` to have a complete result e.g., the 'next_revision_uuid' and
    # 'previous_revision_uuid' being used below.
    # Collections, however, will filter out only public properties for return.
    if not user_authorized:
        forbidden_error(f"The requested {normalized_entity_type} has non-public data."
                        f"  A Globus token with access permission is required.")

    final_result = schema_manager.normalize_document_result_for_response(entity_dict=metadata_dict)

    # Result filtering based on query string
    # The `data_access_level` property is available in all entities Donor/Sample/Dataset
    # and this filter is being used by gateway to check the data_access_level for file assets
    # The `status` property is only available in Dataset and being used by search-api for revision
    result_filtering_accepted_property_keys = ['data_access_level', 'status']

    if bool(request.args):
        property_key = request.args.get('property')

        if property_key is not None:
            # Validate the target property
            if property_key not in result_filtering_accepted_property_keys:
                bad_request_error(f"Only the following property keys are supported in the query string: {COMMA_SEPARATOR.join(result_filtering_accepted_property_keys)}")

            if property_key == 'status' and \
                    not schema_manager.entity_type_instanceof(normalized_entity_type, 'Dataset'):
                bad_request_error(f"Only Dataset or Publication supports 'status' property key in the query string")

            # Response with the property value directly
            # Don't use jsonify() on string value
            
                
            return entity_dict[property_key]
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")
    else:
        # Response with the dict
        if public_entity and has_access is False:
            modified_final_result = schema_manager.exclude_properties_from_response(excluded_fields, final_result)
            return modified_final_result
        return final_result


####################################################################################################
## For local development/testing
####################################################################################################

if __name__ == "__main__":
    try:
        app.run(host='0.0.0.0', port="5002")
    except Exception as e:
        print("Error during starting debug server.")
        print(str(e))
        logger.error(e, exc_info=True)
        print("Error during startup check the log file for further information")

