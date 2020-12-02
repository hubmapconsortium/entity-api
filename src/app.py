from flask import Flask, g, jsonify, abort, request, Response, redirect
from neo4j import GraphDatabase
import sys
import os
import re
import json
import requests
from urllib3.exceptions import InsecureRequestWarning
from cachetools import cached, TTLCache
import functools
from pathlib import Path
import logging
import urllib

# Local modules
import app_neo4j_queries
import schema_manager

# HuBMAP commons
from hubmap_commons import string_helper
from hubmap_commons import file_helper

# Set logging fromat and level (default is warning)
# All the API logging is forwarded to the uWSGI server and gets written into the log file `uwsgo-entity-api.log`
# Log rotation is handled via logrotate on the host system with a configuration file
# Do NOT handle log file and rotation via the Python logging to avoid issues with multi-worker processes
logging.basicConfig(format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s', level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S')

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['UUID_API_URL'] = app.config['UUID_API_URL'].strip('/')
app.config['SEARCH_API_URL'] = app.config['SEARCH_API_URL'].strip('/')

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

# LRU Cache implementation with per-item time-to-live (TTL) value
# with a memoizing callable that saves up to maxsize results based on a Least Frequently Used (LFU) algorithm
# with a per-item time-to-live (TTL) value
# Here we use two hours, 7200 seconds for ttl
cache = TTLCache(maxsize=app.config['CACHE_MAXSIZE'], ttl=app.config['CACHE_TTL'])


####################################################################################################
## Neo4j connection
####################################################################################################

# Have the neo4j connection available in the application context (lifetime of a request)
neo4j_driver = GraphDatabase.driver(app.config['NEO4J_URI'], auth = (app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD']))

"""
Get the current neo4j database connection session if exists
Otherwise create a new one

Returns
-------
neo4j.Session object
    The neo4j database connection session
"""
def get_neo4j_session():
    if not hasattr(g, 'neo4j_session'):
        # Once upgrade to neo4j v4, we can specify the target database 
        #g.neo4j_session = neo4j_driver.session(database = app.config['NEO4J_DB'])
        g.neo4j_session = neo4j_driver.session()
    return g.neo4j_session

"""
Prevent from maxing the connection pool (maximum total number of connections allowed per host)
by closing the current neo4j connection session at the end of every request
"""
@app.teardown_appcontext
def close_neo4j_session(error):
    if hasattr(g, 'neo4j_session'):
        g.neo4j_session.close()


####################################################################################################
## Schema initialization
####################################################################################################

try:
    schema_manager.initialize(app.config['SCHEMA_YAML_FILE'], get_neo4j_session())
except IOError as ioe:
    internal_server_error("Failed to load the schema yaml file")
except TypeError as te:
    internal_server_error(te)
 

####################################################################################################
## Globus groups json loading
####################################################################################################

"""
Load the globus groups information json file

Parameters
----------
valid_json_file : file
    A valid json file

Returns
-------
dict
    A dict containing the groups details
"""
@cached(cache)
def load_globus_groups_json_file(valid_json_file):
    with open(valid_json_file) as file:
        groups = json.load(file)

        app.logger.info("Globus groups json file loaded successfully")

        groups_by_id = {}
        groups_by_name = {}
        groups_by_tmc_prefix = {}

        for group in groups:
            if 'name' in group and 'uuid' in group and 'generateuuid' in group and 'displayname' in group and not string_helper.isBlank(group['name']) and not string_helper.isBlank(group['uuid']) and not string_helper.isBlank(group['displayname']):
                group_obj = {
                    'name' : group['name'].lower().strip(), 
                    'uuid' : group['uuid'].lower().strip(),
                    'displayname' : group['displayname'], 
                    'generateuuid': group['generateuuid']
                }

                if 'tmc_prefix' in group:
                    group_obj['tmc_prefix'] = group['tmc_prefix']

                    if 'uuid' in group and 'displayname' in group and not string_helper.isBlank(group['uuid']) and not string_helper.isBlank(group['displayname']):
                        group_info = {}
                        group_info['uuid'] = group['uuid']
                        group_info['displayname'] = group['displayname']
                        group_info['tmc_prefix'] = group['tmc_prefix']
                       
                        groups_by_tmc_prefix[group['tmc_prefix'].upper().strip()] = group_info
                
                groups_by_name[group['name'].lower().strip()] = group_obj
                groups_by_id[group['uuid']] = group_obj

                app.logger.debug("======groups_by_id======")
                app.logger.debug(groups_by_id)

                app.logger.debug("======groups_by_name======")
                app.logger.debug(groups_by_name)

                app.logger.debug("======groups_by_tmc_prefix======")
                app.logger.debug(groups_by_tmc_prefix)
        
        # Wrap the final data
        globus_groups = {
            'by_id': groups_by_id,
            'by_name': groups_by_name,
            'by_tmc_prefix': groups_by_tmc_prefix
        }
        
        return globus_groups

# Have the groups informaiton available in the application context (lifetime of a request)
globus_groups = load_globus_groups_json_file(app.config['GROUPS_JSON_FILE'])


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

    is_connected = app_neo4j_queries.check_connection(get_neo4j_db())
    
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
    app.logger.info(msg)
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

    # Normalize the returned entity_class
    normalized_entity_class = schema_manager.normalize_entity_class(entity_dict['entity_class'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    entity_dict = schema_manager.remove_undefined_entity_properties(normalized_entity_class, entity_dict)

    generated_on_read_trigger_data_dict = schema_manager.generate_triggered_data('on_read_trigger', normalized_entity_class, entity_dict)

    # Merge the entity info and the generated on read data into one dictionary
    result_dict = {**entity_dict, **generated_on_read_trigger_data_dict}

    # Final result
    final_result = result_dict

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
            property_value = result_dict[property_key]

            # Final result
            final_result = property_value
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    # Response with the final result
    return jsonify(final_result)

"""
Show all the supported entity classes

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor

Returns
-------
json
    A list of all the available entity classes defined in the schema yaml
"""
@app.route('/entity_classes', methods = ['GET'])
def get_entity_classes():
    return jsonify(schema_manager.get_all_entity_classes())

"""
Retrive all the entity nodes for a given entity class
Result filtering is supported based on query string
For example: /<entity_class>/entities?property=uuid

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor

Returns
-------
json
    All the entity nodes in a list of the target entity class
"""
@app.route('/<entity_class>/entities', methods = ['GET'])
def get_entities_by_class(entity_class):
    # Normalize user provided entity_class
    normalized_entity_class = schema_manager.normalize_entity_class(entity_class)

    # Validate the normalized_entity_class to enure it's one of the accepted classes
    schema_manager.validate_normalized_entity_class(normalized_entity_class)

    # Get back a list of entity dicts for the given entity class
    entities_list = app_neo4j_queries.get_entities_by_class(get_neo4j_db(), normalized_entity_class)
    
    final_result = entities_list

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
            property_list = app_neo4j_queries.get_entities_by_class(get_neo4j_db(), normalized_entity_class, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    # Response with the final result
    return jsonify(final_result)

"""
Create an entity (new or derived) of the target class in neo4j

Parameters
----------
entity_class : str
    One of the target entity classes (case-insensitive since will be normalized): Dataset, Collection, Sample, but NOT Donor or Collection

    json body for creating new entity:
    {
        "source_entities": null or [],
        "target_entity": {
            all the standard properties defined in schema yaml for the target class...
        }
    }

    json body for creating derived entity:
    {
        "source_entities": [
            {"class": "Sample", "id": "44324234"},
            {"class": "Sample", "id": "6adsd230"},
            ...
        ],
        "target_entity": {
            all the standard properties defined in schema yaml for the target class...
        }
    }

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
    schema_manager.validate_normalized_entity_class(normalized_entity_class)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # When 'source_entities' appears in request json, it means to create a derived entity
    # from the source entities
    if ('source_entities' not in json_data_dict) or ('target_entity' not in json_data_dict):
        bad_request_error("Incorrect json structure. The json request must contain 'source_entities' and 'target_entity'.")
    
    if len(json_data_dict) > 2:
        bad_request_error("Incorrect json structure. The json request must not contain keys other than 'source_entities' and 'target_entity'.")
    
    if json_data_dict['source_entities'] is None:
        # Only pass in the target entity dict
        entity_dict = create_new_entity(normalized_entity_class, json_data_dict['target_entity'])
    else:
        entity_dict = create_derived_entity(normalized_entity_class, json_data_dict)

    return jsonify(entity_dict)

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

    # Get the uuid of the entity for later use
    entity_uuid = entity_dict['uuid']

    # Validate request json against the yaml schema
    # Pass in the entity_dict for missing required key check, this is different from creating new entity
    schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_class, existing_entity_dict = entity_dict)

    generated_on_update_trigger_data_dict = schema_manager.generate_triggered_data('on_update_trigger', normalized_entity_class, entity_dict)

    # Merge two dictionaries
    merged_dict = {**json_data_dict, **generated_on_update_trigger_data_dict}

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

    app.logger.debug("======update entity with json_list_str======")
    app.logger.debug(json_list_str)

    # Update the exisiting entity
    result_dict = app_neo4j_queries.update_entity(get_neo4j_db(), normalized_entity_class, escaped_json_list_str, entity_uuid)

    # Get rid of the entity node properties that are not defined in the yaml schema
    result_dict = schema_manager.remove_undefined_entity_properties(normalized_entity_class, result_dict)

    # How to handle reindex collection?
    # Also reindex the updated entity node in elasticsearch via search-api
    reindex_entity(entity_uuid)

    return jsonify(result_dict)

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
	auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    hubmap_ids = schema_manager.get_hubmap_ids(app.config['UUID_API_URL'], id, token)
    uuid = hubmap_ids['hmuuid']

    ancestors_list = app_neo4j_queries.get_ancestors(get_neo4j_db(), uuid)

    # Final result
    final_result = ancestors_list

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
            property_list = app_neo4j_queries.get_ancestors(get_neo4j_db(), uuid, property_key)

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
	auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    hubmap_ids = schema_manager.get_hubmap_ids(app.config['UUID_API_URL'], id, token)
    uuid = hubmap_ids['hmuuid']

    descendants_list = app_neo4j_queries.get_descendants(get_neo4j_db(), uuid)

    # Final result
    final_result = descendants_list

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
            property_list = app_neo4j_queries.get_descendants(get_neo4j_db(), uuid, property_key)

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
	auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    hubmap_ids = schema_manager.get_hubmap_ids(app.config['UUID_API_URL'], id, token)
    uuid = hubmap_ids['hmuuid']

    parents_list = app_neo4j_queries.get_parents(get_neo4j_db(), uuid)

    # Final result
    final_result = parents_list

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
            property_list = app_neo4j_queries.get_parents(get_neo4j_db(), uuid, property_key)

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
	auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    hubmap_ids = schema_manager.get_hubmap_ids(app.config['UUID_API_URL'], id, token)
    uuid = hubmap_ids['hmuuid']

    children_list = app_neo4j_queries.get_children(get_neo4j_db(), uuid)

    # Final result
    final_result = children_list

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
            property_list = app_neo4j_queries.get_children(get_neo4j_db(), uuid, property_key)

            # Final result
            final_result = property_list
        else:
            bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    return jsonify(final_result)

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

    group_ids = globus_groups['by_id']
    
    # 'data_access_level' is always available since it's transint property
    data_access_level = entity_dict['data_access_level']

    if not 'group_uuid' in entity_dict or string_helper.isBlank(entity_dict['group_uuid']):
        internal_server_error("Group uuid not set for dataset with id: " + id)

    #look up the Component's group ID, return an error if not found
    data_group_id = entity_dict['group_uuid']
    if not data_group_id in group_ids:
        internal_server_error("Can not find dataset group: " + data_group_id + " for id: " + id)

    # Get the user information (if available) for the caller
    # getUserDataAccessLevel will return just a "data_access_level" of public
    # if no auth token is found
    auth_helper = schema_manager.init_auth_helper()
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
        dir_path = dir_path + access_dir + group_ids[data_group_id]['displayname'] + "/"
    #protected access
    elif user_access_level == ACCESS_LEVEL_PROTECTED and data_access_level == ACCESS_LEVEL_PROTECTED:
        globus_server_uuid = app.config['GLOBUS_PROTECTED_ENDPOINT_UUID']
        access_dir = access_level_prefix_dir(app.config['PROTECTED_DATA_SUBDIR'])
        dir_path = dir_path + access_dir + group_ids[data_group_id]['displayname'] + "/"
        
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
Create a new entity node of the target class in neo4j

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor
json_data_dict: dict
    The json request dict of "target_entity" key

Returns
-------
dict
    A dict of the newly created entity
"""
def create_new_entity(normalized_entity_class, json_data_dict):
    # Validate request json against the yaml schema
    schema_manager.validate_json_data_against_schema(json_data_dict, normalized_entity_class)

    # For new dataset to be linked to existing collections
    collection_uuids_list = []
    if normalized_entity_class == "Dataset":
        if 'collection_uuids' in json_data_dict:
            collection_uuids_list = json_data_dict['collection_uuids']

        # Check existence of those collections
        for collection_uuid in collection_uuids_list:
            collection_dict = query_target_entity(collection_uuid)

    # Dictionaries to be merged and passed to trigger methods
    auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    user_info_dict = schema_manager.get_user_info(auth_helper, request)
    new_ids_dict = schema_manager.create_hubmap_ids(app.config['UUID_API_URL'], normalized_entity_class, token)

    # Merge all the above dictionaries and pass to the trigger methods
    data_dict = {**user_info_dict, **new_ids_dict}

    generated_on_create_trigger_data_dict = schema_manager.generate_triggered_data('on_create_trigger', normalized_entity_class, data_dict)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_on_create_trigger_data_dict}

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.debug("======create_new_entity() with escaped_json_list_str======")
    app.logger.debug(escaped_json_list_str)

    # Create new entity
    # If `collection_uuids_list` is not an empty list, meaning the target entity is Dataset and 
    # we'll be also creating relationships between the new dataset node to the existing collection nodes
    result_dict = app_neo4j_queries.create_entity(get_neo4j_db(), normalized_entity_class, escaped_json_list_str, collection_uuids_list = collection_uuids_list)

    return result_dict

"""
Create a derived entity from the given source entity in neo4j

Parameters
----------
target_entity_class : str
    One of the target entity classes (case-insensitive since will be normalized): Dataset, Collection, Sample, but NOT Donor or Collection
json_data_dict: dict
    The json request dict

    json body:
    {
        "source_entities": [
            {"class": "Sample", "id": "44324234"},
            {"class": "Sample", "id": "6adsd230"},
            ...
        ],
        "target_entity": {
            all the standard properties defined in schema yaml for the target class...
        }
    }

Returns
-------
dict
    A dict of the newly created entity
"""
def create_derived_entity(normalized_target_entity_class, json_data_dict):
    # Donor and Collection can not be the target derived entity classes
    schema_manager.validate_target_entity_class_for_derivation(normalized_target_entity_class)

    # Ensure it's a list
    if not isinstance(json_data_dict['source_entities'], list):
        bad_request_error("The 'source_entities' in json request must be an array.")

    source_entities_list = json_data_dict['source_entities']

    for source_entity in source_entities_list:
        if (not 'class' in source_entity) or (not 'id' in source_entity):
            bad_request_error("Each source entity object within the 'source_entities' array must contain 'class' key and 'id' key")
            
        # Also normalize and validate the source entity class
        normalized_source_entity_class = schema_manager.normalize_entity_class(source_entity['class'])
        schema_manager.validate_source_entity_class_for_derivation(normalized_source_entity_class)

        # Query source entity against uuid-api and neo4j and return as a dict if exists
        source_entity_dict = query_target_entity(source_entity['id'])
        
        # Add the uuid to the source_entity dict of each source for later use
        source_entity['uuid'] = source_entity_dict['uuid']
        # Then delete the 'id' key from each source enity dict
        del source_entity['id']

    # Validate target entity data against the yaml schema
    schema_manager.validate_json_data_against_schema(json_data_dict['target_entity'], normalized_target_entity_class)

    # For derived Dataset to be linked with existing Collections
    collection_uuids_list = []
    if normalized_target_entity_class == 'Dataset':
        if 'collection_uuids' in json_data_dict:
            collection_uuids_list = json_data_dict['collection_uuids']

        # Check existence of those collections
        for collection_uuid in collection_uuids_list:
            collection_dict = query_target_entity(collection_uuid)

    # Dictionaries to be merged and passed to trigger methods
    auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    user_info_dict = schema_manager.get_user_info(request)
    new_ids_dict = schema_manager.create_hubmap_ids(app.config['UUID_API_URL'], normalized_entity_class, token)

    # Merge all the above dictionaries and pass to the trigger methods
    data_dict = {**user_info_dict, **new_ids_dict}

    generated_on_create_trigger_data_dict = schema_manager.generate_triggered_data('on_create_trigger', normalized_target_entity_class, data_dict)

    # Merge two dictionaries
    merged_dict = {**json_data_dict, **generated_on_create_trigger_data_dict}

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.debug("======create_derived_entity() with escaped_json_list_str======")
    app.logger.debug(escaped_json_list_str)

    # For Activity creation.
    # Activity is not an Entity, thus we use "class" for reference
    normalized_activity_class = 'Activity'

    # Get trigger generated data for Activity
    # Dictionaries to be merged and passed to trigger methods
    normalized_entity_class_dict = {'normalized_entity_class': normalized_target_entity_class}

    auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    new_ids_dict_for_activity = schema_manager.create_hubmap_ids(app.config['UUID_API_URL'], normalized_entity_class, token)

    # Merge all the above dictionaries and pass to the trigger methods
    # Use normalized_entity_class_dict for building `creation_action` in Activity node later
    data_dict = {**normalized_entity_class_dict, **user_info_dict, **new_ids_dict_for_activity}

    generated_on_create_trigger_data_dict_for_activity = schema_manager.generate_triggered_data('on_create_trigger', normalized_activity_class, data_dict)
    
    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [generated_on_create_trigger_data_dict_for_activity]
    
    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    app.logger.debug("======create_derived_entity() create activity with activity_json_list_str======")
    app.logger.debug(activity_json_list_str)

    # Create the derived entity alone with the Activity node and relationships
    # If `collection_uuids_list` is not an empty list, meaning the target entity is Dataset and 
    # we'll be also creating relationships between the new dataset node to the existing collection nodes
    result_dict = app_neo4j_queries.create_derived_entity(get_neo4j_db(), normalized_target_entity_class, escaped_json_list_str, activity_json_list_str, source_entities_list, collection_uuids_list = collection_uuids_list)

    return result_dict


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
	auth_helper = init_auth_helper()
    token = auth_helper.getProcessSecret()

    hubmap_ids = schema_manager.get_hubmap_ids(app.config['UUID_API_URL'], id, token)
    uuid = hubmap_ids['hmuuid']

    entity_dict = app_neo4j_queries.get_entity(get_neo4j_db(), uuid)

    # Existence check
    if not bool(entity_dict):
        not_found_error("Could not find the entity of id: " + id)

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
            app.logger.info("The search-api has accepted the reindex request for uuid: " + uuid)
        else:
            app.logger.error("The search-api failed to initialize the reindex for uuid: " + uuid)
    except:
        msg = "Failed to send the reindex request to search-api for entity with uuid: " + uuid

        app.logger.error(msg)

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