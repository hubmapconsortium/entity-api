from flask import Flask, jsonify, abort, request, Response
from neo4j import GraphDatabase, CypherError
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

# Local modules
import neo4j_queries
import schema_triggers

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.provenance import Provenance
from hubmap_commons import string_helper
from hubmap_commons import file_helper

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

# Remove trailing slash / from URL base to avoid "//" caused by config with trailing slash
app.config['UUID_API_URL'] = app.config['UUID_API_URL'].strip('/')
app.config['SEARCH_API_URL'] = app.config['SEARCH_API_URL'].strip('/')

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
            app.logger.error("======schema yaml failed to load======")
            app.logger.error(exc)
   
# Have the schema informaiton available for all API endpoints below
schema = load_provenance_schema_yaml_file(app.config['SCHEMA_YAML_FILE'])


####################################################################################################
## Neo4j connection
####################################################################################################

neo4j_driver = GraphDatabase.driver(app.config['NEO4J_SERVER'], auth = (app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD']))
    

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

    is_connected = neo4j_connection.check_connection(neo4j_driver)
    
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
@app.route('/schema_cache', methods = ['DELETE'])
def delete_schema_cache():
    cache.clear()
    
    msg = "The cache of schema yaml has been deleted."

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
@app.route('/entities/id/<id>', methods = ['GET'])
def get_entity_by_id(id):
    # A list of supported property keys can be used for result filtering in URL query string
    result_filtering_accepted_property_keys = ['data_access_level']

    # Query target entity against neo4j and return as a dict if exists
    entity_dict = query_target_entity(id)

    # Normalize the returned entity_class
    normalized_entity_class = normalize_entity_class(entity_dict['entity_class'])

    # Get rid of the entity node properties that are not defined in the yaml schema
    entity_dict = remove_undefined_entity_properties(normalized_entity_class, entity_dict)

    # Dictionaries to be merged and passed to trigger methods
    normalized_entity_class_dict = {'normalized_entity_class': normalized_entity_class}

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**entity_dict, **normalized_entity_class_dict}

    generated_on_read_trigger_data_dict = generate_triggered_data('on_read_trigger', 'ENTITIES', data_dict)

    # Merge the entity info and the generated on read data into one dictionary
    result_dict = {**entity_dict, **generated_on_read_trigger_data_dict}

    # Final result
    final_result = result_dict

    # Result filtering based on query string
    # For example: /entities/<id>?property=data_access_level
    args = request.args
    if 'property' in args:
        property_key = args['property']

        # Validate the target property
        if property_key not in result_filtering_accepted_property_keys:
            bad_request_error("Unsupported property key specified in the query string")
        
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
    return jsonify(get_all_entity_classes())

"""
Retrive all the entity nodes for a given entity class
Result filtering is supported based on query string
For example: /entities/<entity_class>?property=uuid

Parameters
----------
entity_class : str
    One of the normalized entity classes: Dataset, Collection, Sample, Donor

Returns
-------
json
    All the entity nodes in a list of the target entity class
"""
@app.route('/entities/class/<entity_class>', methods = ['GET'])
def get_entities_by_class(entity_class):
    # A list of supported property keys can be used for result filtering in URL query string
    result_filtering_accepted_property_keys = ['uuid']

    # Normalize user provided entity_class
    normalized_entity_class = normalize_entity_class(entity_class)

    # Validate the normalized_entity_class to enure it's one of the accepted classes
    validate_normalized_entity_class(normalized_entity_class)

    # Get back a list of entity dicts for the given entity class
    entities_list = neo4j_queries.get_entities_by_class(get_neo4j_driver(), normalized_entity_class)
    
    final_result = entities_list

    # Result filtering based on query string
    # For example: /entities/<entity_class>?property=uuid
    args = request.args
    if 'property' in args:
        property_key = args['property']

        # Validate the target property
        if property_key not in result_filtering_accepted_property_keys:
            bad_request_error("Unsupported property key specified in the query string")
        
        # Only return a list of the filtered property value of each entity
        property_list = neo4j_queries.get_entities_by_class(get_neo4j_driver(), normalized_entity_class, property_key)

        # Final result
        final_result = property_list
    else:
        bad_request_error("The specified query string is not supported. Use '?property=<key>' to filter the result")

    # Response with the final result
    return jsonify(final_result)

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
@app.route('/entities/<entity_class>', methods = ['POST'])
def create_entity(entity_class):
    # Normalize user provided entity_class
    normalized_entity_class = normalize_entity_class(entity_class)

    # Validate the normalized_entity_class to make sure it's one of the accepted classes
    validate_normalized_entity_class(normalized_entity_class)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, 'ENTITIES', normalized_entity_class)

    # For new dataset to be linked to existing collections
    collection_uuids_list = []
    if normalized_entity_class == "Dataset":
        if 'collection_uuids' in json_data_dict:
            collection_uuids_list = json_data_dict['collection_uuids']

        # Check existence of those collections
        for collection_uuid in collection_uuids_list:
            collection_dict = query_target_entity(collection_uuid)

    # Dictionaries to be merged and passed to trigger methods
    normalized_entity_class_dict = {'normalized_entity_class': normalized_entity_class}
    user_info_dict = get_user_info(request)
    new_ids_dict = create_new_ids(normalized_entity_class)

    # Merge all the above dictionaries and pass to the trigger methods
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_class_dict, **user_info_dict, **new_ids_dict}

    generated_on_create_trigger_data_dict = generate_triggered_data('on_create_trigger', 'ENTITIES', data_dict)

    # Merge the user json data and generated trigger data into one dictionary
    merged_dict = {**json_data_dict, **generated_on_create_trigger_data_dict}

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.info("======create_entity() with escaped_json_list_str======")
    app.logger.info(escaped_json_list_str)

    # Create new entity
    # If `collection_uuids_list` is not an empty list, meaning the target entity is Dataset and 
    # we'll be also creating relationships between the new dataset node to the existing collection nodes
    result_dict = neo4j_queries.create_entity(get_neo4j_driver(), normalized_entity_class, escaped_json_list_str, collection_uuids_list = collection_uuids_list)

    return jsonify(result_dict)

"""
Create a derived entity from the given source entity in neo4j

Parameters
----------
target_entity_class : str
    One of the target entity classes (case-insensitive since will be normalized): Dataset, Collection, Sample, but NOT Donor or Collection

JSON body:
{
    "source_entities": [
        {"class": "Sample", "id": "44324234"},
        {"class": "Sample", "id": "6adsd230"},
        ...
    ],
    all other standard properties...
}

Returns
-------
json
    All the properties of the newly created entity
"""
@app.route('/derived/<target_entity_class>', methods = ['POST'])
def create_derived_entity(target_entity_class):
    # Normalize user provided target_entity_class
    normalized_target_entity_class = normalize_entity_class(target_entity_class)

    # Donor and Collection can not be the target derived entity classes
    validate_target_entity_class_for_derivation(normalized_target_entity_class)

    # Always expect a json body
    require_json(request)

    # Parse incoming json string into json data(python dict object)
    json_data_dict = request.get_json()

    if not 'source_entities' in json_data_dict:
        bad_request_error("Key 'source_entities' is missing from the JSON request body")

    source_entities_list = json_data_dict['source_entities']

    for source_entity in source_entities_list:
        if (not 'class' in source_entity) or (not 'id' in source_entity):
            bad_request_error("Each source entity object within the 'source_entities' list must contain 'class' key and 'id' key")
            
        # Also normalize and validate the source entity class
        normalized_source_entity_class = normalize_entity_class(source_entity['class'])
        validate_source_entity_class_for_derivation(normalized_source_entity_class)

        # Query source entity against neo4j and return as a dict if exists
        source_entity_dict = query_target_entity(source_entity['id'])
        
        # Add the uuid to the source_entity dict of each source for later use
        source_entity['uuid'] = source_entity_dict['uuid']
        # Then delete the 'id' key from each source enity dict
        del source_entity['id']

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, 'ENTITIES', normalized_target_entity_class)

    # For derived Dataset to be linked with existing Collections
    collection_uuids_list = []
    if normalized_target_entity_class == 'Dataset':
        if 'collection_uuids' in json_data_dict:
            collection_uuids_list = json_data_dict['collection_uuids']

        # Check existence of those collections
        for collection_uuid in collection_uuids_list:
            collection_dict = query_target_entity(collection_uuid)

    # Dictionaries to be merged and passed to trigger methods
    normalized_entity_class_dict = {'normalized_entity_class': normalized_target_entity_class}
    user_info_dict = get_user_info(request)
    new_ids_dict = create_new_ids(normalized_target_entity_class)

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_class_dict, **user_info_dict, **new_ids_dict}

    generated_on_create_trigger_data_dict = generate_triggered_data('on_create_trigger', 'ENTITIES', data_dict)

    # Merge two dictionaries
    merged_dict = {**json_data_dict, **generated_on_create_trigger_data_dict}

    # `UNWIND` in Cypher expects List<T>
    data_list = [merged_dict]
    
    # Convert the list (only contains one entity) to json list string
    json_list_str = json.dumps(data_list)

    # Must also escape single quotes in the json string to build a valid Cypher query later
    escaped_json_list_str = json_list_str.replace("'", r"\'")

    app.logger.info("======create derived entity with escaped_json_list_str======")
    app.logger.info(escaped_json_list_str)

    # For Activity creation.
    # Activity is not an Entity, thus we use "class" for reference
    normalized_activity_class = 'Activity'

    # Dictionaries to be merged and passed to trigger methods
    normalized_activity_class_dict = {'normalized_activity_class': normalized_activity_class}

    # Create new ids for the Activity node
    new_ids_dict_for_activity = create_new_ids(normalized_activity_class)

    # Build a merged dict for Activity
    data_dict_for_activity = {**normalized_activity_class_dict, **user_info_dict, **new_ids_dict_for_activity}

    # Get trigger generated data for Activity
    generated_on_create_trigger_data_dict_for_activity = generate_triggered_data('on_create_trigger', 'ACTIVITIES', data_dict_for_activity)
    
    # `UNWIND` in Cypher expects List<T>
    activity_data_list = [generated_on_create_trigger_data_dict_for_activity]
    
    # Convert the list (only contains one entity) to json list string
    activity_json_list_str = json.dumps(activity_data_list)

    app.logger.info("======create activity with activity_json_list_str======")
    app.logger.info(activity_json_list_str)

    # Create the derived entity alone with the Activity node and relationships
    # If `collection_uuids_list` is not an empty list, meaning the target entity is Dataset and 
    # we'll be also creating relationships between the new dataset node to the existing collection nodes
    result_dict = neo4j_queries.create_derived_entity(get_neo4j_driver(), normalized_target_entity_class, escaped_json_list_str, activity_json_list_str, source_entities_list, collection_uuids_list = collection_uuids_list)

    return jsonify(result_dict)


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
    normalized_entity_class = normalize_entity_class(entity_dict['entity_class'])

    # Get the uuid of the entity for later use
    entity_uuid = entity_dict['uuid']

    # Validate request json against the yaml schema
    validate_json_data_against_schema(json_data_dict, 'ENTITIES', normalized_entity_class)

    # Dictionaries to be merged and passed to trigger methods
    normalized_entity_class_dict = {'normalized_entity_class': normalized_entity_class}
    user_info_dict = get_user_info(request)

    # Merge all the above dictionaries
    # If the latter dictionary contains the same key as the previous one, it will overwrite the value for that key
    data_dict = {**normalized_entity_class_dict, **user_info_dict}

    generated_on_update_trigger_data_dict = generate_triggered_data('on_update_trigger', 'ENTITIES', data_dict)

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

    app.logger.info("======update entity with json_list_str======")
    app.logger.info(json_list_str)

    # Update the exisiting entity
    result_dict = neo4j_queries.update_entity(get_neo4j_driver(), normalized_entity_class, escaped_json_list_str, entity_uuid)

    # Get rid of the entity node properties that are not defined in the yaml schema
    result_dict = remove_undefined_entity_properties(normalized_entity_class, result_dict)

    # How to handle reindex collection?
    # Also reindex the updated entity node in elasticsearch via search-api
    reindex_entity(entity_uuid)

    return jsonify(result_dict)


"""
Get all ancestors by uuid

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
    uuid = get_target_uuid(id)
    ancestors_list = neo4j_queries.get_ancestors(get_neo4j_driver(), uuid)
    return jsonify(ancestors_list)

"""
Get all descendants by uuid

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
    uuid = get_target_uuid(id)
    descendants_list = neo4j_queries.get_descendants(get_neo4j_driver(), uuid)
    return jsonify(descendants_list)

"""
Get all parents by uuid

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
    uuid = get_target_uuid(id)
    parents_list = neo4j_queries.get_parents(get_neo4j_driver(), uuid)
    return jsonify(parents_list)

"""
Get all chilren by uuid

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
    uuid = get_target_uuid(id)
    children_list = neo4j_queries.get_children(get_neo4j_driver(), uuid)
    return jsonify(children_list)


"""
Redirect doi?

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity
"""
@app.route('/doi/redirect/<id>', methods = ['GET'])
def doi_redirect(id):
    return collection_redirect(id)

"""
Redirect a request from a doi service for a collection of data

Parameters
----------
id : str
    The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of given entity
"""
@app.route('/collection/redirect/<id>', methods = ['GET'])
def collection_redirect(id):
    try:
        if string_helper.isBlank(id):
            bad_request_error("No identifier")

        #look up the id, if it doesn't exist return an error
        ug = UUID_Generator(app.config['UUID_API_URL'])
        hmuuid_data = ug.getUUID(AuthHelper.instance().getProcessSecret(), id)    
        if hmuuid_data is None or len(hmuuid_data) == 0:
            not_found_error("Not found")

        if len(hmuuid_data) > 1:
            bad_request_error("Data Collection is defined multiple times")

        uuid_data = hmuuid_data[0]

        if not 'hmuuid' in uuid_data or string_helper.isBlank(uuid_data['hmuuid']) or not 'type' in uuid_data or string_helper.isBlank(uuid_data['type']) or uuid_data['type'].strip().lower() != 'collection':
            not_found_error("Data collection not found")

        if 'COLLECTION_REDIRECT_URL' not in app.config or string_helper.isBlank(app.config['COLLECTION_REDIRECT_URL']):
            internal_server_error("Configuration error")

        redir_url = app.config['COLLECTION_REDIRECT_URL']
        if redir_url.lower().find('<identifier>') == -1:
            internal_server_error("Configuration error")
 
        rep_pattern = re.compile(re.escape('<identifier>'), re.RegexFlag.IGNORECASE)
        redir_url = rep_pattern.sub(uuid_data['hmuuid'], redir_url)
        
        return redirect(redir_url, code = 307)
    except Exception:
        internal_server_error("Unexpected error while redirecting for Collection with id: " + id)


"""
Get the Globus URL to the dataset given a dataset ID

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
@app.route('/entities/dataset/globus-url/<id>', methods = ['GET'])
def get_globus_url(id):
    normalized_entity_class = 'Dataset'
    
    # Get all group (tmc/component/Globus Groups/etc...) info as a dict directly from the
    # hubmap-globus-groups.json file in commons repo
    prov_helper = Provenance(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_API_URL'])
    group_ids = prov_helper.get_group_info_by_id()

    try:
        uuid = get_target_uuid(id)
        
        #look up the dataset in Neo4j and retrieve the allowable data access level (public, protected or consortium)
        #for the dataset and HuBMAP Component ID that the dataset belongs to
        entity_dict = neo4j_queries.get_entity(get_neo4j_driver(), normalized_entity_class, uuid)
        if not 'group_uuid' in entity_dict or string_helper.isBlank(entity_dict['group_uuid']):
            internal_server_error("Group id not set for dataset with uuid: " + uuid)
    
        #if no access level is present on the dataset default to protected
        if not 'data_access_level' in entity_dict or string_helper.isBlank(entity_dict['data_access_level']):
            data_access_level = HubmapConst.ACCESS_LEVEL_PROTECTED
        else:
            data_access_level = entity_dict['data_access_level']
        
        #look up the Component's group ID, return an error if not found
        data_group_id = entity_dict['group_uuid']
        if not data_group_id in group_ids:
            internal_server_error("Dataset group: " + data_group_id + " for uuid:" + uuid + " not found.")

        #get the user information (if available) for the caller
        #getUserDataAccessLevel will return just a "data_access_level" of public
        #if no auth token is found
        auth_helper = init_auth_helper()
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
        if data_access_level == HubmapConst.ACCESS_LEVEL_PUBLIC:
            globus_server_uuid = app.config['GLOBUS_PUBLIC_ENDPOINT_UUID']
            access_dir = access_level_prefix_dir(app.config['PUBLIC_DATA_SUBDIR'])
            dir_path = dir_path +  access_dir + "/"
        #consortium access
        elif data_access_level == HubmapConst.ACCESS_LEVEL_CONSORTIUM and not user_access_level == HubmapConst.ACCESS_LEVEL_PUBLIC:
            globus_server_uuid = app.config['GLOBUS_CONSORTIUM_ENDPOINT_UUID']
            access_dir = access_level_prefix_dir(app.config['CONSORTIUM_DATA_SUBDIR'])
            dir_path = dir_path + access_dir + group_ids[data_group_id]['displayname'] + "/"
        #protected access
        elif user_access_level == HubmapConst.ACCESS_LEVEL_PROTECTED and data_access_level == HubmapConst.ACCESS_LEVEL_PROTECTED:
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
    except HTTPException as hte:
        msg = "HTTPException during get_entity_access_level HTTP code: " + str(hte.get_status_code()) + " " + hte.get_description()

        app.logger.error("======get_globus_url() error======")
        app.logger.error(msg)

        internal_server_error(msg)
    except Exception as e:
        app.logger.error("======get_globus_url() error======")
        app.logger.error(e)

        internal_server_error('Unhandled exception occured during executing get_globus_url()')
    

####################################################################################################
## Internal Functions
####################################################################################################

def get_neo4j_driver():
    if neo4j_driver.closed():
        neo4j_driver = GraphDatabase.driver(app.config['NEO4J_SERVER'], auth = (app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD']))
    
    return neo4j_driver

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
Get a list of all the supported entity classes in the schmea yaml

Returns
-------
list
    A list of entity classes
"""
def get_all_entity_classes():
    dict_keys = schema['ENTITIES'].keys()
    # Need convert the dict_keys object to a list
    return list(dict_keys)

"""
Get a list of entity classes that can be used as derivation source in the schmea yaml

Returns
-------
list
    A list of entity classes
"""
def get_derivation_source_entity_classes():
    derivation_source_entity_classes = []
    entity_classes = get_all_entity_classes()
    for entity_class in entity_classes:
        if schema['ENTITIES'][entity_class]['derivation']['source']:
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
    derivation_target_entity_classes = []
    entity_classes = get_all_entity_classes()
    for entity_class in entity_classes:
        if schema['ENTITIES'][entity_class]['derivation']['target']:
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
Validate the normalized entity class

Parameters
----------
normalized_entity_class : str
    The normalized entity class
"""
def validate_normalized_entity_class(normalized_entity_class):
    separator = ", "
    accepted_entity_classes = get_all_entity_classes()

    # Validate provided entity_class
    if normalized_entity_class not in accepted_entity_classes:
        bad_request_error("The specified entity class in URL must be one of the following: " + separator.join(accepted_entity_classes))

"""
Validate the source and target entity classes for creating derived entity

Parameters
----------
normalized_target_entity_class : str
    The normalized target entity class
"""
def validate_target_entity_class_for_derivation(normalized_target_entity_class):
    separator = ", "
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
    separator = ", "
    accepted_source_entity_classes = get_derivation_source_entity_classes()

    if normalized_source_entity_class not in accepted_source_entity_classes:
        bad_request_error("Invalid source entity class specified for creating the derived entity. Accepted classes: " + separator.join(accepted_source_entity_classes))


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
id : str
    Either the uuid or hubmap_id of target entity 

Returns
-------
str
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
            internal_server_error("unable to find information on identifier: " + id)
        if len(ids_list) > 1:
            internal_server_error("found multiple records for identifier: " + id)
        
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
        msg = "Failed to create new ids via the uuid-api service for during the creation of this new entity"
        
        app.logger.error(msg)
        app.logger.info("response status code: " + str(response.status_code))
        app.logger.info("response text: " + response.text)

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
    # Make a call to uuid-api to get back the uuid
    uuid = get_target_uuid(id)

    try:
        entity_dict = neo4j_queries.get_entity(get_neo4j_driver(), uuid)
    except Exception as e:
        app.logger.info("======Exception from calling neo4j_queries.get_entity()======")
        app.logger.error(e)

        internal_server_error(e)
    except CypherError as ce:
        app.logger.info("======CypherError from calling neo4j_queries.get_entity()======")
        app.logger.error(ce)
        
        internal_server_error(ce)

    # Existence check
    if not bool(entity_dict):
        not_found_error("Could not find the entity of id: " + id)

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
Remove entity node properties that are not defined in the yaml schema prior to response

Parameters
----------
trigger_type : str
    One of the trigger types: on_create_trigger, on_update_trigger, on_read_trigger

data_dict : dict
    A merged dictionary that contains all possible data to be used by the trigger methods

Returns
-------
dict
    A entity dictionary with keys that are all defined in schema yaml
"""
def remove_undefined_entity_properties(normalized_entity_class, entity_dict):
    properties = schema['ENTITIES'][normalized_entity_class]['properties']
    schema_keys = properties.keys() 
    # In Python 3, entity_dict.keys() returns an iterable, which causes error if deleting keys during the loop
    # We can use list to force a copy of the keys to be made
    entity_keys = list(entity_dict)

    for key in entity_keys:
        if key not in schema_keys:
            del entity_dict[key]

    return entity_dict

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
    neo4j_driver_dict = {'neo4j_driver': neo4j_driver}
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
        msg = "Failed to query the user info with the given globus token"

        app.logger.error(msg)

        bad_request_error(msg)

    return user_info

"""
Always expect a json body from user request

request : Flask request object
    The Flask request passed from the API endpoint
"""
def require_json(request):
    if not request.is_json:
        bad_request_error("A JSON body and appropriate Content-Type header are required")

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
