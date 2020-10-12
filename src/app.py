from flask import Flask, jsonify, abort, request, session, Response, redirect
import sys
import os
import yaml
from cachetools import cached, TTLCache
import functools
from pathlib import Path
import logging

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

@app.route('/donor/<id>', methods = ['GET'])
def get_donor(id):
    # Resulting dict
    entity = neo4j_queries.get_entity_by_uuid(neo4j_driver, "Donor", id)

    result = {
        'dataset': entity
    }

    return jsonify(result)


@app.route('/sample/<id>', methods = ['GET'])
def get_sample(id):
    # Resulting dict
    entity = neo4j_queries.get_entity_by_uuid(neo4j_driver, "Sample", id)

    result = {
        'sample': entity
    }

    return jsonify(result)

@app.route('/dataset/<id>', methods = ['GET'])
def get_dataset(id):
    print(schema['ENTITIES']['Dataset']['attributes'])

    # Resulting dict
    entity = neo4j_queries.get_entity_by_uuid(neo4j_driver, "Dataset", id)

    result = {
        'dataset': entity
    }

    return jsonify(result)

@app.route('/dataset', methods = ['PUT'])
def create_dataset():
    # Always expect a json body
    request_json_required(request)

    # Parse incoming json string into json data(python dict object)
    json_data = request.get_json()

    schema_keys = (schema['ENTITIES']['Dataset']['attributes']).keys() 
    print(schema_keys)

    json_data_keys = json_data.keys()

    for key in json_data_keys:
        if key not in schema_keys:
            bad_request_error("Key '" + key + "' in request body json is invalid")

    # Resulting dict
    entity = neo4j_queries.create_entity(neo4j_driver, "Dataset", json_data)

    result = {
        'dataset': entity
    }

    return jsonify(result)

####################################################################################################
## Internal Functions
####################################################################################################

# Initialize AuthHelper (AuthHelper from HuBMAP commons package)
# HuBMAP commons AuthHelper handles "MAuthorization" or "Authorization"
def init_auth_helper():
    if AuthHelper.isInitialized() == False:
        auth_helper = AuthHelper.create(app.config['GLOBUS_APP_ID'], app.config['GLOBUS_APP_SECRET'])
    else:
        auth_helper = AuthHelper.instance()
    
    return auth_helper

# Throws error for 400 Bad Reqeust with message
def bad_request_error(err_msg):
    abort(400, description = err_msg)

# Always expect a json body
def request_json_required(request):
    if not request.is_json:
        bad_request_error("A JSON body and appropriate Content-Type header are required")