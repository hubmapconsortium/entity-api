'''
Created on May 15, 2019

@author: chb69
'''
from flask import Flask, jsonify, abort, request, session, Response, redirect
import sys
import os
from pathlib import Path
from neo4j import CypherError
import argparse
from specimen import Specimen
from dataset import Dataset
import requests
import logging
import time
import traceback
import json
import urllib
import re

# HuBMAP commons
from hubmap_commons.hubmap_const import HubmapConst 
from hubmap_commons.neo4j_connection import Neo4jConnection
from hubmap_commons.uuid_generator import UUID_Generator
from hubmap_commons.hm_auth import AuthHelper, secured, isAuthorized
from hubmap_commons.entity import Entity
from hubmap_commons.autherror import AuthError
from hubmap_commons.provenance import Provenance
from hubmap_commons.exceptions import HTTPException
from hubmap_commons import string_helper
from hubmap_commons import file_helper
from py2neo import Graph

# For debugging
from pprint import pprint

#from hubmap_commons import HubmapConst, Neo4jConnection, uuid_generator, AuthHelper, secured, Entity, AuthError

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

if AuthHelper.isInitialized() == False:
    authcache = AuthHelper.create(
        app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'])
else:
    authcache = AuthHelper.instance()

logger = ''
prov_helper = None

LOG_FILE_NAME = "../log/entity-api-" + time.strftime("%d-%m-%Y-%H-%M-%S") + ".log"

ALLOWED_SAMPLE_UPDATE_ATTRIBUTES = [HubmapConst.METADATA_ATTRIBUTE]

@app.before_first_request
def init():
    global logger
    global prov_helper
    try:
        logger = logging.getLogger('entity.service')
        logger.setLevel(logging.INFO)
        logFH = logging.FileHandler(LOG_FILE_NAME)
        logger.addHandler(logFH)
        prov_helper = Provenance(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])
        logger.info("started")
    except Exception as e:
        print("Error initializing service")
        print(str(e))
        traceback.print_exc()
        
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

    conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
    driver = conn.get_driver()
    is_connected = conn.check_connection(driver)
    
    if is_connected:
        response_data['neo4j_connection'] = True

    return jsonify(response_data)

@app.route('/entities/types/<type_code>', methods = ['GET'])
# @cross_origin(origins=[app.config['UUID_UI_URL']], methods=['GET'])
def get_entity_by_type(type_code):
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        entity_list = Entity.get_entities_by_type(driver, type_code)
        return jsonify( {'uuids' : entity_list}), 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/entities/types', methods = ['GET'])
# @cross_origin(origins=[app.config['UUID_UI_URL']], methods=['GET'])
def get_entity_types():
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        type_list = Entity.get_entity_type_list(driver)
        return jsonify( {'entity_types' : type_list}), 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/entities/<identifier>/provenance', methods = ['GET'])
# @cross_origin(origins=[app.config['UUID_UI_URL']], methods=['GET'])
def get_entity_provenance(identifier):
    try:
        # default to PUBLIC access only
        public_access_only = True
        token = None
        
        if 'AUTHORIZATION' in request.headers:
            token = request.headers['AUTHORIZATION']
        
        ahelper = AuthHelper.instance()
        user_info = ahelper.getUserDataAccessLevel(request)        

        if user_info[HubmapConst.DATA_ACCESS_LEVEL] != HubmapConst.ACCESS_LEVEL_PUBLIC:
            public_access_only = False
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        ug = UUID_Generator(app.config['UUID_WEBSERVICE_URL'])
        identifier_list = ug.getUUID(AuthHelper.instance().getProcessSecret(), identifier)
        if len(identifier_list) == 0:
            raise LookupError('unable to find information on identifier: ' + str(identifier))
        if len(identifier_list) > 1:
            raise LookupError('found multiple records for identifier: ' + str(identifier))
        current_metadata = Entity.get_entity_metadata(driver, identifier_list[0]['hmuuid'])        
        entity_access_level = HubmapConst.ACCESS_LEVEL_PROTECTED
        if HubmapConst.DATA_ACCESS_LEVEL in current_metadata:
            entity_access_level =  current_metadata[HubmapConst.DATA_ACCESS_LEVEL]
        
        entity_type = None
        if HubmapConst.ENTITY_TYPE_ATTRIBUTE in current_metadata:
            entity_type =  current_metadata[HubmapConst.ENTITY_TYPE_ATTRIBUTE]
        else:
            return Response('Cannot determine entity type for identifier: ' + identifier, 500)
        
        if entity_type == 'Dataset':
            dataset_status = False
            if HubmapConst.DATASET_STATUS_ATTRIBUTE in current_metadata:
                dataset_status =  current_metadata[HubmapConst.DATASET_STATUS_ATTRIBUTE]            
            if (token == None or public_access_only == True) and dataset_status != HubmapConst.DATASET_STATUS_PUBLISHED:
                return Response('The current user does not have the credentials required to retrieve provenance data for: ' + str(identifier), 403)
        else:
            # return a 403 if the user has no token or they are trying to access a non-public object without the necessary access
            if (token == None or public_access_only == True) and entity_access_level != HubmapConst.ACCESS_LEVEL_PUBLIC:
                return Response('The current user does not have the credentials required to retrieve provenance data for: ' + str(identifier), 403)
        depth = None
        if 'depth' in request.args:
            depth = int(request.args.get('depth'))
        
        prov = Provenance(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])

        provenance_data = prov.get_provenance_history(driver, identifier_list[0]['hmuuid'], depth)
        #return jsonify( {'provenance_data' : provenance_data}), 200
        return provenance_data, 200
    except AuthError as e:
        print(e)
        return Response('token is invalid', 401)
    except LookupError as le:
        print(le)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        return Response('Unable to perform query to find identifier: ' + identifier, 500)            
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/entities/<identifier>', methods = ['GET'])
# @cross_origin(origins=[app.config['UUID_UI_URL']], methods=['GET'])
def get_entity(identifier):
    try:
        token = str(request.headers["AUTHORIZATION"])[7:]
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        ug = UUID_Generator(app.config['UUID_WEBSERVICE_URL'])
        identifier_list = ug.getUUID(token, identifier)
        if len(identifier_list) == 0:
            raise LookupError('unable to find information on identifier: ' + str(identifier))
        if len(identifier_list) > 1:
            raise LookupError('found multiple records for identifier: ' + str(identifier))

        entity_node = Entity.get_entity_metadata(driver, identifier_list[0]['hmuuid'])
        return jsonify( {'entity_node' : entity_node}), 200
    except AuthError as e:
        print(e)
        return Response('token is invalid', 401)
    except LookupError as le:
        print(le)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        return Response('Unable to perform query to find identifier: ' + identifier, 500)            
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)


# Get data_access_level for a given entity uuid (Donor/Sample/Dataset)
@app.route('/entity-access-level/<uuid>', methods = ['GET'])
@secured(groups="HuBMAP-read")
def get_entity_access_level(uuid):
    try:
        #get the id from the UUID service to resolve to a UUID and check to make sure that it exists
        ug = UUID_Generator(app.config['UUID_WEBSERVICE_URL'])
        hmuuid_data = ug.getUUID(AuthHelper.instance().getProcessSecret(), uuid)
        if hmuuid_data is None or len(hmuuid_data) == 0:
            return Response("UUID: " + uuid + " not found.", 404)
        
        # If the UUID exists, we go get the data_access_level
        dataset = Dataset(app.config)
        return dataset.get_entity_access_level(uuid)
    except HTTPException as hte:
        msg = "HTTPException during get_entity_access_level HTTP code: " + str(hte.get_status_code()) + " " + hte.get_description() 
        logger.warn(msg, exc_info=True)
        print(msg)
        return Response(hte.get_description(), hte.get_status_code())
    except CypherError as ce:
        msg = 'A Cypher error was encountered when calling dataset.get_entity_access_level(), check log file for detail'
        logger.error(msg, exc_info=True)
        print(msg)
        return Response(msg, 500)
    except:
        msg = 'Unhandled exception occured: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)


@app.route('/entities/uuid/<uuid>', methods = ['GET'])
# @cross_origin(origins=[app.config['UUID_UI_URL']], methods=['GET'])
def get_entity_by_uuid(uuid):
    '''
    get entity by uuid
    '''
    entity = {}
    conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
    driver = conn.get_driver()
    with driver.session() as session:
        try:
            stmt = f'MATCH (e:Entity), (e)-[r1:HAS_METADATA]->(m) WHERE e.uuid=\'{uuid}\' RETURN e, m'
            
            count = 0
            for record in session.run(stmt, uuid=uuid):
                entity.update(record.get('e')._properties)
                entity['metadata'] = {}
                for key, value in record.get('m')._properties.items():
                    entity['metadata'].setdefault(key, value)

                count += 1
            
            if count > 1:
                raise Exception("Two or more entity have same uuid in the Neo4j database.")
            else:
                return jsonify( {'entity' : entity}), 200
        except CypherError as cse:
            print ('A Cypher error was encountered: '+ cse.message)
            raise
        except BaseException as be:
            pprint(be)
            raise be

@app.route('/entities', methods=['GET'])
# @cross_origin(origins=[app.config['UUID_UI_URL']], methods=['GET'])
def get_entities():
    try:
        types = request.args.get('entitytypes').split(',') or ["Donor", "Sample", "Dataset"]
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        entities = Entity.get_entities_by_types(driver, types)
        return jsonify(entities), 200
    except AuthError as e:
        print(e)
        return Response('token is invalid', 401)
    except LookupError as le:
        print(le)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        return Response('Unable to perform query to find entities', 500)            
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/entities/ancestors/<uuid>', methods = ['GET'])
def get_ancestors(uuid):
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        ancestors = Entity.get_ancestors(driver, uuid)
        return jsonify(ancestors), 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/entities/descendants/<uuid>', methods = ['GET'])
def get_descendants(uuid):
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        descendants = Entity.get_descendants(driver, uuid)
        return jsonify(descendants), 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/entities/parents/<uuid>', methods = ['GET'])
def get_parents(uuid):
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        parents = Entity.get_parents(driver, uuid)
        return jsonify(parents), 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/entities/children/<uuid>', methods = ['GET'])
def get_children(uuid):
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        children = Entity.get_children(driver, uuid)
        return jsonify(children), 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

#@app.route('/datasets', methods = ['GET'])
#def get_datasets():
#    if 'collection' in request.args:
#        collection_uuid = request.args.get('collection')
#    try:
#        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
#        driver = conn.get_driver()
#
#        collections = Entity.get_collections(driver) 
#        return jsonify(collections), 200
#    except AuthError as e:
#        print(e)
#        return Response('token is invalid', 401)
#    except LookupError as le:
#        print(le)
#        return Response(str(le), 404)
#    except CypherError as ce:
#        print(ce)
#        return Response('Unable to perform query to find identifier: ' + identifier, 500)


#GET /collections/<uuid> where <uuid> is the id of a collection, either a UUID or HuBMAP ID (like HBM123.ABCD.456)
#is accepted.
#
#An optional Globus nexus can be provided in a standard Authentication Bearer header.  If a valid token
#is provided with group membership in the HuBMAP-Read group any collection matching the id will be returned.
#otherwise if no token is provided or a valid token with no HuBMAP-Read group membership then
#only a public collection will be returned.  Public collections are defined as being published via a DOI 
#(collection.has_doi == true) and at least one of the connected datasets is public
#(dataset.metadata.data_access_level == 'public'). For public collections only connected datasets that are
#public are returned with it.
#
#Returns the information of the Collection specified by the uuid with all connected datasets or 404
#if the Collection is not found.  Returns 401 if a supplied token is invalid. Sample return json here:
#
#
# {
#   "child_metadata_properties": null,
#   "display_doi": "HBM836.XJWD.376",
#   "doi": "836XJWD376",
#   "has_doi": false,
#   "entitytype": "Collection",
#   "hubmap_identifier": null,
#   "items": [
#     {
#       "display_doi": "HBM298.GNPK.468",
#       "doi": "298GNPK468",
#       "entitytype": "Dataset",
#       "properties": {
#         "collection_uuid": "90d56f91cd1478101d273bbfdb8792cf",
#         "creator_email": "joe.smith@testing.edu",
#         "creator_name": "Joe Smith",
#         "data_types": [
#           "AF"
#         ],
#         "dataset_collection_uuid": "90d56f91cd1478101d273bbfdb8792cf",
#         "description": "Autofluorescence Microscopy collected from the Left Kidney of a 53 year old Black or African American Male donor by the Biomolecular Multimodal Imaging Center (BIOMC) at Testing University. BIOMIC is a Tissue Mapping Center that is part of the NIH funded Human Biomolecular Atlas Program (HuBMAP). Microscopy images were collected with a Zeiss Axio Scan.Z1 using 3 channels  DAPI (excitation: 335-383 nm/emission: 420-470 nm), eGFP (excitation: 450-490 nm/emission: 500-550 nm), and dsRed (excitation: 538-562 nm/emission: 570-640 nm). Support was provided by the NIH Common Fund and National Institute of Diabetes and Digestive and Kidney Diseases (U54 DK120058). Tissue was collected through the Cooperative Human Tissue Network with support provided by the NIH National Cancer Institute (5 UM1 CA183727-08).",
#         "entitytype": "Metadata",
#         "globus_directory_url_path": "https://app.globus.org/file-manager?origin_id=0ecd3ce8-bf32-4d64-9405-4ebbcbfff116&origin_path=/Testing%20TMC/8bb9a69f2f2a23841ab207951a2c9803",
#         "group_name": "Testing TMC",
#         "group_uuid": "73bb26e4-ed43-11e8-8f19-0a7c1eab007a",
#         "local_directory_url_path": "/Testing TMC/8bb9a69f2f2a23841ab207951a2c9803",
#         "name": "Autofluorescence Microscopy of Human Kidney Tissue",
#         "phi": "no",
#         "provenance_create_timestamp": 1589336113928,
#         "provenance_group_name": "hubmap-testing-tmc",
#         "provenance_group_uuid": "73bb26e4-ed43-11e8-8f19-0a7c1eab007a",
#         "provenance_modified_timestamp": 1589336113928,
#         "provenance_user_displayname": "Joe Smith",
#         "provenance_user_email": "joe.smith@testing.edu",
#         "reference_uuid": "8bb9a69f2f22342b207951a2c9803",
#         "source_uuid": [
#           "VAN0009-LK-102-7"
#         ],
#         "source_uuids": [
#           "6c84417088840d5fbc0c582b6c86503b"
#         ],
#         "status": "New",
#         "uuid": "16a223f35e1905b972a28d0325b03ede"
#       },
#       "uuid": "8bb9a69f2f2a23841ab207951a2c9803"
#     },
#     {
#       "display_doi": "HBM428.LSVG.824",
#       "doi": "428LSVG824",
#       "entitytype": "Dataset",
#       "properties": {
#         "collection_uuid": "90d56f91cd1478101d273bbfdb8792cf",
#         "creator_email": "joe.smith@testing.edu",
#         "creator_name": "Joe Smith",
#         "data_types": [
#           "PAS"
#         ],
#         "dataset_collection_uuid": "90d56f91cd1478101d273bbfdb8792cf",
#         "description": "Periodic acid-Schiff  Stained Microscopy collected from the Left Kidney of a 53 year old Black or African American Male donor by the Biomolecular Multimodal Imaging Center (BIOMC) at Testing University. BIOMIC is a Tissue Mapping Center that is part of the NIH funded Human Biomolecular Atlas Program (HuBMAP). Brightfield microscopy images of fresh-frozen tissue sections were collected with a Leica Biosystems SCN400. Support was provided by the NIH Common Fund and National Institute of Diabetes and Digestive and Kidney Diseases (U54 DK120058). Tissue was collected through the Cooperative Human Tissue Network with support provided by the NIH National Cancer Institute (5 UM1 CA183727-08).",
#         "entitytype": "Metadata",
#         "globus_directory_url_path": "https://app.globus.org/file-manager?origin_id=0ecd3ce8-bf32-4d64-9405-4ebbcbfff116&origin_path=/Testing%20TMC/f1ede6f2c0f3ec8c347f30a1e9488a91",
#         "group_name": "Testing TMC",
#         "group_uuid": "73bb26e4-ed43-11e8-8f19-0a7c1eab007a",
#         "local_directory_url_path": "/Testing TMC/f1ede6f2c0f3ec8c347f30a1e9488a91",
#         "name": "Periodic acid-Schiff Stained Microscopy of Human Kidney Tissue",
#         "phi": "no",
#         "provenance_create_timestamp": 1589335618254,
#         "provenance_group_name": "hubmap-testing-tmc",
#         "provenance_group_uuid": "73bb26e4-ed43-11e8-8f19-0a7c1eab007a",
#         "provenance_modified_timestamp": 1589335618254,
#         "provenance_user_displayname": "Joe Smith",
#         "provenance_user_email": "jeff.spraggins@testing.edu",
#         "reference_uuid": "f1ede6f2c0f3ec8c347f30a1e9488a91",
#         "source_uuid": [
#           "VAN0009-LK-102-7"
#         ],
#         "source_uuids": [
#           "6c84417088840d5fbc0c582b6c86503b"
#         ],
#         "status": "New",
#         "uuid": "57dbee9728a12cd240ba44216fcf2c75"
#       },
#       "uuid": "f1ede6f2c0f3ec8c347f30a1e9488a91"
#     }
#   ],
#   "properties": null,
#   "uuid": "90d56f91cd1478101d273bbfdb8792cf",
#   "creators": [
#     {
#       "affiliation": "Testing University",
#       "first_name": "Test",
#       "last_name": "User",
#       "middle_name_or_initial": "F",
#       "name": "Test F. User",
#       "orcid_id": "0000-0007-8374-2923"
#     },
#     {
#       "affiliation": "Testing University",
#       "first_name": "Joe",
#       "last_name": "Smith",
#       "middle_name_or_initial": "E",
#       "name": "Joe E Smith",
#       "orcid_id": "0000-0007-3023-2323"
#     },
#     ...
#   ]
# }
@app.route('/collections/<identifier>', methods = ['GET'])
def get_collection_children(identifier):
    try:
        access_level = AuthHelper.instance().getUserDataAccessLevel(request)
        token = AuthHelper.instance().getProcessSecret()
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        ug = UUID_Generator(app.config['UUID_WEBSERVICE_URL'])
        identifier_list = ug.getUUID(token, identifier)
        if len(identifier_list) == 0:
            raise LookupError('unable to find information on identifier: ' + str(identifier))
        if len(identifier_list) > 1:
            raise LookupError('found multiple records for identifier: ' + str(identifier))
        
        collection_data = Entity.get_entities_and_children_by_relationship(driver, identifier_list[0]['hmuuid'], HubmapConst.IN_COLLECTION_REL)
        if access_level['data_access_level'] == 'public':
            public_datasets = []            
            if (collection_data is None or
                'has_doi' not in collection_data or
                not collection_data['has_doi']):
                return Response("Not found", 404) 
            for item in collection_data['items']:
                if 'data_access_level' in item['properties'] and item['properties']['data_access_level'] == 'public':
                    public_datasets.append(item)
            if len(public_datasets) > 0:
                collection_data['items'] = public_datasets
            else:
                return Response("not found", 404)

        return Response(json.dumps(collection_data), 200, mimetype='application/json')
    except HTTPException as hte:
        msg = "HTTPException during get_collection_children: " + str(hte.get_status_code()) + " " + hte.get_description() 
        logger.warn(msg, exc_info=True)
        print(msg)
        return Response(hte.get_description(), hte.get_status_code())
    except AuthError as e:
        print(e)
        return Response('token is invalid', 401)
    except LookupError as le:
        print(le)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        return Response('Unable to perform query to find identifier: ' + identifier, 500)            




#GET /collections with optional argument ?component=<COMP CODE>, where <COMP CODE> is from the table below.
#Returns a list of Collections which include the basic Collection information and the uuids of all connected
#datasets.  If the component argument is omitted all Collections are returned.
#
#An optional Globus nexus can be provided in a standard Authentication Bearer header.  If a valid token
#is provided with group membership in the HuBMAP-Read group all collections matching the input will be
#returned otherwise if no token is provided or a valid token with no HuBMAP-Read group membership then
#only public collections will be returned.  Public collections are defined as being published via a DOI 
#(collection.has_doi == true) and at least one of the connected datasets is public
#(dataset.metadata.data_access_level == 'public'). For public collections only connected datasets that are
#public are returned with it.
#
# 200 Returned with json list of collections (see below), empty list is returned if there are no matches
# 401 Returned if an invalid token is supplied
#
#Sample return json like:
#
# [
#   {
#     "uuid": "90d56f91cd1478101d273bbfdb8792cf",
#     "name": "Multimodal Molecular Imaging of Human Kidney Tissue",
#     "dataset_uuids": [
#       "8bb9a69f2f2a23841ab207951a2c9803",
#       "f1ede6f2c0f3ec8c347f30a1e9488a91"
#     ],
#     "has_doi": true,
#     "creators": "[{\"name\": \"Test User\", \"first_name\": \"Test\", \"last_name\": \"User\", \"middle_name_or_initial\": \"L\", \"orcid_id\": \"0000-0007-9585-9585\", \"affiliation\": \"University of Testing\"}, {\"name\": \"Joe Smith\", \"first_name\": \"Joe\", \"last_name\": \"Smith\", \"middle_name_or_initial\": \"E\", \"orcid_id\": \"0000-0007-2283-8374\", \"affiliation\": \"University of Testing\"}]",
#    "doi_id": "HBM836.XJWD.376",
#     "description": "Mulltimodal molecular imaging data collected from the Left Kidney of a 53 year old Black or African American Male donor by the Biomolecular Multimodal Imaging Center (BIOMC) at Testing University. BIOMIC is a Tissue Mapping Center that is part of the NIH funded Human Biomolecular Atlas Program (HuBMAP). Datasets generated by BIOMIC combine MALDI imaging mass spectrometry with various microscopy modalities including multiplexed immunofluorescence, autofluorescence, and stained microscopy. Support was provided by the NIH Common Fund and National Institute of Diabetes and Digestive and Kidney Diseases (U54 DK120058). Tissue was collected through the Cooperative Human Tissue Network with support provided by the NIH National Cancer Institute (5 UM1 CA183727-08)."
#   },
#   {
#     "uuid": "1ff9aa1193ac810eb84234d9943114dc",
#     "name": "Multimodal Molecular Imaging of Human Kidney Tissue",
#     "dataset_uuids": [
#       "37365995f648d5b20a3fe40d8ea75107",
#       "499aa4b8e249100ccfdec79ed1417440"
#     ],
#     "has_doi": false,
#     "creators": "[{\"name\": \"Test User\", \"first_name\": \"Test\", \"last_name\": \"User\", \"middle_name_or_initial\": \"L\", \"orcid_id\": \"0000-0007-9585-9585\", \"affiliation\": \"University of Testing\"}, {\"name\": \"Joe Smith\", \"first_name\": \"Joe\", \"last_name\": \"Smith\", \"middle_name_or_initial\": \"E\", \"orcid_id\": \"0000-0007-2283-8374\", \"affiliation\": \"University of Testing\"}]",
#     "doi_id": "HBM757.HKPZ.868",
#     "description": "Mulltimodal molecular imaging data collected from the Left Kidney of a 53 year old Black or African American Male donor by the Biomolecular Multimodal Imaging Center (BIOMC) at Testing University. BIOMIC is a Tissue Mapping Center that is part of the NIH funded Human Biomolecular Atlas Program (HuBMAP). Datasets generated by BIOMIC combine MALDI imaging mass spectrometry with various microscopy modalities including multiplexed immunofluorescence, autofluorescence, and stained microscopy. Support was provided by the NIH Common Fund and National Institute of Diabetes and Digestive and Kidney Diseases (U54 DK120058). Tissue was collected through the Cooperative Human Tissue Network with support provided by the NIH National Cancer Institute (5 UM1 CA183727-08)."
#   }
#   ...
#  
# ]
#
# COMP CODES     Component Name
# --------------------------------------------------------
# UFL            University of Florida TMC
# CALT           California Institute of Technology TMC
# VAN            Vanderbilt TMC
# STAN           Stanford TMC
# UCSD           University of California San Diego TMC
# RTIBD          Broad Institute RTI
# RTIGE          General Electric RTI
# RTINW          Northwestern RTI
# RTIST          Stanford RTI
# TTDCT          Cal Tech TTD
# TTDHV          Harvard TTD
# TTDPD          Purdue TT
# TTDST          Stanford TTD
# TEST           IEC Testing Group
@app.route('/collections', methods = ['GET'])
def get_collections():
    global prov_helper
    global logger
    driver = None
    try:
        access_level = AuthHelper.instance().getUserDataAccessLevel(request)        
        component = request.args.get('component', default = 'all', type = str)
        where_clause = "where not collection.has_doi is null "
        if access_level['data_access_level'] == 'public':
            where_clause = "where collection.has_doi = True and metadata.data_access_level = 'public' "
        if component == 'all':
            stmt = "MATCH (collection:Collection)<-[:IN_COLLECTION]-(dataset:Entity)-[:HAS_METADATA]->(metadata:Metadata) " + where_clause + "RETURN collection.uuid, collection.label, collection.description, collection.creators, collection.has_doi, collection.display_doi, collection.doi_url, collection.registered_doi, collection.provenance_create_timestamp, collection.provenance_modified_timestamp, collection.provenance_user_displayname, apoc.coll.toSet(COLLECT(dataset.uuid)) AS dataset_uuid_list"
        else:
            grp_info = prov_helper.get_groups_by_tmc_prefix()
            comp = component.strip().upper()
            if not comp in grp_info:
                valid_comps = ""
                comma = ""
                first = True
                for key in grp_info.keys():
                    valid_comps = valid_comps + comma + grp_info[key]['tmc_prefix'] + " (" + grp_info[key]['displayname'] + ")"
                    if first:
                        comma = ", "
                        first = False
                return Response("Invalid component code: " + component + " Valid values: " + valid_comps, 400)
            query = "MATCH (collection:Collection)<-[:IN_COLLECTION]-(dataset:Entity)-[:HAS_METADATA]->(metadata:Metadata {{provenance_group_uuid: '{guuid}'}}) " + where_clause + "RETURN collection.uuid, collection.label, collection.description, collection.creators, collection.has_doi, collection.display_doi, collection.registered_doi, collection.doi_url, collection.provenance_create_timestamp, collection.provenance_modified_timestamp, collection.provenance_user_displayname, apoc.coll.toSet(COLLECT(dataset.uuid)) AS dataset_uuid_list"
            stmt = query.format(guuid=grp_info[comp]['uuid'])

        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        return_list = []
        with driver.session() as session:
            for record in session.run(stmt):
                #return_list.append(record)
                return_list.append(_coll_record_to_json(record))

        return Response(json.dumps(return_list), 200, mimetype='application/json')

    except HTTPException as hte:
        msg = "HTTPException during get_collections: " + str(hte.get_status_code()) + " " + hte.get_description() 
        logger.warn(msg, exc_info=True)
        print(msg)
        return Response(hte.get_description(), hte.get_status_code())
    except AuthError as e:                                                                                                                        
        print(e)
        return Response('invalid token access', 401)
    except LookupError as le:
        print(le)
        logger.error(le, exc_info=True)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        logger.error(ce, exc_info=True)
        return Response('Unable to perform query to find collections', 500)
    except Exception as e:
        print ('A general error occurred. Check log file.')
        logger.error(e, exc_info=True)
        return Response('Unhandled exception occured', 500)
    finally:
        if not driver is None:
            driver.close()
            
def _coll_record_to_json(record):
                        #collection.uuid
                    #collection.label
                    #collection.description
                    #dataset_uuid_list
    rval = {}                    
    _set_from(record, rval, 'collection.uuid', 'uuid')
    _set_from(record, rval, 'collection.label', 'name')
    _set_from(record, rval, 'dataset_uuid_list', 'dataset_uuids')
    _set_from(record, rval, 'collection.has_doi', 'has_doi')
    _set_from(record, rval, 'collection.creators', 'creators')
    _set_from(record, rval, 'collection.display_doi', 'doi_id')
    _set_from(record, rval, 'collection.description', 'description')
    _set_from(record, rval, 'collection.doi_url', 'doi_url')
    _set_from(record, rval, 'collection.provenance_create_timestamp', 'provenance_create_timestamp')
    _set_from(record, rval, 'collection.provenance_modified_timestamp', 'provenance_modified_timestamp')
    _set_from(record, rval, 'collection.provenance_user_displayname', 'provenance_user_displayname')
    _set_from(record, rval, 'collection.registered_doi', 'registered_doi')
    return(rval)

def _set_from(src, dest, src_attrib_name, dest_attrib_name = None, default_val = None):
    if dest_attrib_name is None:
        dest_attrib_name = src_attrib_name
    if src[src_attrib_name] is None:
        if not default_val is None:
            dest[dest_attrib_name] = default_val
            return dest
    else:
        dest[dest_attrib_name] = src[src_attrib_name]
        return dest
        
@app.route('/entities/donor/<uuid>', methods = ['PUT'])
@secured(groups="HuBMAP-read")
def update_donor(uuid):
    try:
        token = AuthHelper.parseAuthorizationTokens(request.headers)
        token = token['nexus_token'] if type(token) == dict else token
        entity_helper = Entity(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])

        if not isAuthorized(conn, token, entity_helper, uuid):
            return "User has no permission to edit the Donor", 403
        else:
            driver = conn.get_driver()
            specimen = Specimen(app.config)
            entity = Entity.get_entity(driver, uuid)
            if entity.get('entitytype', None) != 'Donor':
                abort(400, "The UUID is not a Donor.")
            new_uuid_record = specimen.update_specimen(driver, uuid, request, request.get_json(), request.files, token)
        conn.close()
        try:
            #reindex this node in elasticsearch
            rspn = requests.put(app.config['SEARCH_WEBSERVICE_URL'] + "/reindex/" + new_uuid_record, headers={'Authorization': 'Bearer '+token})
        except:
            print("Error happened when calling reindex web service")

        return "OK", 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

#update a Sample entity, currently only the Sample.Metadata.metadata attribute
#can be updated.  Input json with top level attribute names matching the attribute
#names as stored in the Sample.Metadata node.
@app.route('/samples/<uuid>', methods = ['PUT'])
@secured(groups="HuBMAP-read")
def update_sample_by_attrib(uuid):
    try:
        token = AuthHelper.parseAuthorizationTokens(request.headers)
        token = token['nexus_token'] if type(token) == dict else token
        entity_helper = Entity(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        if not isAuthorized(conn, token, entity_helper, uuid):
            return "User has no permission to edit the Sample", 403

        response = _update_sample(uuid, request.get_json()) 
        
        if response.status_code == 200:
            try:
                #reindex this node in elasticsearch
                rspn = requests.put(file_helper.removeTrailingSlashURL(app.config['SEARCH_WEBSERVICE_URL']) + "/reindex/" + uuid, headers={'Authorization': 'Bearer '+token})
            except Exception as se:
                print("Error happened when calling reindex web service for Sample with uuid: " + uuid)
                logger.error("Error while reindexing for Sample with uuid:" + uuid, exc_info=True)
        return(response)


    
    except Exception as e:
        logger.error("Unhandled error while updateing sample uuid:" + uuid, exc_info=True)


#helper method to update the Metadata node attached to a 
#sample record.  Currently only the Metadata.metadata attribute
#can be updated
def _update_sample(uuid, record):
    if (HubmapConst.UUID_ATTRIBUTE in record or
        HubmapConst.DOI_ATTRIBUTE in record or
        HubmapConst.DISPLAY_DOI_ATTRIBUTE in record or
        HubmapConst.ENTITY_TYPE_ATTRIBUTE in record):
        raise HTTPException("ID attributes cannot be changed", 400)
    
    not_allowed = []
    for attrib in record.keys():
        if not attrib in ALLOWED_SAMPLE_UPDATE_ATTRIBUTES:
            not_allowed.append(attrib)
            
    if len(not_allowed) > 0:
        return Response("Attribute(s) not allowed: " + string_helper.listToDelimited(not_allowed, " "), 400)
    
        
    save_record = {}
    for attrib in record.keys():
        if attrib == HubmapConst.METADATA_ATTRIBUTE:
            save_record[attrib] = json.dumps(record[attrib])

    graph = Graph(app.config['NEO4J_SERVER'], auth=(app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD']))
    
    rval = graph.run("match(e:Entity {uuid: {uuid}})-[:HAS_METADATA]-(m:Metadata) set m += {params} return e.uuid", uuid=uuid, params=save_record).data()
    if len(rval) == 0:
        return Response("Update failed for Sample with uuid " + uuid + ".  UUID possibly not found.", 400)
    else:
        if rval[0]['e.uuid'] != uuid:
            return Response("Update failed, wrong uuid returned while trying to update Sample with uuid:" + uuid + " returned: " + rval[0]['e.uuid'])  

    return Response("Update Finished", 200)


@app.route('/entities/sample/<uuid>', methods = ['PUT'])
@secured(groups="HuBMAP-read")
def update_sample(uuid):
    try:
        token = AuthHelper.parseAuthorizationTokens(request.headers)
        token = token['nexus_token'] if type(token) == dict else token
        entity_helper = Entity(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])

        if not isAuthorized(conn, token, entity_helper, uuid):
            return "User has no permission to edit the Sample", 403
        else:
            driver = conn.get_driver()
            specimen = Specimen(app.config)
            entity = Entity.get_entity(driver, uuid)
            if entity.get('entitytype', None) != 'Sample':
                abort(400, "The UUID is not a Sample.")
            new_uuid_record = specimen.update_specimen(driver, uuid, request, request.get_json(), request.files, token)
        conn.close()
        try:
            #reindex this node in elasticsearch
            rspn = requests.put(app.config['SEARCH_WEBSERVICE_URL'] + "/reindex/" + new_uuid_record, headers={'Authorization': 'Bearer '+token})
        except:
            print("Error happened when calling reindex web service")
        return "OK", 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)
        
@app.route('/entities/dataset/<uuid>', methods = ['PUT'])
@secured(groups="HuBMAP-read")
def update_dataset(uuid):
    try:
        token = AuthHelper.parseAuthorizationTokens(request.headers)
        token = token['nexus_token'] if type(token) == dict else token
        entity_helper = Entity(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])

        if not isAuthorized(conn, token, entity_helper, uuid):
            return "User has no permission to edit the Dataset", 403
        else:
            driver = conn.get_driver()
            dataset = Dataset(app.config)
            entity = Entity.get_entity(driver, uuid)
            if entity.get('entitytype', None) != 'Dataset':
                abort(400, "The UUID is not a Dataset.")
            new_uuid_record = dataset.modify_dataset(driver, token, uuid, request.get_json())
        conn.close()

        try:
            #reindex this node in elasticsearch
            rspn = requests.put(app.config['SEARCH_WEBSERVICE_URL'] + "/reindex/" + new_uuid_record, headers={'Authorization': 'Bearer '+token})
        except:
            print("Error happened when calling reindex web service")

        return "OK", 200
    except:
        msg = 'An error occurred: '
        for x in sys.exc_info():
            msg += str(x)
        abort(400, msg)

@app.route('/doi/redirect/<identifier>', methods = ['GET'])
def doi_redirect(identifier):
    return collection_redirect(identifier)

#redirect a request from a doi service for a collection of data
@app.route('/collection/redirect/<identifier>', methods = ['GET'])
def collection_redirect(identifier):
    try:
        if string_helper.isBlank(identifier):
            return Response("No identifier", 400)
            #return _redirect_error_response('ERROR: No Data Collection identifier found.')
        
        #look up the id, if it doesn't exist return an error
        ug = UUID_Generator(app.config['UUID_WEBSERVICE_URL'])
        hmuuid_data = ug.getUUID(AuthHelper.instance().getProcessSecret(), identifier)    
        if hmuuid_data is None or len(hmuuid_data) == 0:
            return Response("Not found", 404)
            #return _redirect_error_response("The Data Collection was not found.", "A collection with an id matching " + identifier + " was not found.")
        
        if len(hmuuid_data) > 1:
            return Response("Data Collection is defined multiple times", 400)
            #return _redirect_error_response("The Data Collection is multiply defined.", "The provided collection id has multiple entries id: " + identifier)
        
        uuid_data = hmuuid_data[0]

        if not 'hmuuid' in uuid_data or string_helper.isBlank(uuid_data['hmuuid']) or not 'type' in uuid_data or string_helper.isBlank(uuid_data['type']) or uuid_data['type'].strip().lower() != 'collection':
            return Response("Data collection not found", 404)
            #return _redirect_error_response("A Data Collection was not found.", "A collection entry with an id matching " + identifier + " was not found.")
        
        if 'COLLECTION_REDIRECT_URL' not in app.config or string_helper.isBlank(app.config['COLLECTION_REDIRECT_URL']):
            return Response("Configuration error", 500)
            #return _redirect_error_response("Cannot complete due to a configuration error.", "The COLLECTION_REDIRECT_URL parameter is not found in the application configuration file.")
            
        redir_url = app.config['COLLECTION_REDIRECT_URL']
        if redir_url.lower().find('<identifier>') == -1:
            return Response("Configuration error", 500)
            #return _redirect_error_response("Cannot complete due to a configuration error.", "The COLLECTION_REDIRECT_URL parameter in the application configuration file does not contain the identifier pattern")
    
        rep_pattern = re.compile(re.escape('<identifier>'), re.RegexFlag.IGNORECASE)
        redir_url = rep_pattern.sub(uuid_data['hmuuid'], redir_url)
        
        return redirect(redir_url, code = 307)
    except Exception:
        logger.error("Unexpected error while redirecting for Collection with id: " + identifier, exc_info=True)
        return Response("Unexpected error", 500)
        #return _redirect_error_response("An unexpected error occurred." "An unexpected error occurred while redirecting for Data Collection with id: " + identifier + " Check the Enitity API log file for more information.")


#helper method to show an error message through the ingest
#portal's error display page a brief description of the error
#is a required parameter a more detailed description is an optional parameter 
def _redirect_error_response(description, detail=None):
    if not 'ERROR_PAGE_URL' in  app.config or string_helper.isBlank(app.config['ERROR_PAGE_URL']):
        return Response("Config ERROR.  ERROR_PAGE_URL not in application configuration.")
    
    redir_url = file_helper.removeTrailingSlashURL(app.config['ERROR_PAGE_URL'])
    desc = urllib.parse.quote(description, safe='')
    description_and_details = "?description=" + desc
    if not string_helper.isBlank(detail):
        det = urllib.parse.quote(detail, save='')
        description_and_details = description_and_details + "&details=" + det
    redir_url = redir_url + description_and_details
    return redirect(redir_url, code = 307)    

#get the Globus URL to the dataset given a dataset ID
#
# It will provide a Globus URL to the dataset directory in of three Globus endpoints based on the access
# level of the user (public, consortium or protected), public only, of course, if no token is provided.
# If a dataset isn't found a 404 will be returned. There is a chance that a 500 can be returned, but not
# likely under normal circumstances, only for a misconfigured or failing in some way endpoint.  If the 
# Auth token is provided but is expired or invalid a 401 is returned.  If access to the dataset is not
# allowed for the user (or lack of user) a 403 is returned.
# Inputs via HTTP GET at /entities/dataset/globus-url/<identifier>
#   identifier: The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID via a url path parameter 
#   auth token: (optional) A Globus Nexus token specified in a standard Authorization: Bearer header
#
# Outputs
#   200 with the Globus Application URL to the datasets's directory
#   404 Dataset not found
#   403 Access Forbidden
#   401 Unauthorized (bad or expired token)
#   500 Unexpected server or other error

@app.route('/entities/dataset/globus-url/<identifier>', methods = ['GET'])
def get_globus_url(identifier):
    global prov_helper
    try:
        #get the id from the UUID service to resolve to a UUID and check to make sure that it exists
        ug = UUID_Generator(app.config['UUID_WEBSERVICE_URL'])
        hmuuid_data = ug.getUUID(AuthHelper.instance().getProcessSecret(), identifier)
        if hmuuid_data is None or len(hmuuid_data) == 0:
            return Response("Dataset id:" + identifier + " not found.", 404)
        uuid = hmuuid_data[0]['hmuuid']
        
        #look up the dataset in Neo4j and retrieve the allowable data access level (public, protected or consortium)
        #for the dataset and HuBMAP Component ID that the dataset belongs to
        dset = Dataset(app.config)
        ds_attribs = dset.get_dataset_metadata_attributes(uuid, ['data_access_level', 'provenance_group_uuid'])
        if not 'provenance_group_uuid' in ds_attribs or string_helper.isBlank(ds_attribs['provenance_group_uuid']):
            return Response("Group id not set for dataset with uuid:" + uuid, 500)
    
        #if no access level is present on the dataset default to protected
        if not 'data_access_level' in ds_attribs or string_helper.isBlank(ds_attribs['data_access_level']):
            data_access_level = HubmapConst.ACCESS_LEVEL_PROTECTED
        else:
            data_access_level = ds_attribs['data_access_level']
        
        #look up the Component's group ID, return an error if not found
        data_group_id = ds_attribs['provenance_group_uuid']
        group_ids = prov_helper.get_group_info_by_id()
        if not data_group_id in group_ids:
            return Response("Dataset group: " + data_group_id + " for uuid:" + uuid + " not found.", 500)

        #get the user information (if available) for the caller
        #getUserDataAccessLevel will return just a "data_access_level" of public
        #if no auth token is found
        ahelper = AuthHelper.instance()
        user_info = ahelper.getUserDataAccessLevel(request)        
        
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
                return Response("Unexpected error, data access level could not be found for user trying to access dataset uuid:" + uuid)        
            user_access_level = user_info['data_access_level']

        #public access
        if data_access_level == HubmapConst.ACCESS_LEVEL_PUBLIC:
            globus_server_uuid = app.config['GLOBUS_PUBLIC_ENDPOINT_UUID']
            access_dir = _access_level_prefix_dir(app.config['PUBLIC_DATA_SUBDIR'])
            dir_path = dir_path +  access_dir + "/"
        #consortium access
        elif data_access_level == HubmapConst.ACCESS_LEVEL_CONSORTIUM and not user_access_level == HubmapConst.ACCESS_LEVEL_PUBLIC:
            globus_server_uuid = app.config['GLOBUS_CONSORTIUM_ENDPOINT_UUID']
            access_dir = _access_level_prefix_dir(app.config['CONSORTIUM_DATA_SUBDIR'])
            dir_path = dir_path + access_dir + group_ids[data_group_id]['displayname'] + "/"
        #protected access
        elif user_access_level == HubmapConst.ACCESS_LEVEL_PROTECTED and data_access_level == HubmapConst.ACCESS_LEVEL_PROTECTED:
            globus_server_uuid = app.config['GLOBUS_PROTECTED_ENDPOINT_UUID']
            access_dir = _access_level_prefix_dir(app.config['PROTECTED_DATA_SUBDIR'])
            dir_path = dir_path + access_dir + group_ids[data_group_id]['displayname'] + "/"
            
        if globus_server_uuid is None:
            return Response("Access not granted", 403)   
    
        dir_path = dir_path + uuid + "/"
        dir_path = urllib.parse.quote(dir_path, safe='')
        
        #https://app.globus.org/file-manager?origin_id=28bbb03c-a87d-4dd7-a661-7ea2fb6ea631&origin_path=%2FIEC%20Testing%20Group%2F03584b3d0f8b46de1b629f04be156879%2F
        url = file_helper.ensureTrailingSlashURL(app.config['GLOBUS_APP_BASE_URL']) + "file-manager?origin_id=" + globus_server_uuid + "&origin_path=" + dir_path  
                
        return Response(url, 200)
    
    except HTTPException as hte:
        msg = "HTTPException during get_globus_url HTTP code: " + str(hte.get_status_code()) + " " + hte.get_description() 
        print(msg)
        logger.warn(msg, exc_info=True)
        return Response(hte.get_description(), hte.get_status_code())
    except Exception as e:
        print ('An unexpected error occurred. Check log file.')
        logger.error(e, exc_info=True)
        return Response('Unhandled exception occured', 500)
    
def _access_level_prefix_dir(pth):
    if string_helper.isBlank(pth):
        return('')
    else:
        return(file_helper.ensureTrailingSlashURL(file_helper.ensureBeginningSlashURL(pth)))


# This is for development only
if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-p", "--port")
        args = parser.parse_args()
        port = 5006
        if args.port:
            port = int(args.port)
        app.run(port=port)
    finally:
        pass
