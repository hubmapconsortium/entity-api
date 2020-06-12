'''
Created on May 15, 2019

@author: chb69
'''
from flask import Flask, jsonify, abort, request, session, Response
import sys
import os
from neo4j import CypherError
import argparse
from specimen import Specimen
from dataset import Dataset
import requests
import logging
import time
import traceback
import json

# HuBMAP commons
from hubmap_commons.hubmap_const import HubmapConst 
from hubmap_commons.neo4j_connection import Neo4jConnection
from hubmap_commons.uuid_generator import UUID_Generator
from hubmap_commons.hm_auth import AuthHelper, secured, isAuthorized
from hubmap_commons.entity import Entity
from hubmap_commons.autherror import AuthError
from hubmap_commons.provenance import Provenance

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
        token = str(request.headers["AUTHORIZATION"])[7:]
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        ug = UUID_Generator(app.config['UUID_WEBSERVICE_URL'])
        identifier_list = ug.getUUID(token, identifier)
        if len(identifier_list) == 0:
            raise LookupError('unable to find information on identifier: ' + str(identifier))
        if len(identifier_list) > 1:
            raise LookupError('found multiple records for identifier: ' + str(identifier))
        
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
        return Response('Unable to perform query to find identifier: ' + identifier, 500)            
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

@app.route('/collections', methods = ['GET'])
def get_collections():
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()

        collections = Entity.get_collections(driver) 
        return jsonify(collections), 200
    except AuthError as e:
        print(e)
        return Response('token is invalid', 401)
    except LookupError as le:
        print(le)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        return Response('Unable to perform query to find identifier: ' + identifier, 500)

@app.route('/datasets', methods = ['GET'])
def get_datasets():
    if 'collection' in request.args:
        collection_uuid = request.args.get('collection')
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()

        collections = Entity.get_collections(driver) 
        return jsonify(collections), 200
    except AuthError as e:
        print(e)
        return Response('token is invalid', 401)
    except LookupError as le:
        print(le)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        return Response('Unable to perform query to find identifier: ' + identifier, 500)

@app.route('/collections/<identifier>', methods = ['GET'])
def get_collection_children(identifier):
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

        collection_data = Entity.get_entities_and_children_by_relationship(driver, identifier_list[0]['hmuuid'], HubmapConst.IN_COLLECTION_REL) 
        return jsonify( collection_data), 200
    except AuthError as e:
        print(e)
        return Response('token is invalid', 401)
    except LookupError as le:
        print(le)
        return Response(str(le), 404)
    except CypherError as ce:
        print(ce)
        return Response('Unable to perform query to find identifier: ' + identifier, 500)            

@app.route('/collections', methods = ['GET'])
def get_collections():
    global prov_helper
    global logger
    try:
        component = request.args.get('component', default = 'all', type = str)
        if component == 'all':
            stmt = "MATCH (collection:Collection)<-[:IN_COLLECTION]-(dataset) RETURN collection.uuid, collection.label, collection.description, collection.creators, collection.doi_registered, collection.display_doi, apoc.coll.toSet(COLLECT(dataset.uuid)) AS dataset_uuid_list"
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
            stmt = "MATCH (collection:Collection)<-[:IN_COLLECTION]-(dataset:Entity)-[:HAS_METADATA]-(metadata:Metadata {{provenance_group_uuid: '{guuid}'}}) RETURN collection.uuid, collection.label, collection.description, collection.creators, collection.doi_registered, collection.display_doi, apoc.coll.toSet(COLLECT(dataset.uuid)) AS dataset_uuid_list".format(guuid=grp_info[comp]['uuid'])

        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        return_list = []
        with driver.session() as session:
                for record in session.run(stmt):
                    #return_list.append(record)
                    return_list.append(_coll_record_to_json(record))

        return json.dumps(return_list), 200

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

def _coll_record_to_json(record):
                        #collection.uuid
                    #collection.label
                    #collection.description
                    #dataset_uuid_list
    rval = {}                    
    _set_from(record, rval, 'collection.uuid', 'uuid')
    _set_from(record, rval, 'collection.label', 'name')
    _set_from(record, rval, 'dataset_uuid_list', 'dataset_uuids')
    _set_from(record, rval, 'collection.doi_registered', 'doi_registered')
    _set_from(record, rval, 'collection.creators', 'creators')
    _set_from(record, rval, 'collection.display_doi', 'doi_id')
    _set_from(record, rval, 'collection.description', 'description')
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
