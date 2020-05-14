'''
Created on May 15, 2019

@author: chb69
'''
from flask import Flask, jsonify, abort, request, make_response, url_for, session, redirect, json, Response
import globus_sdk
from globus_sdk import AccessTokenAuthorizer, TransferClient, AuthClient 
import base64
from globus_sdk.exc import TransferAPIError
import sys
import os
from neo4j import TransactionError, CypherError
from flask_cors import CORS, cross_origin
import argparse
import ast
from specimen import Specimen
from dataset import Dataset

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

@app.route('/entities/donor/<uuid>', methods = ['PUT'])
@secured(groups="HuBMAP-read")
def update_donor(uuid):
    try:
        token = AuthHelper.parseAuthorizationTokens(request.headers)
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
        entity_helper = Entity(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])

        if not isAuthorized(conn, token, entity_helper, uuid):
            return "User has no permission to edit the Donor", 403
        else:
            driver = conn.get_driver()
            specimen = Specimen(app.config)
            entity = Entity.get_entity(driver, uuid)
            if entity.get('entitytype', None) != 'Sample':
                abort(400, "The UUID is not a Sample.")
            new_uuid_record = specimen.update_specimen(driver, uuid, request, request.get_json(), request.files, token)
        conn.close()

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
        entity_helper = Entity(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'], app.config['UUID_WEBSERVICE_URL'])
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])

        if not isAuthorized(conn, token, entity_helper, uuid):
            return "User has no permission to edit the Donor", 403
        else:
            driver = conn.get_driver()
            dataset = Dataset(app.config)
            entity = Entity.get_entity(driver, uuid)
            if entity.get('entitytype', None) != 'Dataset':
                abort(400, "The UUID is not a Dataset.")
            new_uuid = dataset.modify_dataset(driver, token, uuid, request.get_json())
        conn.close()

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