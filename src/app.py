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

# HuBMAP commons
from hubmap_commons.hubmap_const import HubmapConst 
from hubmap_commons.neo4j_connection import Neo4jConnection
from hubmap_commons.uuid_generator import UUID_Generator
from hubmap_commons.hm_auth import AuthHelper, secured
from hubmap_commons.entity import Entity
from hubmap_commons.autherror import AuthError
from hubmap_commons.provenance import Provenance

# For debugging
from pprint import pprint

#from hubmap_commons import HubmapConst, Neo4jConnection, uuid_generator, AuthHelper, secured, Entity, AuthError

# Specify the absolute path of the instance folder and use the config file relative to the instance path
app = Flask(__name__, instance_path=os.path.join(os.path.abspath(os.path.dirname(__file__)), 'instance'), instance_relative_config=True)
app.config.from_pyfile('app.cfg')

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

@app.route('/entities', methods = ['GET'])
def get_entities():
    """
    Example: /entities?entitytypes=Donor,Sample,Dataset
    """
    entitytypes = request.args.get('entitytypes')
    if entitytypes is None:
        method = "get_entities"
    else:
        if entitytypes == "":
            entitytypes = []
        else:
            entitytypes = entitytypes.split(',')
        method = "get_entities_by_entitytypes"
    
    try:
        conn = Neo4jConnection(app.config['NEO4J_SERVER'], app.config['NEO4J_USERNAME'], app.config['NEO4J_PASSWORD'])
        driver = conn.get_driver()
        args = [driver]
        if entitytypes is not None:
            args.append(entitytypes)
        entities = getattr(Entity, method)(*args)
        return jsonify(entities), 200
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