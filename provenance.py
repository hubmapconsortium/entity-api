'''
Created on Sep 1, 2019

@author: chb69
'''
from neo4j import TransactionError, CypherError
import os
import sys
from hubmap_const import HubmapConst 
from neo4j_connection import Neo4jConnection
from uuid_generator import getNewUUID
import configparser
import requests
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common-api'))
from hubmap_const import HubmapConst 
from neo4j_connection import Neo4jConnection
from uuid_generator import getNewUUID
from entity import Entity
from hm_auth import AuthHelper, AuthCache

class Provenance:

    md_config = {}
    
    def __init__(self):
        self.load_config_file()
        
    def load_config_file(self):    
        config = configparser.ConfigParser()
        try:
            config.read(os.path.join(os.path.dirname(__file__), '..', 'common-api', 'app.properties'))
            self.md_config['APP_CLIENT_ID'] = config.get('GLOBUS', 'APP_CLIENT_ID')
            self.md_config['APP_CLIENT_SECRET'] = config.get('GLOBUS', 'APP_CLIENT_SECRET')
            self.md_config['STAGING_ENDPOINT_UUID'] = config.get('GLOBUS', 'STAGING_ENDPOINT_UUID')
            self.md_config['PUBLISH_ENDPOINT_UUID'] = config.get('GLOBUS', 'PUBLISH_ENDPOINT_UUID')
            self.md_config['SECRET_KEY'] = config.get('GLOBUS', 'SECRET_KEY')
            #app.config['DEBUG'] = True
        except OSError as err:
            msg = "OS error.  Check config.ini file to make sure it exists and is readable: {0}".format(err)
            print (msg + "  Program stopped.")
            exit(0)
        except configparser.NoSectionError as noSectError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(noSectError)
            print (msg + "  Program stopped.")
            exit(0)
        except configparser.NoOptionError as noOptError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(noOptError)
            print (msg + "  Program stopped.")
            exit(0)
        except SyntaxError as syntaxError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(syntaxError)
            msg = msg + "  Cannot read line: {0}".format(syntaxError.text)
            print (msg + "  Program stopped.")
            exit(0)        
        except AttributeError as attrError:
            msg = "Error reading the config.ini file.  Check config.ini file to make sure it matches the structure in config.ini.example: {0}".format(attrError)
            msg = msg + "  Cannot read line: {0}".format(attrError.text)
            print (msg + "  Program stopped.")
            exit(0)        
        except:
            msg = "Unexpected error:", sys.exc_info()[0]
            print (msg + "  Program stopped.")
            exit(0)

    def get_provenance_data_object(self, token, groupUUID=None):
        provenance_group = None
        try:
            if groupUUID != None:
                provenance_group = self.get_group_by_identifier(groupUUID)
            else:
                #manually find the group id given the current user:
                group_uuid = None
                entity = Entity()
                group_list = entity.get_user_groups(token)
                for grp in group_list:
                    if grp['generateuuid'] == True:
                        groupUUID = grp['uuid']
                        # if provenance_group is already set, this means the user belongs to more than one writable group
                        if provenance_group != None:
                            ValueError('Error: Current user is a member of multiple groups allowed to create new entities.  The user must select which one to use')
                        provenance_group = self.get_group_by_identifier(groupUUID)
                        break    
                if groupUUID == None:
                    raise ValueError('Unauthorized: Current user is not a member of a group allowed to create new entities')
        except ValueError as ve:
            raise ve
        ret_provenance_group = {HubmapConst.PROVENANCE_GROUP_UUID_ATTRIBUTE : groupUUID, 
                                   HubmapConst.PROVENANCE_GROUP_NAME_ATTRIBUTE: provenance_group[HubmapConst.PROVENANCE_GROUP_NAME_ATTRIBUTE]}
        authcache = None
        if AuthHelper.isInitialized() == False:
            authcache = AuthHelper.create(
                self.md_config['appclientid'], self.md_config['appclientsecret'])
        else:
            authcache = AuthHelper.instance()
        userinfo = authcache.getUserInfo(token, True)
        ret_provenance_group[HubmapConst.PROVENANCE_SUB_ATTRIBUTE] = userinfo[HubmapConst.PROVENANCE_SUB_ATTRIBUTE]
        ret_provenance_group[HubmapConst.PROVENANCE_USER_EMAIL_ATTRIBUTE] = userinfo[HubmapConst.PROVENANCE_USER_EMAIL_ATTRIBUTE]
        ret_provenance_group[HubmapConst.PROVENANCE_USER_DISPLAYNAME_ATTRIBUTE] = userinfo[HubmapConst.PROVENANCE_USER_DISPLAYNAME_ATTRIBUTE]
        return ret_provenance_group
    
    
    def get_group_by_identifier(self, identifier):
        if len(identifier) == 0:
            raise ValueError("identifier cannot be blank")
        authcache = None
        if AuthHelper.isInitialized() == False:
            authcache = AuthHelper.create(
                self.md_config['APP_CLIENT_ID'], self.md_config['APP_CLIENT_SECRET'])
        else:
            authcache = AuthHelper.instance()
        groupinfo = authcache.getHuBMAPGroupInfo()
        # search through the keys for the identifier, return the value
        for k in groupinfo.keys():
            if str(k).lower() == str(identifier).lower():
                group = groupinfo.get(k)
                return group
            else:
                group = groupinfo.get(k)
                if str(group['uuid']).lower() == str(identifier).lower():
                    return group
        raise ValueError("cannot find a Hubmap group matching: [" + identifier + "]")
