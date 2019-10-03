#utilities for file and os access

import os
import logging
import globus_sdk
from globus_sdk import AccessTokenAuthorizer, TransferClient, AuthClient 
import configparser
from pprint import pprint
from globus_sdk.exc import TransferAPIError
from hubmap_const import HubmapConst
from hm_auth import AuthCache, AuthHelper

class GlobusFileHelper:
    
    transfer_token = None
    globus_endpoint = None
    
    def __init__(self, transfer_token, globus_endpoint):
        self.transfer_token = transfer_token
        self.globus_endpoint = globus_endpoint
    
    # NOTE: The globus API would return a "No effective ACL rules on the endpoint" error
    # if the file path was wrong.  
    def mkdir(self, new_directory):
        if new_directory == None or len(str(new_directory)) == 0:
            raise ValueError('The dataset UUID must have a value')
        tc = globus_sdk.TransferClient(authorizer=AccessTokenAuthorizer(self.transfer_token))
        try:
            #for entry in tc.operation_ls(ep_id, path="/~/project1/"):
            tc.operation_mkdir(self.globus_endpoint,new_directory)
            print ("Done adding directory: " + new_directory)
            return new_directory
        except TransferAPIError as error:
            if 'MkdirFailed.Exists' not in error.code:
                raise
        except:
            raise
    
    def move_directory(self, dir_UUID, oldpath, newpath):
        if dir_UUID == None or len(str(dir_UUID)) == 0:
            raise ValueError('The dataset UUID must have a value')
        tc = globus_sdk.TransferClient(authorizer=AccessTokenAuthorizer(self.transfer_token))
        try:
            tc.operation_rename(self.globus_endpoint,oldpath=oldpath, newpath=newpath)
            print ("Done moving directory: " + oldpath + " to:" + newpath)
            return str(newpath)            
        except:
            raise
    
    
    def publish_directory(self, dir_UUID):
        try:
            self.move_directory(dir_UUID, self.get_staging_path(dir_UUID), self.get_publish_path(dir_UUID))
            print ("Done publishing directory: " + self.get_publish_path(dir_UUID))
            return self.get_publish_path(dir_UUID)
        except:
            raise
    
    #TODO: This method needs the user's group id
    def get_staging_path(self, uuid):
        return self.staging_file_path
    
    #TODO: This method needs the user's group id
    def get_publish_path(self, uuid):
        return self.publish_file_path
    
    def create_site_directories(self, parent_folder):
        hubmap_groups = AuthCache.getHMGroups()
        for group in hubmap_groups.values():
            self.mkdir(os.path.join(parent_folder , group['uuid']))


if __name__ == "__main__":
    transfer_token = "AgVaVwdp58ojOKD6D8l4jx9yY04P85v529lQrMgr4l5YGmaXx6SbC9Db8JE7m8kgDyMoo2zbyz1DzKSle35j7IWdmo"
    transfer_endpoint = "28bbb03c-a87d-4dd7-a661-7ea2fb6ea631"
    #transfer_endpoint = "fe2c6e95-9dd6-463f-8b1d-e21a3114f1a1"
    gfh = GlobusFileHelper(transfer_token, transfer_endpoint)
    gfh.create_site_directories('/testing/staging')
