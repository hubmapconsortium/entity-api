import os
from os import listdir
import secrets
import shutil
import logging
import pathlib
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning
# Don't confuse urllib (Python native library) with urllib3 (3rd-party library, requests also uses urllib3)
#from requests.packages.urllib3.exceptions import InsecureRequestWarning
import hashlib
import os
from werkzeug.utils import secure_filename

# Local modules
from schema import schema_errors

# HuBMAP commons
from hubmap_commons import file_helper

logger = logging.getLogger(__name__)

# Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)


####################################################################################################
## UploadFileHelper
####################################################################################################

ID_CHARS=['2','3','4','5','6','7','8','9','b','c','d','e','g','h','j','k','l','m','n','p','q','r','s','t','v','w','x','z']                 

instance = None

class UploadFileHelper:
    @staticmethod
    def create(upload_temp_dir, upload_dir, uuid_api_url):
        if instance is not None:
            raise Exception("An instance of UploadFileHelper exists already. Use the UploadFileHelper.instance() method to retrieve it.")
        
        return UploadFileHelper(upload_temp_dir, upload_dir, uuid_api_url)

    @staticmethod
    def instance():
        if instance is None:
            raise Exception("An instance of UploadFileHelper does not yet exist. Use UploadFileHelper.create(...) to create a new instance")
        
        return instance

    @staticmethod
    def is_initialized():
        if instance is None:
            return False

        return True

    def __init__(self, upload_temp_dir, upload_dir, uuid_api_url):
        self.upload_temp_dir = file_helper.ensureTrailingSlash(upload_temp_dir)
        self.upload_dir = file_helper.ensureTrailingSlash(upload_dir)
        self.uuid_api_url = uuid_api_url
    
    #def clean_temp_dir(self):
    #    for dirname in os.listdir(self.upload_temp_dir):
    #        dirpath = self.upload_temp_dir + dirname
    #        if os.path.isdir(dirpath):
    #            shutil.rmtree(dirpath)
    
    def save_temp_file(self, file):
        temp_id = self.__get_temp_file_id()
        file_dir = self.__get_temp_file_dir(temp_id)

        # Use pathlib to create dir instead of file_helper.mkDir
        pathlib.Path(file_dir).mkdir(parents=True, exist_ok=True)

        file.save(os.path.join(file_dir, secure_filename(file.filename)))
        
        return temp_id
    
    def __get_temp_file_dir(self, temp_id):
        return self.upload_temp_dir + temp_id + os.sep
    
    def __get_temp_file_id(self, iteration=0):
        if iteration == 100:
            raise Exception("Unable to get a temporary file id after 100 attempts")
        rid = ''
        for _ in range(20):                                                                                                                
            rid = rid + secrets.choice(ID_CHARS)
        while os.path.exists(self.__get_temp_file_dir(rid)):
            rid = self.get_temp_file_id(iteration = iteration + 1)
        
        return rid
    
    
    def commit_file(self, temp_file_id, entity_uuid, user_token):
        logger.debug(temp_file_id)

        file_temp_dir = self.__get_temp_file_dir(temp_file_id.strip())
        if not os.path.exists(file_temp_dir):
            raise schema_errors.FileUploadException("Temporary file with id " + temp_file_id + " does not have a temp directory.")
        
        fcount = 0
        temp_file_name = None
        for tfile in listdir(file_temp_dir):
            fcount = fcount + 1
            temp_file_name = tfile
        
        if fcount == 0:
            raise schema_errors.FileUploadException("File not found for temporary file with id " + temp_file_id)
        if fcount > 1:
            raise schema_errors.FileUploadException("Multiple files found in temporary file path for temp file id " + temp_file_id)
        
        
        file_from_path = file_temp_dir + temp_file_name
        file_to_dir = self.upload_dir + entity_uuid + os.sep
        
        
        
        #get a uuid for the file
        checksum = hashlib.md5(open(file_from_path, 'rb').read()).hexdigest()
        filesize = os.path.getsize(file_from_path)
        headers = {'Authorization': 'Bearer ' + user_token, 'Content-Type': 'application/json'}
        data = {}
        data['entity_type'] = 'FILE'
        data['parent_ids'] = [entity_uuid]
        file_info= {}
        file_info['path'] = file_to_dir + '<uuid>' + os.sep + temp_file_name
        file_info['checksum'] = checksum
        file_info['base_dir'] = 'INGEST_PORTAL_UPLOAD'
        file_info['size'] = filesize
        data['file_info'] = [file_info]
        response = requests.post(self.uuid_api_url, json = data, headers = headers, verify = False)
        if response is None or response.status_code != 200:
            raise schema_errors.FileUploadException(f"Unable to generate uuid for file {file_temp_dir}{temp_file_name}")
        
        rsjs = response.json()
        file_uuid = rsjs[0]['uuid']
        
        file_to_dir = file_to_dir + file_uuid
        file_dest_path = file_to_dir + os.sep + temp_file_name
        
        if not os.path.exists(file_to_dir):
            os.makedirs(file_to_dir)

        shutil.move(file_from_path, file_dest_path)
        os.rmdir(file_temp_dir)
        
        #self.temp_files[temp_file_id]['filename']
        return {"filename": temp_file_name, "file_uuid": file_uuid}

    #the file will be stored at /<base_dir>/<entity_uuid>/<file_uuid>/<filename>
    #where file_dir = /<base_dir>/<entity_uuid>
    def remove_file(self, file_dir, file_uuid, files_info_list):
        for file_info in files_info_list:
            if file_info['file_uuid'] == file_uuid:
                # Remove from file system
                file_dir = file_helper.ensureTrailingSlash(file_dir) + file_info['file_uuid']
                path_to_file = file_dir + os.sep + file_info['filename']
                os.remove(path_to_file)
                os.rmdir(file_dir)
                
                # Remove from the list
                files_info_list.remove(file_info)
                break
        
        return files_info_list