import os
import secrets
import shutil
import logging
import pathlib
import requests
import hashlib
import os
from werkzeug.utils import secure_filename

# HuBMAP commons
from hubmap_commons import file_helper
from schema import schema_errors

logger = logging.getLogger(__name__)

ID_CHARS=['2','3','4','5','6','7','8','9','b','c','d','e','g','h','j','k','l','m','n','p','q','r','s','t','v','w','x','z']                 

class UploadFileHelper:
    def __init__(self, upload_temp_dir, upload_dir, uuid_api_url):
        self.base_temp_dir = file_helper.ensureTrailingSlash(upload_temp_dir)
        self.upload_temp_dir = self.base_temp_dir + 'hm_tmp_uploads' + str(os.getpid()) + os.sep

        # Use pathlib to create dir instead of file_helper.mkDir
        #file_helper.mkDir(self.upload_temp_dir)
        pathlib.Path(self.upload_temp_dir).mkdir(parents=True, exist_ok=True)

        self.upload_dir = file_helper.ensureTrailingSlash(upload_dir)
        self.temp_files = {}
    
    def clean_temp_dir(self):
        for dirname in os.listdir(self.base_temp_dir):
            dirpath = self.base_temp_dir + dirname
            if os.path.isdir(dirpath):
                shutil.rmtree(dirpath)
    
    def save_temp_file(self, file):
        logger.debug(file)

        temp_id = self.__get_temp_file_id()
        file_dir = self.upload_temp_dir + temp_id + os.sep
        self.temp_files[temp_id] = {}
        self.temp_files[temp_id]['filename'] = file.filename
        self.temp_files[temp_id]['filedir'] = file_dir

        # Use pathlib to create dir instead of file_helper.mkDir
        #file_helper.mkDir(file_dir)
        pathlib.Path(file_dir).mkdir(parents=True, exist_ok=True)

        file.save(os.path.join(file_dir, secure_filename(file.filename)))
        
        return temp_id
    
    def __get_temp_file_id(self, iteration=0):
        if iteration == 100:
            raise Exception("Unable to get a temporary file id after 100 attempts")
        rid = ''
        for _ in range(20):                                                                                                                
            rid = rid + secrets.choice(ID_CHARS)
        while rid in self.temp_files:
            rid = self.get_temp_file_id(iteration = iteration + 1)
        
        return rid
    
    
    def commit_file(self, temp_file_id, entity_uuid, auth_token):
        logger.debug(self.temp_files)
        logger.debug(temp_file_id)

        entity_dir = self.upload_dir + entity_uuid
        if not os.path.exists(entity_dir):
            # Use pathlib to create dir instead of file_helper.mkDir
            #file_helper.mkDir(entity_dir)
            pathlib.Path(entity_dir).mkdir(parents=True)

        elif not os.path.isdir(entity_dir):
            # ?
            raise Exception("Entity file uploads directory exists and is not a directory: " + entity_dir)
        
        if not temp_file_id in self.temp_files:
            raise Exception("Temporary file with id " + temp_file_id + " does not exist.")
        
        file_from_path = self.temp_files[temp_file_id]['filedir'] + self.temp_files[temp_file_id]['filename']
        file_dest_path = self.upload_dir + self.temp_files[temp_file_id]['filename']
        
        #get a uuid for the file
        checksum = hashlib.md5(open(file_from_path,'rb').read()).hexdigest()
        filesize = os.path.getsize(file_from_path)
        headers = {'Authorization': 'Bearer ' + auth_token, 'Content-Type': 'application/json'}
        data = {}
        data['entity_type'] = 'FILE'
        data['parent_ids'] = [entity_uuid]
        file_info= {}
        file_info['path'] = file_dest_path
        file_info['checksum'] = checksum
        file_info['base_dir'] = 'INGEST_PORTAL_UPLOAD'
        file_info['size'] = filesize
        data['file_info'] = [file_info]
        response = requests.post('http://localhost:5001/hmuuid', json=data, headers = headers, verify = False)
        if response is None or response.status_code != 200:
            raise schema_errors.FileUUIDCreateException("Unable to generate uuid for file " + self.temp_files[temp_file_id]['filename'])
        
        file_uuid = response.json()['uuid']
        
        shutil.move(file_from_path, file_dest_path)
        
        return {"filename": self.temp_files[temp_file_id]['filename'], "file_uuid": file_uuid}


    def remove_file(self, file_dir, file_uuid, files_info_list):
        for file_info in files_info_list:
            if file_info['file_uuid'] == file_uuid:
                # Remove from file system
                path_to_file = file_helper.ensureTrailingSlash(file_dir) + file_info['filename']
                os.remove(path_to_file)
                
                # Remove from the list
                files_info_list.remove(file_info)
                break
        
        return files_info_list