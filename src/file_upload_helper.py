import os
import secrets
import shutil
import logging
import pathlib
from werkzeug.utils import secure_filename

# HuBMAP commons
from hubmap_commons import file_helper

logger = logging.getLogger(__name__)

ID_CHARS=['2','3','4','5','6','7','8','9','b','c','d','e','g','h','j','k','l','m','n','p','q','r','s','t','v','w','x','z']                 

class UploadFileHelper:
    def __init__(self, upload_temp_dir, upload_dir):
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
    
    
    def commit_file(self, temp_file_id, entity_uuid):
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
        
        shutil.move(self.temp_files[temp_file_id]['filedir'] + self.temp_files[temp_file_id]['filename'], self.upload_dir + self.temp_files[temp_file_id]['filename'])
        
        return self.temp_files[temp_file_id]['filename']


    def remove_file(file_dir, filename, files_info_list):
        for file_info in files_info_list:
            if file_info['filename'] == filename:
                # Remove from the list
                files_info_list.remove(file_info)
        
        # Remove from file system
        path_to_file = file_dir + filename
        os.remove(path_to_file)
        
        return files_info_list