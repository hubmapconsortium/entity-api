import os
import json
from hubmap_commons import file_helper

#remove image files from an entity
#image files are stored in a json encoded text field named image_files in the entity_record
#the images to remove are specified as filenames in the image_files_to_remove field
#
#inputs:
#   entity_record- dictonary of the entity's record.  With required fields
#                  image_files_to_remove
#                  image_files
#                  uuid
#
#   uploads_file_path- the path to the base directory where uploaded files are stored
#                      this is the property FILE_UPLOAD_DIR in app.cfg
#
def delete_image_files(entity_record, uploads_file_path):
    return __delete_files(entity_record, uploads_file_path, 'image_files', 'image_files_to_remove')

def __delete_files(entity_record, uploads_file_path, saved_to_attribute_name, remove_files_attribute_name):
    if remove_files_attribute_name in entity_record:
        entity_uuid = entity_record['uuid']
        entity_upload_dir = file_helper.ensureTrailingSlash(uploads_file_path) + entity_uuid + os.sep
        files = json.loads(entity_record[saved_to_attribute_name])
        for filename in entity_record[remove_files_attribute_name]:
            files = __remove_file(entity_upload_dir, filename, files)
        entity_record[saved_to_attribute_name] = json.dumps(files)
    return entity_record
    
def __remove_file(file_dir, filename, file_info_array):
    for file_info in file_info_array:
        if file_info['filename'] == filename:
            file_info_array.remove(file_info)
    
    path_to_file = file_dir + filename
    os.remove(path_to_file)
    return file_info

#method to commit image files that were previously uploaded with UploadFileHelper.save_file
#
#The information, filename and optional description is saved in the image_files field 
#in the provided entity_record.  The image files needed to be previously uploaded
#using the temp file service (UploadFileHelper.save_file).  The temp file id provided
#from UploadFileHelper, paired with an optional description of the file must be provided
#in the field image_files_to_add in the entity_record for each file being committed
# in a json array like: [{"temp_file_id":"eiaja823jafd", "description","Image file 1"}, {"temp_file_id":"pd34hu4spb3lk43usdr"}, {"temp_file_id":"32kafoiw4fbazd", "description","Image file 3"}]
def commit_image_files(entity_record, upload_file_helper):
    return __commit_files(entity_record, upload_file_helper, 'image_files', 'image_files_to_add')
    
def __commit_files(entity_record, upload_file_helper, save_to_attribute_name, added_files_attribute_name):
    if added_files_attribute_name in entity_record:
        commit_file_info_arry = entity_record[added_files_attribute_name]
        if save_to_attribute_name in entity_record:
            return_file_info_array = json.loads(entity_record[save_to_attribute_name])
        else:
            return_file_info_array = []
            
        for file_info in commit_file_info_arry:
            filename = upload_file_helper.commit_file(file_info['temp_file_id'], entity_record['uuid'])
            add_file_info = {'filename':filename}
            if 'description' in file_info:
                add_file_info['description'] = file_info['description']
            return_file_info_array.append(add_file_info)
        entity_record[save_to_attribute_name] = return_file_info_array
    return entity_record
            
        
    