import datetime;

def get_current_timestamp():
    current_time = datetime.datetime.now() 
    return current_time.timestamp() 

def get_entity_type(normalized_entity_type):
    return normalized_entity_type

def get_user_sub(user_info):
    return user_info['sub']

def get_user_email(user_info):
    return user_info['email']

def get_user_name(user_info):
    return user_info['name']


