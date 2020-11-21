import logging
import requests
from urllib3.exceptions import InsecureRequestWarning

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper

logger = logging.getLogger(__name__)

"""
Initialize AuthHelper (AuthHelper from HuBMAP commons package)
HuBMAP commons AuthHelper handles "MAuthorization" or "Authorization"

Returns
-------
AuthHelper
    An instnce of AuthHelper
"""
def init_auth_helper():
    if AuthHelper.isInitialized() == False:
        auth_helper = AuthHelper.create(app.config['APP_CLIENT_ID'], app.config['APP_CLIENT_SECRET'])
    else:
        auth_helper = AuthHelper.instance()
    
    return auth_helper

"""
Get user infomation dict based on the http request(headers)

Parameters
----------
request : Flask request object
    The Flask request passed from the API endpoint 

Returns
-------
dict
    A dict containing all the user info

    {
        "scope": "urn:globus:auth:scope:nexus.api.globus.org:groups",
        "name": "First Last",
        "iss": "https://auth.globus.org",
        "client_id": "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114",
        "active": True,
        "nbf": 1603761442,
        "token_type": "Bearer",
        "aud": ["nexus.api.globus.org", "21f293b0-5fa5-4ee1-9e0e-3cf88bd70114"],
        "iat": 1603761442,
        "dependent_tokens_cache_id": "af2d5979090a97536619e8fbad1ebd0afa875c880a0d8058cddf510fc288555c",
        "exp": 1603934242,
        "sub": "c0f8907a-ec78-48a7-9c85-7da995b05446",
        "email": "email@pitt.edu",
        "username": "username@pitt.edu",
        "hmscopes": ["urn:globus:auth:scope:nexus.api.globus.org:groups"],
    }
"""
def get_user_info(request):
    auth_helper = init_auth_helper()
    # `group_required` is a boolean, when True, 'hmgroupids' is in the output
    user_info = auth_helper.getUserInfoUsingRequest(request, False)

    logger.debug("======get_user_info()======")
    logger.debug(user_info)

    # If returns error response, invalid header or token
    if isinstance(user_info, Response):
        msg = "Failed to query the user info with the given globus token"

        logger.error(msg)

        bad_request_error(msg)

    return user_info

"""
Create a dict of HTTP Authorization header with Bearer token for making calls to uuid-api

Returns
-------
dict
    The headers dict to be used by requests
"""
def create_request_headers():
    # Will need this to call getProcessSecret()
    auth_helper = init_auth_helper()

    auth_header_name = 'Authorization'
    auth_scheme = 'Bearer'

    headers_dict = {
        # Don't forget the space between scheme and the token value
        auth_header_name: auth_scheme + ' ' + auth_helper.getProcessSecret()
    }

    return headers_dict

"""
Create a set of new ids for the new entity to be created
Make a POST call to uuid-api with the following json:
{
    "entityType":"Dataset",
    "generateDOI": "true"
}

The list returned by uuid-api that contains all the associated ids, e.g.:
{
    "uuid": "c754a4f878628f3c072d4e8024f707cd",
    "doi": "479NDDG476",
    "displayDoi": "HBM479.NDDG.476"
}

Then map them to the target ids:
uuid -> uuid
doi -> doi_suffix_id
displayDoi -> hubmap_id

Returns
-------
dict
    The dictionary of new ids

    {
        "uuid": "c754a4f878628f3c072d4e8024f707cd",
        "doi_suffix_id": "479NDDG476",
        "hubmap_id": "HBM479.NDDG.476"
    }

"""
def create_hubmap_ids(normalized_entity_class):
    target_url = app.config['UUID_API_URL']

    # Suppress InsecureRequestWarning warning when requesting status on https with ssl cert verify disabled
    requests.packages.urllib3.disable_warnings(category = InsecureRequestWarning)

    # Must use "generateDOI": "true" to generate the doi (doi_suffix_id) and displayDoi (hubmap_id)
    json_to_post = {
        'entityType': normalized_entity_class, 
        'generateDOI': "true"
    }

    # Use modified version of globus app secrect from configuration as the internal token
    # All API endpoints specified in gateway regardless of auth is required or not, 
    # will consider this internal token as valid and has the access to HuBMAP-Read group
    request_headers = create_request_headers()

    # Disable ssl certificate verification
    response = requests.post(url = target_url, headers = request_headers, json = json_to_post, verify = False) 
    
    if response.status_code == 200:
        ids_list = response.json()
        ids_dict = ids_list[0]

        # Create a new dict with desired keys
        new_ids_dict = {
            'uuid': ids_dict['uuid'],
            'hubmap_id': ids_dict['displayDoi']
        }

        return new_ids_dict
    else:
        msg = "Failed to create new ids via the uuid-api service during the creation of this new " + normalized_entity_class
        
        logger.error(msg)

        logger.debug("======create_new_ids() status code======")
        logger.debug(response.status_code)

        logger.debug("======create_new_ids() response text======")
        logger.debug(response.text)
