import logging
from datetime import datetime
from typing import Annotated
from requests.exceptions import RequestException

from flask import Response

# Local modules
import app_neo4j_queries
from schema import schema_neo4j_queries
from schema.schema_constants import SchemaConstants
from schema.schema_constants import DataVisibilityEnum

# HuBMAP commons
from hubmap_commons.hm_auth import AuthHelper
from hubmap_commons.S3_worker import S3Worker

import dev_entity_exceptions as entityEx

COMMA_SEPARATOR = ','

class EntityWorker:
    authHelper = None
    schemaMgr = None
    memcachedClient = None
    neo4jDriver = None
    MEMCACHED_MODE = False
    MEMCACHED_PREFIX = 'NONE'

    def __init__(self, app_config, memcached_client_instance, schema_mgr, neo4j_driver_instance):
        self.logger = logging.getLogger('entity.service')

        if app_config is None:
            raise entityEx.EntityConfigurationException('Configuration data loaded by the app must be passed to the worker.')
        try:
            ####################################################################################################
            ## Load configuration variables used by this class
            ####################################################################################################
            clientId = app_config['APP_CLIENT_ID']
            clientSecret = app_config['APP_CLIENT_SECRET']

            # Whether Memcached is being used or not
            # Default to false if the property is missing in the configuration file

            self.MEMCACHED_MODE = app_config['MEMCACHED_MODE'] if 'MEMCACHED_MODE' in app_config else False
            # Use prefix to distinguish the cached data of same source across different deployments
            self.MEMCACHED_PREFIX = app_config['MEMCACHED_PREFIX'] if 'MEMCACHED_PREFIX' in app_config else 'NONE'

            self.logger.debug(f"KBKBKB During init from config, MEMCACHED_MODE={self.MEMCACHED_MODE}")
            self.logger.debug(f"KBKBKB During init from config, MEMCACHED_PREFIX={self.MEMCACHED_PREFIX}")

            ####################################################################################################
            ## S3Worker initialization
            ####################################################################################################
            if 'LARGE_RESPONSE_THRESHOLD' not in app_config \
                or not isinstance(app_config['LARGE_RESPONSE_THRESHOLD'], int) \
                or app_config['LARGE_RESPONSE_THRESHOLD'] > 10*(2**20)-1:
                self.logger.error(f"There is a problem with the LARGE_RESPONSE_THRESHOLD setting in app.cfg."
                                  f" Defaulting to small value so noticed quickly.")
                large_response_threshold = 5000000
            else:
                large_response_threshold = int(app_config['LARGE_RESPONSE_THRESHOLD'])

            self.logger.info(f"large_response_threshold set to {large_response_threshold}.")
            self.S3_settings_dict = {   'large_response_threshold': large_response_threshold
                                        ,'aws_access_key_id': app_config['AWS_ACCESS_KEY_ID']
                                        ,'aws_secret_access_key': app_config['AWS_SECRET_ACCESS_KEY']
                                        ,'aws_s3_bucket_name': app_config['AWS_S3_BUCKET_NAME']
                                        ,'aws_object_url_expiration_in_secs': app_config['AWS_OBJECT_URL_EXPIRATION_IN_SECS']
                                        ,'service_configured_obj_prefix': app_config['AWS_S3_OBJECT_PREFIX']}
            try:
                self.theS3Worker = S3Worker(ACCESS_KEY_ID=self.S3_settings_dict['aws_access_key_id']
                                            , SECRET_ACCESS_KEY=self.S3_settings_dict['aws_secret_access_key']
                                            , S3_BUCKET_NAME=self.S3_settings_dict['aws_s3_bucket_name']
                                            , S3_OBJECT_URL_EXPIRATION_IN_SECS=self.S3_settings_dict['aws_object_url_expiration_in_secs']
                                            , LARGE_RESPONSE_THRESHOLD=self.S3_settings_dict['large_response_threshold']
                                            , SERVICE_S3_OBJ_PREFIX=self.S3_settings_dict['service_configured_obj_prefix'])
                self.logger.info("self.theS3Worker initialized")
            except Exception as e:
                self.logger.error(f"Error initializing self.theS3Worker - '{str(e)}'.", exc_info=True)
                raise entityEx.EntityConfigurationException(f"Unexpected error: {str(e)}")

        except KeyError as ke:
            self.logger.error(f"Expected configuration failed to load %s from app_config={app_config}"
                              f" due to ke={str(ke)}")
            raise entityEx.EntityConfigurationException('Expected configuration failed to load. See the logs.')

        if schema_mgr is None:
            raise entityEx.EntityConfigurationException('A schema manager must be passed to the worker until it instantiates its own.')
        else:
            self.schemaMgr = schema_mgr

        if neo4j_driver_instance is None:
            raise entityEx.EntityConfigurationException('A Neo4j driver must be passed to the worker until it instantiates its own.')
        else:
            self.neo4jDriver = neo4j_driver_instance

        if memcached_client_instance is None:
            self.logger.info('No cache client passed to the worker, running without memcache.')
        self.memcachedClient = memcached_client_instance

        ####################################################################################################
        ## AuthHelper initialization
        ####################################################################################################
        if not clientId  or not clientSecret:
            raise entityEx.EntityConfigurationException('Globus client id and secret are required in AuthHelper')
        # Initialize AuthHelper class and ensure singleton
        try:
            if not AuthHelper.isInitialized():
                self.authHelper = AuthHelper.create(    clientId,
                                                        clientSecret)
                self.logger.info('Initialized AuthHelper class successfully')
            else:
                self.authHelper = AuthHelper.instance()
        except Exception as e:
            msg = 'Failed to initialize the AuthHelper class'
            # Log the full stack trace, prepend a line with our message
            self.logger.exception(msg)
            raise entityEx.EntityConfigurationException(msg)

    def _user_in_hubmap_read_group(self, user_info):
        if user_info is None:
            return False
        try:
            # The property 'hmgroupids' is ALWAYS in the output with using schema_manager.get_user_info()
            # when the token in request is a groups token
            hubmap_read_group_uuid = self.authHelper.groupNameToId('HuBMAP-READ')['uuid']
        except Exception as e:
            # Log the full stack trace, prepend a line with our message
            self.logger.exception(e)

            # If the token is not a groups token, no group information available
            # The commons.hm_auth.AuthCache would return a Response with 500 error message
            # We treat such cases as the user not in the HuBMAP-READ group
            # KBKBKB @TODO clarify these comments...there is no token in this scope...
            # KBKBKB @TODO https://github.com/hubmapconsortium/commons/blob/6d2f7b323191b272d97b79bc41b1a04295444006/hubmap_commons/hm_auth.py#L482-L488
            # KBKBKB @TODO seems to indicate we will get None rather than any kind of Response...
            return False

        return ('hmgroupids' in user_info and hubmap_read_group_uuid in user_info['hmgroupids'])

    '''
    Return the "visibility" of an entity as DataVisibilityEnum value.  Determination of
    "public" or "non-public" is specific to entity type.
    
    Parameters
    ----------
    entity_dict : dict
        A Python dictionary retrieved for the entity 
    
    Returns
    -------
    DataVisibilityEnum
        A value identifying if the entity is public or non-public
    '''
    def _get_entity_visibility(self, entity_dict):
        normalized_entity_type = entity_dict['entity_type']
        if normalized_entity_type not in self.schemaMgr.get_all_entity_types():
            self.logger.log(logging.ERROR
                            ,f"normalized_entity_type={normalized_entity_type}"
                              f" not recognized by schema_manager.get_all_entity_types().")
            raise entityEx.EntityBadRequestException(f"'{normalized_entity_type}' is not a recognized entity type.")

        # Use the characteristics of the entity's data to classify the entity's visibility, so
        # it can be used along with the user's authorization to determine access.
        entity_visibility = DataVisibilityEnum.NONPUBLIC
        if self.schemaMgr.entity_type_instanceof(normalized_entity_type, 'Dataset') and \
                entity_dict['status'].lower() == SchemaConstants.DATASET_STATUS_PUBLISHED:
            entity_visibility = DataVisibilityEnum.PUBLIC
        elif self.schemaMgr.entity_type_instanceof(normalized_entity_type, 'Collection') and \
                'registered_doi' in entity_dict and \
                'doi_url' in entity_dict and \
                'contacts' in entity_dict and \
                'contributors' in entity_dict and \
                len(entity_dict['contacts']) > 0 and \
                len(entity_dict['contributors']) > 0:
            # Get the data_access_level for each Dataset in the Collection from Neo4j
            collection_dataset_statuses = schema_neo4j_queries.get_collection_datasets_statuses(self.neo4jDriver
                                                                                                , entity_dict['uuid'])

            # If the list of distinct statuses for Datasets in the Collection only has one entry, and
            # it is 'published', the Collection is public
            if len(collection_dataset_statuses) == 1 and \
                    collection_dataset_statuses[0].lower() == SchemaConstants.DATASET_STATUS_PUBLISHED:
                entity_visibility = DataVisibilityEnum.PUBLIC
        elif normalized_entity_type == 'Upload':
            # Upload entities require authorization to access, so keep the
            # entity_visibility as non-public, as initialized outside block.
            pass
        elif normalized_entity_type in ['Donor', 'Sample'] and \
                entity_dict['data_access_level'] == SchemaConstants.ACCESS_LEVEL_PUBLIC:
            entity_visibility = DataVisibilityEnum.PUBLIC
        return entity_visibility

    '''
    Get target entity dict from the cache or from Neo4j query for the given id. When caching enabled,
    data retrieved from Neo4j will be cached.

    Parameters
    ----------
    entity_id : str
        The uuid or hubmap_id of target entity

    Returns
    -------
    dict
        A dictionary of entity details either from the cache or a neo4j query.
    '''
    def _query_target_entity(self, entity_id):
        # Get the entity dict from cache if exists
        # Otherwise query against uuid-api and neo4j to get the entity dict if the entity_id exists

        cache_result = None

        try:
            # Get cached ids if exist otherwise retrieve from UUID-API
            hubmap_ids = self.schemaMgr.get_hubmap_ids(entity_id.strip())

            # Get the target uuid if all good
            uuid = hubmap_ids['hm_uuid']

            # Look up the cache again by the uuid since we only use uuid in the cache key
            if self.MEMCACHED_MODE and self.MEMCACHED_PREFIX and self.memcachedClient:
                cache_key = f'{self.MEMCACHED_PREFIX}_neo4j_{uuid}'
                cache_result = self.memcachedClient.get(cache_key)

            if cache_result is None:
                self.logger.info(f'Neo4j entity cache of {uuid} not found or expired at time {datetime.now()}')

                # Make a new query against neo4j
                entity_dict = schema_neo4j_queries.get_entity(neo4j_driver=self.neo4jDriver
                                                              ,uuid=uuid)

                # The uuid exists via uuid-api doesn't mean it also exists in Neo4j
                if not entity_dict:
                    msg = f"Entity of entity_id: {entity_id} not found in Neo4j"
                    self.logger.debug(msg)
                    raise entityEx.EntityNotFoundException(msg)

                # Save to cache
                if self.MEMCACHED_MODE and self.MEMCACHED_PREFIX and self.memcachedClient:
                    self.logger.info(f'Creating neo4j entity result cache of {uuid} at time {datetime.now()}')
                    cache_key = f'{self.MEMCACHED_PREFIX}_neo4j_{uuid}'
                    self.memcachedClient.set(cache_key, entity_dict, expire=SchemaConstants.MEMCACHED_TTL)
            else:
                self.logger.info(f"Using neo4j entity cache of UUID {uuid} at time {datetime.now()}")
                self.logger.debug(cache_result)
                entity_dict = cache_result
        except RequestException as e:
            # Due to the use of response.raise_for_status() in schema_manager.get_hubmap_ids()
            # we can access the status codes from the exception
            status_code = e.response.status_code

            if status_code == 400:
                raise entityEx.EntityBadRequestException(e.response.text)
            if status_code == 404:
                raise entityEx.EntityNotFoundException(e.response.text)
            else:
                raise entityEx.EntityUnauthorizedException(e.response.text)

        # One final return
        return entity_dict

    ''''
    Retrieve the metadata information for an entity by id.
    
    Get target entity dict based upon the user's authorization. The full dictionary may be
    filtered down if credentials were not presented for full access.
    
    Parameters
    ----------
    entity_id : str
        The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity .
    valid_user_token : str
        Either the valid current token for an authenticated user or None.
    user_info : dict
        Information for the logged-in user to be used for authorization accessing non-public entities.
        
    Returns
    -------
    dict
        A dictionary containing all the properties the target entity.
    '''
    def _get_entity_by_id_for_auth_level(self, entity_id:Annotated[str, 32], valid_user_token:Annotated[str, 32]
                                         , user_info:dict) -> dict:

        # Use the internal token to query the target entity to assure it is returned. This way public
        # entities can be accessed even if valid_user_token is None.
        internal_token = self.authHelper.getProcessSecret()
        entity_dict = self._query_target_entity(entity_id=entity_id)
        normalized_entity_type = entity_dict['entity_type']

        # Get the generated complete entity result from cache if exists
        # Otherwise re-generate on the fly
        complete_dict = self.schemaMgr.get_complete_entity_result(token=internal_token
                                                                  , entity_dict=entity_dict)

        # Determine if the entity is publicly visible base on its data, only.
        # To verify if a Collection is public, it is necessary to have its Datasets, which
        # are populated as triggered data.  So pull back the complete entity for
        # _get_entity_visibility() to check.
        entity_scope = self._get_entity_visibility(entity_dict=complete_dict)
        public_entity = (entity_scope is DataVisibilityEnum.PUBLIC)

        # Set a variable reflecting the user's authorization by being in the HuBMAP-READ Globus Group
        user_authorized = self._user_in_hubmap_read_group(user_info=user_info)

        # For non-public documents, reject the request if the user is not authorized
        if not public_entity:
            if valid_user_token is None:
                raise entityEx.EntityForbiddenException(f"{normalized_entity_type} for {entity_id} is not"
                                                        f" accessible without presenting a token.")
            if not user_authorized:
                raise entityEx.EntityForbiddenException(f"The requested {normalized_entity_type} has non-public data."
                                                        f"  A Globus token with access permission is required.")

        # We'll need to return all the properties including those generated by
        # `on_read_trigger` to have a complete result e.g., the 'next_revision_uuid' and
        # 'previous_revision_uuid' being used below.
        # Collections, however, will filter out only public properties for return.

        # Also normalize the result based on schema
        final_result = self.schemaMgr.normalize_entity_result_for_response(complete_dict)

        # Identify fields to exclude from non-authorized responses for the entity type.
        fields_to_exclude = self.schemaMgr.get_fields_to_exclude(normalized_entity_type)

        # Response with the dict
        #if public_entity and not user_in_hubmap_read_group(request):
        if public_entity and not user_authorized:
            final_result = self.schemaMgr.exclude_properties_from_response(fields_to_exclude, final_result)
        if normalized_entity_type == 'Collection':
            for i, dataset in enumerate(final_result.get('datasets', [])):
                if self._get_entity_visibility( entity_dict=dataset) != DataVisibilityEnum.PUBLIC \
                        or user_authorized: # or user_in_hubmap_read_group(request):
                    # If the dataset is public, or if the user has read-group access, there is
                    # no need to remove fields, continue to the next dataset
                    continue
                dataset_excluded_fields = self.schemaMgr.get_fields_to_exclude('Dataset')
                final_result.get('datasets')[i] = self.schemaMgr.exclude_properties_from_response(dataset_excluded_fields, dataset)
        return final_result

    '''
    Retrieve the metadata information for certain data associated with entity.  This method supports
    Dataset entities, and can get the associated data for organs, samples, or donors.
    
    Get associated data dict based upon the user's authorization. The associated data may be
    filtered down if credentials were not presented for full access.
    
    Parameters
    ----------
    dataset_dict : dict
        A dictionary containing all the properties the target entity.
    dataset_visibility : DataVisibilityEnum
        An indication of the entity itself is public or not, so the associated data can
        be filtered to match the entity dictionary before being returned.
    valid_user_token : str
        Either the valid current token for an authenticated user or None.
    user_info : dict
        Information for the logged-in user to be used for authorization accessing non-public entities.
    associated_data : str
        A string indicating the associated property to be retrieved, which must be from
        the values supported by this method.
        
    Returns
    -------
    list
        A dictionary containing all the properties the target entity.
    '''
    def _get_dataset_associated_data(   self, dataset_dict:dict, dataset_visibility:DataVisibilityEnum
                                        , valid_user_token:Annotated[str, 32], user_info:dict
                                        , associated_data:str) -> list:

        # Confirm the associated data requested is supported by this method.
        retrievable_associations = ['organs', 'samples', 'donors']
        if associated_data.lower() not in retrievable_associations:
            raise entityEx.EntityBadRequestException(   f"Dataset associated data cannot be retrieved for"
                                                        f" {associated_data}, only"
                                                        f" {COMMA_SEPARATOR.join(retrievable_associations)}.")

        # Confirm the dictionary passed in is for a Dataset entity.
        if not self.schemaMgr.entity_type_instanceof(dataset_dict['entity_type'], 'Dataset'):
            raise entityEx.EntityBadRequestException(   f"'{dataset_dict['entity_type']}' for"
                                                        f" uuid={dataset_dict['uuid']} is not a Dataset or Publication,"
                                                        f" so '{associated_data}' can not be retrieved for it.")
        # Set up fields to be excluded when retrieving the entities associated with
        # the Dataset.  Organs are one kind of Sample.
        if associated_data.lower() in ['organs', 'samples']:
            fields_to_exclude = self.schemaMgr.get_fields_to_exclude('Sample')
        elif associated_data.lower() in ['donors']:
            fields_to_exclude = self.schemaMgr.get_fields_to_exclude('Donor')
        else:
            self.logger.error(  f"Expected associated data type to be verified, but got"
                                f" associated_data.lower()={associated_data.lower()}.")
            raise entityEx.EntityServerErrorException(f"Unexpected error retrieving '{associated_data}' for a Dataset")

        public_entity = (dataset_visibility is DataVisibilityEnum.PUBLIC)

        # Set a variable reflecting the user's authorization by being in the HuBMAP-READ Globus Group
        user_authorized = self._user_in_hubmap_read_group(user_info=user_info)

        # For non-public documents, reject the request if the user is not authorized
        if not public_entity:
            if valid_user_token is None:
                raise entityEx.EntityForbiddenException(f"{dataset_dict['entity_type']} for"
                                                        f" {dataset_dict['uuid']} is not"
                                                        f" accessible without presenting a token.")
            if not user_authorized:
                raise entityEx.EntityForbiddenException(f"The requested Dataset has non-public data."
                                                        f"  A Globus token with access permission is required.")

        # By now, either the entity is public accessible or the user has the correct access level
        if associated_data.lower() == 'organs':
            associated_entities = app_neo4j_queries.get_associated_organs_from_dataset( self.neo4jDriver,
                                                                                        dataset_dict['uuid'])
        elif associated_data.lower() == 'samples':
            associated_entities = app_neo4j_queries.get_associated_samples_from_dataset(self.neo4jDriver,
                                                                                        dataset_dict['uuid'])
        elif associated_data.lower() == 'donors':
            associated_entities = app_neo4j_queries.get_associated_donors_from_dataset( self.neo4jDriver,
                                                                                        dataset_dict['uuid'])
        else:
            self.logger.error(  f"Expected associated data type to be verified, but got"
                                f" associated_data.lower()={associated_data.lower()} while retrieving from Neo4j.")
            raise entityEx.EntityServerErrorException(f"Unexpected error retrieving '{associated_data}' from the data store")

        # If there are zero items in the list of associated_entities, return an empty list rather than retrieving.
        if len(associated_entities) < 1:
            return []

        # Use the internal token to query the target entity to assure it is returned. This way public
        # entities can be accessed even if valid_user_token is None.
        internal_token = self.authHelper.getProcessSecret()
        complete_entities_list = self.schemaMgr.get_complete_entities_list(token=internal_token
                                                                           , entities_list=associated_entities)
        # Final result after normalization
        final_result = self.schemaMgr.normalize_entities_list_for_response(entities_list=complete_entities_list)

        # For public entities, limit the fields in the response unless the authorization presented in the
        # Request allows the user to see all properties.
        if public_entity and not user_authorized:
            filtered_entities_list = []
            for entity in final_result:
                final_entity_dict = self.schemaMgr.exclude_properties_from_response( excluded_fields=fields_to_exclude
                                                                                    , output_dict=entity)
                filtered_entities_list.append(final_entity_dict)
            final_result = filtered_entities_list

        return final_result

    '''
    Retrieve authentication bearer token presented on the request.  If no token is in the request, return None as
    a valid user token value indicating public access.

    Parameters
    ----------
    request : flask.request
        The Flask http request object

    Returns
    -------
    str
        The bearer token presented on the request for authentication.
    '''
    def get_request_auth_token(self, request) -> str:
        if 'Authorization' not in request.headers:
            return None

        # Get user token from Authorization header
        # getAuthorizationTokens() also handles MAuthorization header but we are not using that here
        try:
            # HTTP header names are case-insensitive
            # request.headers.get('Authorization') returns None if the header doesn't exist
            request_token = self.authHelper.getAuthorizationTokens(request.headers)
        except Exception as e:
            msg = "Failed to parse the Authorization token by calling commons.auth_helper.getAuthorizationTokens()"
            # Log the full stack trace, prepend a line with our message
            self.logger.exception(msg)
            raise entityEx.EntityRequestAuthorizationException(msg)

        # When the token is a flask.Response instance, it MUST be a 401 error with message.
        # That's how commons.auth_helper.getAuthorizationTokens() was designed.
        if isinstance(request_token, Response):
            # The Response.data returns binary string, need to decode
            raise entityEx.EntityUnauthorizedException(request_token.get_data().decode())

        # Make sure the token is not invalid or expired by calling a method with
        # the side-effect of returning a Response
        user_info = self.authHelper.getUserInfo(request_token, False)
        if isinstance(user_info, Response):
            # The Response.data returns binary string, need to decode
            raise entityEx.EntityUnauthorizedException(user_info.get_data().decode())

        return request_token

    '''
    Retrieve authorization information for the bearer token presented on the request, including
    Globus Group information.

    Parameters
    ----------
    request : flask.request
        The Flask http request object

    Returns
    -------
    dict
        The user information, including Globus Group data, which can be used for authorization.
    '''
    def get_request_user_info_with_groups(self, request):
        try:
            # The property 'hmgroupids' is ALWAYS in the output with using schema_manager.get_user_info()
            # when the token in request is a groups token
            user_info = self.authHelper.getUserInfoUsingRequest(httpReq=request
                                                                , getGroups=True)
            self.logger.info("======user_info======")
            self.logger.info(user_info)
            if isinstance(user_info, Response):
                # Bubble up the actual error message from commons
                # The Response.data returns binary string, need to decode
                msg = user_info.get_data().decode()
                # Log the full stack trace, prepend a line with our message
                self.logger.exception(msg)
                return None
        except Exception as e:
            # Log the full stack trace, prepend a line with our message
            self.logger.exception(e)
            return None
        return user_info

    ''''
    Retrieve expanded metadata information for an entity by id, including metadata for
    associated data.

    Get target entity dict based upon the user's authorization. The full dictionary may be
    filtered down if credentials were not presented for full access.

    Parameters
    ----------
    entity_id : str
        The HuBMAP ID (e.g. HBM123.ABCD.456) or UUID of target entity .
    valid_user_token : str
        Either the valid current token for an authenticated user or None.
    user_info : dict
        Information for the logged-in user to be used for authorization accessing non-public entities.

    Returns
    -------
    dict
        A dictionary containing all the properties the target entity.
    '''
    def get_expanded_entity_metadata(self, entity_id:Annotated[str, 32], valid_user_token:Annotated[str, 32]
                                     , user_info:dict) -> dict:
        # Retrieve the metadata dictionary for the entity, which will be expanded later to hold entries for the
        # associated data.
        expanded_entity_dict = self._get_entity_by_id_for_auth_level(entity_id=entity_id
                                                                     , valid_user_token=valid_user_token
                                                                     , user_info=user_info)
        # Determine if the entity is publicly visible base on its data, only.
        # To verify if a Collection is public, it is necessary to have its Datasets, which
        # are populated as triggered data.  So pull back the complete entity for
        # _get_entity_visibility() to check.
        entity_scope = self._get_entity_visibility(entity_dict=expanded_entity_dict)

        # Retrieve the associated data for the entity, and add it to the expanded dictionary.
        associated_organ_list = self._get_dataset_associated_data(  dataset_dict=expanded_entity_dict
                                                                    , dataset_visibility=entity_scope
                                                                    , valid_user_token=valid_user_token
                                                                    , user_info=user_info
                                                                    , associated_data='Organs')
        expanded_entity_dict['organs'] = associated_organ_list

        associated_sample_list = self._get_dataset_associated_data( dataset_dict=expanded_entity_dict
                                                                    , dataset_visibility=entity_scope
                                                                    , valid_user_token=valid_user_token
                                                                    , user_info=user_info
                                                                    , associated_data='Samples')
        expanded_entity_dict['samples'] = associated_sample_list

        associated_donor_list = self._get_dataset_associated_data(  dataset_dict=expanded_entity_dict
                                                                    , dataset_visibility=entity_scope
                                                                    , valid_user_token=valid_user_token
                                                                    , user_info=user_info
                                                                    , associated_data='Donors')

        expanded_entity_dict['donors'] = associated_donor_list

        # Return the dictionary containing the entity metadata as well as metadata for the associated data.
        return expanded_entity_dict

    # KBKBKB @TODO for future use of /datasets/<id>/organs endpoint of app.py needs.
    def get_organs_associated_with_dataset(self, dataset_id: Annotated[str, 32], valid_user_token: Annotated[str, 32]
        , user_info: dict) -> list:

        dataset_dict = self._get_entity_by_id_for_auth_level(entity_id=dataset_id
                                                             , valid_user_token=valid_user_token
                                                             , user_info=user_info)
        # Determine if the entity is publicly visible base on its data, only.
        # To verify if a Collection is public, it is necessary to have its Datasets, which
        # are populated as triggered data.  So pull back the complete entity for
        # _get_entity_visibility() to check.
        entity_scope = self._get_entity_visibility(entity_dict=dataset_dict)

        associated_organ_list = self._get_dataset_associated_data(  dataset_dict=dataset_dict
                                                                    , dataset_visibility=entity_scope
                                                                    , valid_user_token=valid_user_token
                                                                    , user_info=user_info
                                                                    , associated_data='Organs')

        # If there are zero items in the list associated_organs, then there are no associated
        # Organs and a 404 will be returned.
        if len(associated_organ_list) < 1:
            raise entityEx.EntityNotFoundException(f'Dataset {dataset_id} does not have any associated organs')

        return associated_organ_list
