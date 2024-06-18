import re

import yaml
import logging
import requests
from datetime import datetime

# Local modules
from schema import schema_manager
from schema import schema_errors
from schema import schema_neo4j_queries
from schema.schema_constants import SchemaConstants
from hubmap_commons import hm_auth

logger = logging.getLogger(__name__)


####################################################################################################
## Entity Level Validators
####################################################################################################

"""
Validate the application specified in the custom HTTP header
for creating a new entity via POST or updating via PUT

Parameters
----------
normalized_type : str
    One of the types defined in the schema yaml: Dataset, Upload
request: Flask request
    The instance of Flask request passed in from application request
"""
def validate_application_header_before_entity_create(normalized_entity_type, request):
    # A list of applications allowed to create this new entity
    # Currently only ingest-api and ingest-pipeline are allowed
    # to create or update Dataset and Upload
    # Use lowercase for comparison
    applications_allowed = [SchemaConstants.INGEST_API_APP, SchemaConstants.INGEST_PIPELINE_APP]

    _validate_application_header(applications_allowed, request.headers)


##############################################################################################
## Property Level Validators
####################################################################################################


"""
Validate the specified value for a Dataset's dataset_type is in the valueset UBKG recognizes. 

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_recognized_dataset_type(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # If the proposed Dataset dataset_type ends with something in square brackets, anything inside
    # those square brackets are acceptable at the end of the string.  Simply validate the start.
    proposed_dataset_type_prefix = re.sub(pattern='(\S)\s\[.*\]$', repl=r'\1', string=new_data_dict['dataset_type'])
    target_list = schema_manager.get_dataset_type_valueset_list()

    if proposed_dataset_type_prefix not in target_list:
        raise ValueError(f"Proposed Dataset dataset_type '{proposed_dataset_type_prefix}'"
                         f" is not recognized in the existing ontology."
                         f" Valid values are: {str(target_list)}.")


"""
Validate the target list has no duplicated items

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_no_duplicates_in_list(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # Use lowercase for comparison via list comprehensions
    target_list = [v.lower() for v in new_data_dict[property_key]]
    if len(set(target_list)) != len(target_list):
        raise ValueError(f"The {property_key} field must only contain unique items")


"""
Validate that a given dataset is not a component of a multi-assay split parent dataset fore allowing status to be 
updated. If a component dataset needs to be updated, update it via its parent multi-assay dataset

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""


def validate_dataset_not_component(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    headers = request.headers
    if not headers.get(SchemaConstants.INTERNAL_TRIGGER) == SchemaConstants.COMPONENT_DATASET:
        neo4j_driver_instance = schema_manager.get_neo4j_driver_instance()
        uuid = existing_data_dict['uuid']
        creation_action = schema_neo4j_queries.get_entity_creation_action_activity(neo4j_driver_instance, uuid)
        if creation_action == 'Multi-Assay Split':
            raise ValueError(f"Unable to modify existing {existing_data_dict['entity_type']}"
                             f" {existing_data_dict['uuid']}. Can not change status on component datasets directly. Status"
                             f"change must occur on parent multi-assay split dataset")


"""
If the provided previous revision is already a revision of another dataset, disallow
"""
def validate_if_revision_is_unique(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    previous_revision = new_data_dict['previous_revision_uuid']
    neo4j_driver_instance = schema_manager.get_neo4j_driver_instance()
    next_revision = schema_neo4j_queries.get_next_revision_uuid(neo4j_driver_instance, previous_revision)
    if next_revision:
        raise ValueError(f"Dataset marked as previous revision is already the previous revision of another dataset. "
                         f"Each dataset may only be the previous revision of one other dataset")


"""
If an entity has a DOI, do not allow it to be updated 
"""
def halt_update_if_DOI_exists(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    if 'doi_url' in existing_data_dict or 'registered_doi' in existing_data_dict:
        raise ValueError(f"Unable to modify existing {existing_data_dict['entity_type']}"
                         f" {existing_data_dict['uuid']} due DOI.")

"""
Do not allow a Collection to be created or updated with DOI information if it does not meet all the
criteria of being a public entity.
"""
def halt_DOI_if_collection_missing_elements(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    if 'contacts' not in existing_data_dict:
        raise ValueError(f"Unable to modify existing {existing_data_dict['entity_type']}"
                         f" {existing_data_dict['uuid']} for DOI because it has no contacts.")
    if 'creators' not in existing_data_dict:
        raise ValueError(f"Unable to modify existing {existing_data_dict['entity_type']}"
                         f" {existing_data_dict['uuid']} for DOI because it has no creators.")
    # Count up other validations to check 'datasets', since a transient property

"""
Do not allow a Collection to be created or updated with DOI information if any Dataset in the Collection is not public.
"""
def halt_DOI_if_unpublished_dataset(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # If the request is not trying to create/update DOI, simply return so the request can proceed.
    if 'doi_url' not in new_data_dict or 'registered_doi' not in new_data_dict:
        return

    neo4j_driver_instance = schema_manager.get_neo4j_driver_instance()

    distinct_dataset_levels = []
    if 'dataset_uuids' in new_data_dict:
        # For a Create POST request, or for an Update PUT request with 'dataset_uuids' specified,
        # retrieve all the existing Datasets specified with the request.
        dataset_uuids = existing_data_dict['dataset_uuids']
        collection_datasets = []
        for dataset_uuid in dataset_uuids:
            try:
                ds = schema_neo4j_queries.get_entity(neo4j_driver_instance
                                                     ,dataset_uuid)
                if ds['data_access_level'] not in distinct_dataset_levels:
                    distinct_dataset_levels.append(ds['data_access_level'])
            except Exception as nfe:
                raise ValueError(f"Unable to modify existing {new_data_dict['entity_type']}"
                                 f" {new_data_dict['uuid']} since"
                                 f" Dataset {dataset_uuid} could not be found to verify.")
    else:
        # For an Update PUT request without 'dataset_uuids' specified,
        # simply get the existing, distinct 'data_access_level' setting for all the Datasets in the Collection
        distinct_dataset_statuses = schema_neo4j_queries.get_collection_datasets_statuses(neo4j_driver_instance
                                                                                          ,existing_data_dict['uuid'])
    if len( distinct_dataset_statuses) != 1 or \
            distinct_dataset_statuses[0].lower() != SchemaConstants.DATASET_STATUS_PUBLISHED:
        raise ValueError(f"Unable to modify existing {existing_data_dict['entity_type']}"
                         f" {existing_data_dict['uuid']} for DOI since it contains unpublished Datasets.")

"""
Validate the DOI parameters are presented as a pair during creation or modification.
Even if one is populated already, disallow setting the other, so the data is consciously synced.
Verify the values are compatible with each other.
"""
def verify_DOI_pair(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # Disallow providing one DOI parameter but not the other
    if ('doi_url' in new_data_dict and 'registered_doi' not in new_data_dict) or \
       ('doi_url' not in new_data_dict and 'registered_doi' in new_data_dict):
        raise ValueError(   f"The properties 'doi_url' and 'registered_doi' must both be set in the same operation.")
    # Since both DOI parameters are present, make sure neither is the empty string
    if new_data_dict['doi_url'] == '' or new_data_dict['registered_doi'] == '':
        raise ValueError(   f"The properties 'doi_url' and 'registered_doi' cannot be empty, when specified.")

    # Check if doi_url matches registered_doi with the expected prefix
    try:
        expected_doi_url = SchemaConstants.DOI_BASE_URL + new_data_dict['registered_doi']
    except Exception as e:
        # If SchemaConstants.DOI_BASE_URL is not set, or there is some other
        # problem, give up and fail this validation.
        logger.error(f"During verify_DOI_pair schema validator, unexpected exception e={str(e)}")
        raise ValueError(f"An unexpected error occurred during evaluation of DOI parameters.  See logs.")
    if expected_doi_url and new_data_dict['doi_url'] != expected_doi_url:
        raise ValueError(   f"The 'doi_url' property should match the 'registered_doi' property, after"
                            f" the prefix {SchemaConstants.DOI_BASE_URL}.")

"""
Validate every entity in a list is of entity_type accepted

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def collection_entities_are_existing_datasets(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # `dataset_uuids` is required for creating a Collection
    # Verify each UUID specified exists in the uuid-api, exists in Neo4j, and is for a Dataset before
    # proceeding with creation of Collection.
    bad_dataset_uuids = []
    for dataset_uuid in new_data_dict['dataset_uuids']:
        try:
            ## The following code duplicates some functionality existing in app.py, in
            ## query_target_entity(), which also deals with caching. In the future, the
            ## validation logic shared by this file and app.py should become a utility
            ## module, shared by validators as well as app.py.  But for now, the code
            ## is repeated for the following.

            # Get cached ids if exist otherwise retrieve from UUID-API. Expect an
            # Exception to be raised if not found.
            dataset_uuid_entity = schema_manager.get_hubmap_ids(id=dataset_uuid)

            # If the uuid exists per the uuid-api, make sure it also exists as a Neo4j entity.
            uuid = dataset_uuid_entity['uuid']
            entity_dict = schema_neo4j_queries.get_entity(schema_manager.get_neo4j_driver_instance(), dataset_uuid)

            # If dataset_uuid is not found in Neo4j or is not for a Dataset, fail the validation.
            if not entity_dict:
                logger.info(f"Request for {dataset_uuid} inclusion in Collection,"
                            f" but not found in Neo4j.")
                bad_dataset_uuids.append(dataset_uuid)
            elif entity_dict['entity_type'] != 'Dataset':
                logger.info(f"Request for {dataset_uuid} inclusion in Collection,"
                            f" but entity_type={entity_dict['entity_type']}, not Dataset.")
                bad_dataset_uuids.append(dataset_uuid)
        except Exception as nfe:
            # If the dataset_uuid is not found, fail the validation.
            logger.info(f"Request for {dataset_uuid} inclusion in Collection"
                        f" failed uuid-api retrieval.")
            bad_dataset_uuids.append(dataset_uuid)
    # If any uuids in the request dataset_uuids are not for an existing Dataset entity which
    # exists in uuid-api and Neo4j, raise an Exception so the validation fails and the
    # operation can be rejected.
    if bad_dataset_uuids:
        raise ValueError(f"Unable to find Datasets for {bad_dataset_uuids}.")

"""
Validate the provided value of Dataset.status on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Dataset
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_application_header_before_property_update(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # A list of applications allowed to update this property
    # Currently only ingest-api and ingest-pipeline are allowed
    # to update Dataset.status or Upload.status
    # Use lowercase for comparison
    applications_allowed = [SchemaConstants.INGEST_API_APP, SchemaConstants.INGEST_PIPELINE_APP]

    _validate_application_header(applications_allowed, request.headers)


"""
Validate the provided value of Dataset.status on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Dataset
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_dataset_status_value(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # Use lowercase for comparison
    accepted_status_values = [
        'new', 'processing', 'published', 'qa', 'error', 'hold', 'invalid', 'submitted', 'incomplete'
    ]
    new_status = new_data_dict[property_key].lower()

    if new_status not in accepted_status_values:
        raise ValueError("The provided status value of Dataset is not valid")

    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'validate_dataset_status_value()' validator method.")

    # If status == 'Published' already in Neo4j, then fail for any changes at all
    # Because once published, the dataset should be read-only
    if existing_data_dict['status'].lower() == SchemaConstants.DATASET_STATUS_PUBLISHED:
        raise ValueError(f"The status of this {normalized_entity_type} is already 'Published', status change is not allowed")

    # HTTP header names are case-insensitive
    # request.headers.get('X-Hubmap-Application') returns None if the header doesn't exist
    app_header = request.headers.get(SchemaConstants.HUBMAP_APP_HEADER)

    # Change status to 'Published' can only happen via ingest-api 
    # because file system changes are needed
    if (new_status == SchemaConstants.DATASET_STATUS_PUBLISHED) and (app_header.lower() != SchemaConstants.INGEST_API_APP):
        raise ValueError(f"Dataset status change to 'Published' can only be made via {SchemaConstants.INGEST_API_APP}")


"""
Validate that status, if included in new_data_dict, is different from the existing status value

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_status_changed(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'validate_status_changed()' validator method.")
    # Only allow 'status' in new_data_dict if its different than the existing status value
    if existing_data_dict['status'].lower() == new_data_dict['status'].lower():
        raise ValueError(f"Status value is already {existing_data_dict['status']}, cannot change to {existing_data_dict['status']}. If no change, do not include status field in update")


"""
Validate the sub_status field is also provided when Dataset.retraction_reason is provided on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_if_retraction_permitted(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'validate_if_retraction_permitted()' validator method.")

    # Only published dataset can be retracted
    if existing_data_dict['status'].lower() != SchemaConstants.DATASET_STATUS_PUBLISHED:
        raise ValueError("This dataset is not published, retraction is not allowed")

    # Only token in HuBMAP-Data-Admin group can retract a published dataset
    try:
        # The property 'hmgroupids' is ALWAYS in the output with using schema_manager.get_user_info()
        # when the token in request is a nexus_token
        user_info = schema_manager.get_user_info(request)
        hubmap_admin_group_uuid = schema_manager.get_auth_helper_instance().groupNameToId('HuBMAP-Data-Admin')['uuid']
    except Exception as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)

        # If the token is not a nexus token, no group information available
        # The commons.hm_auth.AuthCache would return a Response with 500 error message
        # We treat such cases as the user not in the HuBMAP-READ group
        raise ValueError("Failed to parse the permission based on token, retraction is not allowed")

    if hubmap_admin_group_uuid not in user_info['hmgroupids']:
        raise ValueError("Permission denied, retraction is not allowed")


"""
Validate the sub_status field is also provided when Dataset.retraction_reason is provided on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_sub_status_provided(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    if 'sub_status' not in new_data_dict:
        raise ValueError("Missing sub_status field when retraction_reason is provided")


"""
Validate the reaction_reason field is also provided when Dataset.sub_status is provided on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_retraction_reason_provided(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    if 'retraction_reason' not in new_data_dict:
        raise ValueError("Missing retraction_reason field when sub_status is provided")


"""
Validate the provided value of Dataset.sub_status on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_retracted_dataset_sub_status_value(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # Use lowercase for comparison
    accepted_sub_status_values = ['retracted']
    sub_status = new_data_dict[property_key].lower()

    if sub_status not in accepted_sub_status_values:
        raise ValueError("Invalid sub_status value of the Dataset to be retracted")


"""
Validate the provided value of Upload.status on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_upload_status_value(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    # Use lowercase for comparison
    accepted_status_values = [
        'new', 'valid', 'invalid', 'error', 'reorganized', 'processing', 'submitted', 'incomplete'
    ]
    new_status = new_data_dict[property_key].lower()

    if new_status not in accepted_status_values:
        raise ValueError(f"Invalid status value: {new_status}")


"""
Validate the provided value of Sample.sample_category on create via POST and update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_sample_category(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    defined_tissue_types = ["organ", "block", "section", "suspension"]
    sample_category = new_data_dict[property_key].lower()

    if sample_category not in defined_tissue_types:
        raise ValueError(f"Invalid sample_category: {sample_category}")


"""
Validate the provided value of Publication.publication_date is in the correct format against ISO 8601 Format: 
'2022-10-31T09:00:00Z' for example, but we only care the date part 'YYYY-MM-DD'
on create via POST and update via PUT

Note: we allow users to use a future date value

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_publication_date(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    try:
        # The user provided date string is valid if we can convert it to a datetime object 
        # base on the ISO 8601 format, 'YYYY-MM-DD', it's fine if the user entered the time part
        date_obj = datetime.fromisoformat(new_data_dict[property_key])
    except ValueError:
        raise ValueError(f"Invalid {property_key} format, must be YYYY-MM-DD")

"""
Validate the provided value of the activity creation action. Only very specific
values are allowed.
Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_creation_action(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    accepted_creation_action_values = ["central process", "lab process", "external process"]
    creation_action = new_data_dict[property_key].lower()
    if creation_action and creation_action not in accepted_creation_action_values:
        raise ValueError("Invalid {} value. Accepted values are: {}".format(property_key, ", ".join(accepted_creation_action_values)))
    if creation_action == '':
        raise ValueError(f"The property {property_key} cannot be empty, when specified.")


"""
Validate the provided value of the activity creation action before updating direct ancestors. Certain values prohibited
Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_not_invalid_creation_action(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    prohibited_creation_action_values = ["Central Process", "Multi-Assay Split"]
    entity_uuid = existing_data_dict.get('uuid')
    creation_action = schema_neo4j_queries.get_entity_creation_action_activity(schema_manager.get_neo4j_driver_instance(), entity_uuid)
    if creation_action and creation_action in prohibited_creation_action_values:
        raise ValueError("Cannot update {} value if creation_action of parent activity is {}".format(property_key, ", ".join(prohibited_creation_action_values)))


"""
Validate that the user is in  Hubmap-Data-Admin before creating or updating field 'assigned_to_group_name'

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_in_admin_group(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    try:
        # The property 'hmgroupids' is ALWAYS in the output with using schema_manager.get_user_info()
        # when the token in request is a nexus_token
        user_info = schema_manager.get_user_info(request)
        hubmap_admin_group_uuid = schema_manager.get_auth_helper_instance().groupNameToId('HuBMAP-Data-Admin')['uuid']
    except Exception as e:
        # Log the full stack trace, prepend a line with our message
        logger.exception(e)

        # If the token is not a groups token, no group information available
        # The commons.hm_auth.AuthCache would return a Response with 500 error message
        # We treat such cases as the user not in the HuBMAP-Data group
        raise ValueError("Failed to parse the permission based on token, retraction is not allowed")

    if hubmap_admin_group_uuid not in user_info['hmgroupids']:
        raise ValueError(f"Permission denied, not permitted to set property {property_key}")


"""
Validate that the provided group_name is one of the group name 'shortname' values where data_provider == true available
from hubmap-commons in the xxx-globus-groups.json file on entity creation

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request: Flask request object
    The instance of Flask request passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_group_name(property_key, normalized_entity_type, request, existing_data_dict, new_data_dict):
    assigned_to_group_name = new_data_dict['assigned_to_group_name']
    globus_groups = schema_manager.get_auth_helper_instance().getHuBMAPGroupInfo()
    group_dict = next((entry for entry in globus_groups.values() if entry.get("shortname") == assigned_to_group_name), None)
    if group_dict is None:
        raise ValueError("Invalid value for 'assigned_to_group_name'")
    is_data_provider = group_dict.get('data_provider')
    if not is_data_provider:
        raise ValueError("Invalid group in 'assigned_to_group_name'. Must be a data provider")


"""
Trigger event method to verify tracking fields new_associated_multi_assay_uuid and
superseded_associated_processed_component_uuids are set coherently on Multi-Assay Datasets and
their component Datasets.

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties as Neo4j data types.
    N.B. elements are not Python data types and must be converted with utilities like schema_manager.convert_str_literal()
new_data_dict : dict
    The request input data as Python data structures converted from JSON, which has passed schema validation, entity
    validation, and possibly other property validations.
"""

def verify_multi_assay_dataset_components(property_key, normalized_type, user_token, existing_data_dict, new_data_dict):

    if  'superseded_associated_processed_component_uuids' in existing_data_dict \
        and 'superseded_associated_processed_component_uuids' in new_data_dict:
        raise ValueError(   f"'superseded_associated_processed_component_uuids' is already set on"
                            f" {existing_data_dict['uuid']}.")
    if  'new_associated_multi_assay_uuid' in existing_data_dict \
        and 'new_associated_multi_assay_uuid' in new_data_dict:
        raise ValueError(   f"'new_associated_multi_assay_uuid' is already set on"
                            f" {existing_data_dict['uuid']}.")
    if  'superseded_associated_processed_component_uuids' in new_data_dict \
        and 'new_associated_multi_assay_uuid' in new_data_dict:
        raise ValueError(   f"'superseded_associated_processed_component_uuids' and 'new_associated_multi_assay_uuid'"
                            f" cannot both be specified on a single Dataset.")
    if  'superseded_associated_processed_component_uuids' in new_data_dict \
        and 'new_associated_multi_assay_uuid' in existing_data_dict:
        raise ValueError( f"'superseded_associated_processed_component_uuids' cannot be set on"
                        f" existing Dataset {existing_data_dict['uuid']} because it is a component Dataset of"
                        f" {existing_data_dict['new_associated_multi_assay_uuid']}.")
    if  'new_associated_multi_assay_uuid' in new_data_dict \
        and 'superseded_associated_processed_component_uuids' in existing_data_dict:
        # Convert the string from Neo4j the Python list
        supersededComponentDatasets = schema_manager.convert_str_literal(existing_data_dict['superseded_associated_processed_component_uuids'])
        raise ValueError( f"'new_associated_multi_assay_uuid' cannot be set on"
                        f" existing Dataset {existing_data_dict['uuid']} because it is a Multi-Assay Dataset"
                        f" with {len(supersededComponentDatasets)}"
                        f" component Datasets it supersedes.")
    # If no contradictions above have caused a ValueError, check if new data contains UUIDs for valid entities.
    if  'new_associated_multi_assay_uuid' in new_data_dict:
        proposedMultiAssayDataset = schema_neo4j_queries.get_entity(schema_manager.get_neo4j_driver_instance()
                                                                    , new_data_dict['new_associated_multi_assay_uuid'])
        if len(proposedMultiAssayDataset) < 1:
            raise ValueError(   f"'new_associated_multi_assay_uuid' value"
                                f" {new_data_dict['new_associated_multi_assay_uuid']}"
                                f" does not exist.")
    if  'superseded_associated_processed_component_uuids' in new_data_dict:
        for uuid in new_data_dict['superseded_associated_processed_component_uuids']:
            proposedComponentDataset = schema_neo4j_queries.get_entity( schema_manager.get_neo4j_driver_instance()
                                                                        , uuid)
            if len(proposedComponentDataset) < 1:
                raise ValueError(f"'superseded_associated_processed_component_uuids' entry with value"
                                 f" {uuid} does not exist.")

    # fall out successfully if no raise() occurred.
    return

####################################################################################################
## Internal Functions
####################################################################################################

"""
Validate the application specified in the custom HTTP header

Parameters
----------
applications_allowed : list
    A list of applications allowed, use lowercase for comparison
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
"""
def _validate_application_header(applications_allowed, request_headers):
    # HTTP header names are case-insensitive
    # request_headers.get('X-Hubmap-Application') returns None if the header doesn't exist
    app_header = request_headers.get(SchemaConstants.HUBMAP_APP_HEADER)

    if not app_header:
        msg = f"Unable to proceed due to missing {SchemaConstants.HUBMAP_APP_HEADER} header from request"
        raise schema_errors.MissingApplicationHeaderException(msg)

    # Use lowercase for comparing the application header value against the yaml
    if app_header.lower() not in applications_allowed:
        msg = f"Unable to proceed due to invalid {SchemaConstants.HUBMAP_APP_HEADER} header value: {app_header}"
        raise schema_errors.InvalidApplicationHeaderException(msg)
