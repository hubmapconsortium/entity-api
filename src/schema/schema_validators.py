import logging

# Local modules
from schema import schema_errors

logger = logging.getLogger(__name__)

# Shared constants
INGEST_API_APP = 'ingest-api'
INGEST_PIPELINE_APP = 'ingest-pipeline'
HUBMAP_APP_HEADER = 'X-Hubmap-Application'
DATASET_STATUS_PUBLISHED = 'published'

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
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
"""
def validate_application_header(normalized_entity_type, request_headers):
    # A list of applications allowed to create this new entity
    # Currently only ingest-api and ingest-pipeline are allowed
    # to create or update Dataset and Upload
    # Use lowercase for comparison
    applications_allowed = [INGEST_API_APP, INGEST_PIPELINE_APP]

    # HTTP header names are case-insensitive
    # request_headers.get('X-Hubmap-Application') returns None if the header doesn't exist
    app_header = request_headers.get(HUBMAP_APP_HEADER)

    if not app_header:
        msg = f"Unbale to proceed due to missing {HUBMAP_APP_HEADER} header from request"
        raise schema_errors.MissingApplicationHeaderException(msg)

    # Use lowercase for comparing the application header value against the yaml
    if app_header.lower() not in applications_allowed:
        msg = f"Unable to proceed due to invalid {HUBMAP_APP_HEADER} header value: {app_header}"
        raise schema_errors.InvalidApplicationHeaderException(msg)


##############################################################################################
## Property Level Validators specific to Dataset
####################################################################################################

"""
Validate the provided value of Dataset.status on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Dataset
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_dataset_status_value(property_key, normalized_entity_type, request_headers, existing_data_dict, new_data_dict):
    # Use lowercase for comparison
    accepted_status_values = ['new', 'processing', 'published', 'qa', 'error', 'hold', 'invalid']
    new_status = new_data_dict[property_key].lower()

    if new_status not in accepted_status_values:
        raise ValueError("The provided status value of Dataset is not valid")

    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'validate_dataset_status_value()' validator method.")

    # If status == 'Published' already in Neo4j, then fail for any changes at all
    # Because once published, the dataset should be read-only
    if existing_data_dict['status'].lower() == DATASET_STATUS_PUBLISHED:
        raise ValueError("This dataset is already published, status change is not allowed")

    # HTTP header names are case-insensitive
    # request_headers.get('X-Hubmap-Application') returns None if the header doesn't exist
    app_header = request_headers.get(HUBMAP_APP_HEADER)

    # Change status to 'Published' can only happen via ingest-api 
    # because file system changes are needed
    if (new_status == DATASET_STATUS_PUBLISHED) and (app_header.lower() != INGEST_API_APP):
        raise ValueError(f"Dataset status change to 'Published' can only be made via {INGEST_API_APP}")


####################################################################################################
## Property Level Validators specific to Upload
####################################################################################################

"""
Validate the provided value of Upload.status on update via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Submission
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_upload_status_value(property_key, normalized_entity_type, request_headers, existing_data_dict, new_data_dict):
    # Use lowercase for comparison
    accepted_status_values = ['new', 'valid', 'invalid', 'error', 'reorganized', 'processing']
    new_status = new_data_dict[property_key].lower()

    if new_status not in accepted_status_values:
        raise ValueError("The provided status value of Upload is not valid")
