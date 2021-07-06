import logging

# Local modules
from schema import schema_errors

logger = logging.getLogger(__name__)


####################################################################################################
## Entity Level Validators
####################################################################################################

"""
Validate the application specified in the custom HTTP header 'X-Hubmap-Application'
for creating a new entity via POST

Parameters
----------
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample, Dataset, Upload
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
"""
def validate_application_header_before_entity_create(normalized_entity_type, request_headers):
    _validate_application_header(normalized_entity_type, request_headers)

def validate_application_header_before_entity_update(normalized_entity_type, request_headers):
    _validate_application_header(normalized_entity_type, request_headers)


####################################################################################################
## Property Level Validators
####################################################################################################


"""
Validate the application specified in the custom HTTP header 'X-Hubmap-Application'
for updating the properties of an exisiting entity via PUT

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample, Dataset, Upload
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    The json data in request body, already after the regular validations
"""
def validate_application_header_before_property_update(property_key, normalized_entity_type, request_headers, existing_data_dict, new_data_dict):
    _validate_application_header(normalized_entity_type, request_headers)


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
    accepted_status_values = ['new', 'processing', 'published', 'qa', 'error', 'hold', 'invalid']

    # Use lowercase for comparison
    new_status = new_data_dict[property_key].lower()

    if new_status not in accepted_status_values:
        raise ValueError("The provided status value is not valid")

    if 'status' not in existing_data_dict:
        raise KeyError("Missing 'status' key in 'existing_data_dict' during calling 'validate_dataset_status_value()' validator method.")

    # If status == 'Published' already in Neo4j, then fail for any changes at all
    # Because once published, the dataset should be read-only
    if existing_data_dict['staus'].lower() == 'published':
        raise ValueError("This dataset is already published, status change is not allowed")

    # HTTP header names are case-insensitive
    # request_headers.get('X-Hubmap-Application') returns None if the header doesn't exist
    app_header = request_headers.get('X-Hubmap-Application')

    # If status is being changed to 'Published', fail
    # This can only happen via ingest-api because file system changes are needed
    if (new_status == 'published') and (app_header.lower() != 'ingest-api'):
        raise ValueError("Status change to 'Published' can only be made via ingest-api")

"""
Validate the provided value of Submission.status on update

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
    accepted_status_values = ['new', 'valid', 'invalid', 'error', 'reorganized', 'processing']
    
    # Use lowercase for comparison
    new_status = new_data_dict[property_key].lower()

    if new_status not in accepted_status_values:
        raise ValueError("The provided status value is not valid")

    
####################################################################################################
## Internal functions
####################################################################################################

"""
Validate the application specified in the custom HTTP header 'X-Hubmap-Application'

Parameters
----------
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample, Dataset, Submission
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
"""
def _validate_application_header(normalized_entity_type, request_headers):
    # Get the list of applications allowed to create or update this entity
    # Returns empty list if no restrictions, meaning both users and aplications can create or update
    applications_allowed = ['ingest-api', 'ingest-pipeline']

    # HTTP header names are case-insensitive
    # request_headers.get('X-Hubmap-Application') returns None if the header doesn't exist
    app_header = request_headers.get('X-Hubmap-Application')

    if not app_header:
        msg = "Unbale to proceed due to missing X-Hubmap-Application header from request"
        raise schema_errors.MissingApplicationHeaderException(msg)

    # Use lowercase for comparing the application header value against the yaml
    if app_header.lower() not in applications_allowed:
        msg = f"Unable to proceed due to invalid X-Hubmap-Application header value: {request_headers.get('X-Hubmap-Application')}"
        raise schema_errors.InvalidApplicationHeaderException(msg)

