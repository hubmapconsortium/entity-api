import logging

# Local modules
from schema import schema_errors

logger = logging.getLogger(__name__)


####################################################################################################
## Entity Level Validators
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
def validate_application_header_before_entity_create(normalized_entity_type, request_headers):
    _validate_application_header(normalized_entity_type, request_headers)

def validate_application_header_before_entity_update(normalized_entity_type, request_headers):
    _validate_application_header(normalized_entity_type, request_headers)


####################################################################################################
## Property Level Validators
####################################################################################################


"""
Validate the application specified in the custom HTTP header 'X-Hubmap-Application'

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample, Dataset, Submission
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
request_json_data : dict
    The json data in request body, already after the regular validations
"""
def validate_application_header_before_property_update(property_key, normalized_entity_type, request_headers, request_json_data):
    _validate_application_header(normalized_entity_type, request_headers)


"""
Validate the provided value of Dataset.status on update

Parameters
----------
property_key : str
    The target property key
normalized_type : str
    Dataset
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
request_json_data : dict
    The json data in request body, already after the regular validations
"""
def validate_dataset_status_value(property_key, normalized_entity_type, request_headers, request_json_data):
    accepted_status_values = ['New', 'Published', 'QA', 'Error', 'Hold', 'Invalid']

    if request_json_data[property_key] not in accepted_status_values:
        raise ValueError("The provided status value is not valid")

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
request_json_data : dict
    The json data in request body, already after the regular validations
"""
def validate_upload_status_value(property_key, normalized_entity_type, request_headers, request_json_data):
    accepted_status_values = ['New', 'Valid', 'Invalid', 'Error', 'Submitted']

    if request_json_data[property_key] not in accepted_status_values:
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
    applications_allowed = ['ingest-api']

    # HTTP header names are case-insensitive
    # request_headers.get('X-Hubmap-Application') returns None if the header doesn't exist
    if not request_headers.get('X-Hubmap-Application'):
        msg = "Unbale to proceed due to missing X-Hubmap-Application header from request"
        raise schema_errors.MissingApplicationHeaderException(msg)

    # Use lowercase for comparing the application header value against the yaml
    if request_headers.get('X-Hubmap-Application').lower() not in applications_allowed:
        msg = f"Unable to proceed due to invalid X-Hubmap-Application header value: {request_headers.get('X-Hubmap-Application')}"
        raise schema_errors.InvalidApplicationHeaderException(msg)
