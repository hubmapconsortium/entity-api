import logging

# Local modules
from schema import schema_manager
from schema import schema_errors

logger = logging.getLogger(__name__)


"""
Trigger event method of generating current timestamp

Parameters
----------
property_key : str
    The target property key of the value to be generated
normalized_type : str
    One of the types defined in the schema yaml: Activity, Collection, Donor, Sample, Dataset
user_token: str
    The user's globus nexus token
existing_data_dict : dict
    A dictionary that contains all existing entity properties
new_data_dict : dict
    A merged dictionary that contains all possible input data to be used

Returns
-------
str: The target property key
str: The neo4j TIMESTAMP() function as string
"""
def validate_application(property_key, normalized_entity_type, request_headers):
    # Get the list of applications allowed to create or update this entity
    # Returns empty list if no restrictions, meaning both users and aplications can create or update
    applications_allowed = ['ingest-api']

    # HTTP header names are case-insensitive
    # request_headers.get('X-Hubmap-Application') returns None is the header doesn't exist
    if not request_headers.get('X-Hubmap-Application'):
        msg = "Unbale to proceed due to missing X-Hubmap-Application header from request"
        raise schema_errors.MissingApplicationHeaderException(msg)

    # Use lowercase for comparing the application header value against the yaml
    if request_headers.get('X-Hubmap-Application').lower() not in applications_allowed:
        msg = f"Unable to proceed due to invalid X-Hubmap-Application header value: {request_headers.get('X-Hubmap-Application')}"
        raise schema_errors.InvalidApplicationHeaderException(msg)


