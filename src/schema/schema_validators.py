import logging

# Local modules
from schema import schema_errors

logger = logging.getLogger(__name__)


"""
Validate the application specified in the custom HTTP header 'X-Hubmap-Application'

Parameters
----------
normalized_type : str
    One of the types defined in the schema yaml: Donor, Sample, Dataset, Submission
request_headers: Flask request.headers object, behaves like a dict
    The instance of Flask request.headers passed in from application request
"""
def validate_application(normalized_entity_type, request_headers):
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


