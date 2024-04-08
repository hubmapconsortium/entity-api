from enum import Enum
class SchemaConstants(object):
    MEMCACHED_TTL = 7200

    INGEST_API_APP = 'ingest-api'
    COMPONENT_DATASET = 'component-dataset'
    INGEST_PIPELINE_APP = 'ingest-pipeline'
    HUBMAP_APP_HEADER = 'X-Hubmap-Application'
    INTERNAL_TRIGGER = 'X-Internal-Trigger'
    DATASET_STATUS_PUBLISHED = 'published'

    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'

    ENTITY_API_UPDATE_ENDPOINT = '/entities'
    UUID_API_ID_ENDPOINT = '/uuid'
    INGEST_API_FILE_COMMIT_ENDPOINT = '/file-commit'
    INGEST_API_FILE_REMOVE_ENDPOINT = '/file-remove'
    ONTOLOGY_API_ASSAY_TYPES_ENDPOINT = '/assaytype?application_context=HUBMAP'
    ONTOLOGY_API_ORGAN_TYPES_ENDPOINT = '/organs/by-code?application_context=HUBMAP'

    DOI_BASE_URL = 'https://doi.org/'

# Define an enumeration to classify an entity's visibility, which can be combined with
# authorization info when verify operations on a request.
class DataVisibilityEnum(Enum):
    PUBLIC = SchemaConstants.ACCESS_LEVEL_PUBLIC
    # Since initial release just requires public/non-public, add
    # another entry indicating non-public.
    NONPUBLIC = 'nonpublic'
