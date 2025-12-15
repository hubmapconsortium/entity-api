from enum import Enum
class SchemaConstants(object):
    MEMCACHED_TTL = 7200

    INGEST_API_APP = 'ingest-api'
    ENTITY_API_APP = 'entity-api'
    COMPONENT_DATASET = 'component-dataset'
    INGEST_PIPELINE_APP = 'ingest-pipeline'
    INGEST_UI = 'ingest-ui'
    HUBMAP_APP_HEADER = 'X-Hubmap-Application'
    LOCKED_ENTITY_UPDATE_HEADER = 'X-HuBMAP-Update-Override'
    INTERNAL_TRIGGER = 'X-Internal-Trigger'
    DATASET_STATUS_PUBLISHED = 'published'

    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'

    ENTITY_API_UPDATE_ENDPOINT = '/entities'
    UUID_API_ID_ENDPOINT = '/uuid'
    INGEST_API_FILE_COMMIT_ENDPOINT = '/file-commit'
    INGEST_API_FILE_REMOVE_ENDPOINT = '/file-remove'
    ONTOLOGY_API_ORGAN_TYPES_ENDPOINT = '/organs/by-code?application_context=HUBMAP'

    DOI_BASE_URL = 'https://doi.org/'

    OMITTED_FIELDS = ['ingest_metadata', 'files']

    ALLOWED_PRIORITY_PROJECTS = ['SWAT (Integration Paper)', 'MOSDAP']

# Define an enumeration to classify an entity's visibility, which can be combined with
# authorization info when verify operations on a request.
class DataVisibilityEnum(Enum):
    PUBLIC = SchemaConstants.ACCESS_LEVEL_PUBLIC
    # Since initial release just requires public/non-public, add
    # another entry indicating non-public.
    NONPUBLIC = 'nonpublic'

# Define an enumeration to classify metadata scope which can be returned.
class MetadataScopeEnum(Enum):
    # Legacy notion of complete metadata for an entity includes generated
    # data populated by triggers.
    COMPLETE = 'complete_metadata'
    # Index metadata is for storage in Open Search documents, and should not
    # include data which must be generated and then removed, nor any data which
    # is not stored in an index document.
    INDEX = 'index_metadata'

# Define an enumeration of accepted trigger types.
class TriggerTypeEnum(Enum):
    ON_READ = 'on_read_trigger'
    ON_INDEX = 'on_index_trigger'
    BEFORE_CREATE = 'before_create_trigger'
    BEFORE_UPDATE = 'before_update_trigger'
    AFTER_CREATE = 'after_create_trigger'
    AFTER_UPDATE = 'after_update_trigger'

# Define an enumeration of accepted Neo4j relationship types.
class Neo4jRelationshipEnum(Enum):
    ACTIVITY_INPUT = 'ACTIVITY_INPUT'
    ACTIVITY_OUTPUT = 'ACTIVITY_INPUT'
    IN_COLLECTION = 'IN_COLLECTION'
    N_UPLOAD = 'N_UPLOAD'
    REVISION_OF = 'REVISION_OF'
    USES_DATA = 'USES_DATA'

