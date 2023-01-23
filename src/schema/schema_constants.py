class SchemaConstants(object):
    # Use application-specific prefix for Memcached key
    # Expire the cache after the time-to-live (seconds), default 2 hours
    MEMCACHED_PREFIX = 'hm_entity_'
    MEMCACHED_TTL = 7200

    # Constants used by validators
    INGEST_API_APP = 'ingest-api'
    INGEST_PIPELINE_APP = 'ingest-pipeline'
    HUBMAP_APP_HEADER = 'X-Hubmap-Application'
    DATASET_STATUS_PUBLISHED = 'published'

    # Used by triggers, all lowercase for easy comparision
    ACCESS_LEVEL_PUBLIC = 'public'
    ACCESS_LEVEL_CONSORTIUM = 'consortium'
    ACCESS_LEVEL_PROTECTED = 'protected'

    # Yaml file to parse organ description
    ORGAN_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/main/src/search-schema/data/definitions/enums/organ_types.yaml'
    ASSAY_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/main/src/search-schema/data/definitions/enums/assay_types.yaml'

    # For generating Sample.tissue_type
    TISSUE_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/main/src/search-schema/data/definitions/enums/tissue_sample_types.yaml'