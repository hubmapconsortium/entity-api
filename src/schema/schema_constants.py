class SchemaConstants(object):
    # Expire the request cache after the time-to-live (seconds), default 15 minutes
    REQUEST_CACHE_TTL = 900

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
    ORGAN_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/organ_types.yaml'
    
    # yaml file to parse assay type description
    ASSAY_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/assay_types.yaml'

    # For generating Sample.tissue_type
    TISSUE_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/tissue_sample_types.yaml'