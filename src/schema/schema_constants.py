class SchemaConstants(object):
    # Used by function cache (memoization)
    # The maximum integer number of entries in the cache queue
    # Expire the cache after the time-to-live (seconds)
    CACHE_MAXSIZE = 1024
    CACHE_TTL = 7200

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
    ASSAY_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/assay_types.yaml'

    # For generating Sample.tissue_type
    TISSUE_TYPES_YAML = 'https://raw.githubusercontent.com/hubmapconsortium/search-api/master/src/search-schema/data/definitions/enums/tissue_sample_types.yaml'