class SchemaConstants(object):

    # File path to the requests_cache generated sqlite (without extension) within docker container, DO NOT MODIFY
    # Expire the cache after the time-to-live (seconds)
    REQUESTS_CACHE_BACKEND = 'sqlite'
    REQUESTS_CACHE_SQLITE_NAME = '/usr/src/app/requests_cache/entity-api'
    REQUESTS_CACHE_TTL = 7200

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