
class UnimplementedValidatorException(Exception):
    pass

class SchemaValidationException(Exception):
    pass

class InvalidNormalizedEntityTypeException(Exception):
    pass

class InvalidNormalizedTypeException(Exception):
    pass

class BeforeCreateTriggerException(Exception):
    pass

class AfterCreateTriggerException(Exception):
    pass

class BeforeUpdateTriggerException(Exception):
    pass

class AfterUpdateTriggerException(Exception):
    pass

class NoDataProviderGroupException(Exception):
    pass

class MultipleDataProviderGroupException(Exception):
    pass

class UnmatchedDataProviderGroupException(Exception):
    pass
    
class FileUploadException(Exception):
    pass

class MissingApplicationHeaderException(Exception):
    pass

class InvalidApplicationHeaderException(Exception):
    pass
