
class SchemaValidationException(Exception):
    pass

class InvalidNormalizedEntityClassException(Exception):
    pass

class InvalidNormalizedClassException(Exception):
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