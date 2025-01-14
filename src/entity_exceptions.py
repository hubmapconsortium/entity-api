# Exceptions used internally by the service, typically for anticipated exceptions.
# Knowledge of Flask, HTTP codes, and formatting of the Response should be
# closer to the endpoint @app.route() methods rather than throughout service.
class EntityConfigurationException(Exception):
    """Exception raised when problems loading the service configuration are encountered."""
    def __init__(self, message='There were problems loading the configuration for the service.'):
        self.message = message
        super().__init__(self.message)

class EntityRequestAuthorizationException(Exception):
    """Exception raised for authorization info on a Request."""
    def __init__(self, message='Request authorization problem.'):
        self.message = message
        super().__init__(self.message)

class EntityUnauthorizedException(Exception):
    """Exception raised when authorization for a resource fails."""
    def __init__(self, message='Authorization failed.'):
        self.message = message
        super().__init__(self.message)

class EntityForbiddenException(Exception):
    """Exception raised when authorization for a resource is forbidden."""
    def __init__(self, message='Access forbidden.'):
        self.message = message
        super().__init__(self.message)

class EntityNotFoundException(Exception):
    """Exception raised when entity retrieval returns no results."""
    def __init__(self, message='Not found.'):
        self.message = message
        super().__init__(self.message)

class EntityBadRequestException(Exception):
    """Exception raised when entity retrieval is flagged as a bad request."""
    def __init__(self, message='Bad request.'):
        self.message = message
        super().__init__(self.message)

class EntityServerErrorException(Exception):
    """Exception raised when entity retrieval causes an internal server error."""
    def __init__(self, message='Internal server error.'):
        self.message = message
        super().__init__(self.message)