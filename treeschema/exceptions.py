
class DataAssetDoesNotExist(Exception):
    def __init__(self, message):
        super().__init__(message)

class InvalidFieldInputs(Exception):
    def __init__(self, message):
        super().__init__(message)

class InvalidInputs(Exception):
    def __init__(self, message):
        super().__init__(message)

class InvalidLinksException(Exception):
    def __init__(self, message):
        super().__init__(message)

class TreeSchemaApiError(Exception):
    def __init__(self, message):
        super().__init__(message)

class UsernameSecretRequired(Exception):
    def __init__(self, message):
        super().__init__(message)


# Tree Schema dbt Exceptions
class DbtManifestInvalid(Exception):
    def __init__(self, message):
        super().__init__(message)

class DbtManifestNotParsed(Exception):
    def __init__(self, message):
        super().__init__(message)

class InvalidManifestParseStatus(Exception):
    def __init__(self, message):
        super().__init__(message)

class ManifestParseWaitTimeout(Exception):
    def __init__(self, message):
        super().__init__(message)
