class BloombergError(Exception):
    """Base class for Bloomberg API errors."""
    def __init__(self, message, details=None):
        super().__init__(message)
        self.details = details if details is not None else {}

class BloombergConnectionError(BloombergError):
    """Raised for connection-related issues."""
    pass

class BloombergRequestError(BloombergError):
    """Raised for errors in request construction or invalid request parameters."""
    pass

class BloombergTimeoutError(BloombergError):
    """Raised when a request times out."""
    pass

class BloombergSecurityError(BloombergError):
    """Raised when a security is invalid, not found, or permission is denied."""
    pass

class BloombergFieldError(BloombergError):
    """Raised when a field is invalid or not applicable."""
    pass

class BloombergDataError(BloombergError):
    """Raised when data cannot be retrieved for a valid request (e.g., no data available)."""
    pass

class BloombergPartialDataError(BloombergDataError):
    """Raised when some data is returned, but errors occurred for some securities/fields."""
    def __init__(self, message, partial_data, errors):
        super().__init__(message, details={'errors': errors})
        self.partial_data = partial_data
        self.errors = errors # List of error details

class BloombergLimitError(BloombergError):
    """Raised when a Bloomberg data or request limit is reached."""
    pass