class MiiaError(Exception):
    """
    Base class for MIIA exceptions following the `error_code` + `message`
    pattern.

    Algorithm code can raise this class directly. API route code should raise
    `MiiaHttpError` instead.
    """

    def __init__(self, error_code: str, message: str):
        super().__init__(f"{error_code} - {message}")
        self.error_code = error_code
        self.message= message


class ServiceUnavailable(MiiaError):
    """
    Raised when a service provided by a function is temporarily unavailable.

    This exception doesn't represent an error in the program, but rather an
    external issue (e.g. in a third-party HTTP service or database component).
    It is expected that repeating the call after some time may succeed if the
    external component operation returns to normal.
    """

    def __init__(self, message: str = "Service unavailable"):
        super().__init__("service_unavailable", message)
