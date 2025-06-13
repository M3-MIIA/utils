class ServiceUnavailable(Exception):
    """
    Raised when a service provided by a function is temporarily unavailable.

    This exception doesn't represent an error in the program, but rather an
    external issue (e.g. in a third-party HTTP service or database component).
    It is expected that repeating the call after some time may succeed if the
    external component operation returns to normal.
    """

    def __init__(self, reason: str = "Service unavailable"):
        super().__init__(reason)
