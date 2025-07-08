from fastapi import HTTPException


class MiiaHttpError(HTTPException):
    """
    An HTTP exception you can raise in your own code to show errors to the
    client.

    This class can be used instead of the plain `HTTPException` class to ease
    composing error responses following our error response format pattern.
    """

    def __init__(self, status_code: int, error_code: str, message: str,
                 extra_fields: dict = {}
    ):
        super().__init__(status_code, detail={
            "error_code": error_code,
            "message": message,
            **extra_fields
        })
