class BaseError(Exception):
    """Base package error."""


class InvalidErrorMessage(BaseError):
    """Model input contains an error./Spark Context error"""
