class HttpClientException(Exception):
    """A Custom exception class for handling HTTP errors."""

    message = None

    def __init__(self, message=None, response=None):
        super().__init__(message or self.message)
        self.response = response


class Http400Error(HttpClientException):
    """Class to handle 400 client exception."""

    message = "Bad Request. Client error - the request doesn't meet all requirements."


class Http401Error(HttpClientException):
    """Class to handle 401 Not Authorized exception."""

    message = "Not Authorized. OAuth2 token is either not provided or not valid."


class Http403Error(HttpClientException):
    """Class to handle 403 Access Denied exception."""

    message = (
        "Access denied. Your OAuth2 token is valid, but you are denied access - the response should contain details."
    )


class Http404Error(HttpClientException):
    """Class to handle 404 Not Found exception."""

    message = "Not Found. Resource was not found - it doesn't exist or it was deleted."


class Http409Error(HttpClientException):
    """Class to handle 409 Conflict exception."""

    message = "Conflict. Resource cannot be created because conflicting entity already exists."


class Http412Error(HttpClientException):
    """Class to handle 412 Precondition failed exception."""

    message = "Precondition failed. The request was well formed and valid, but some other conditions were not met."


class Http413Error(HttpClientException):
    """Class to handle 413 Payload Too Large exception."""

    message = "Payload Too Large. The request was well formed and valid, but some other conditions were not met."


class Http415Error(HttpClientException):
    """Class to handle 415 Unsupported Media Type exception."""

    message = "Unsupported Media Type. The API is unable to work with the provided payload."


class Http429Error(HttpClientException):
    """Class to handle 429 Rate Limit exception."""

    message = "Too Many Requests. You reached the rate limit, Please retry after sometime."


class Http500Error(HttpClientException):
    """Class to handle 500 Internal Server Error exception."""

    message = "Internal Server Error."


class Http503Error(HttpClientException):
    """Class to handle 503 Service Unavailable exception."""

    message = "Service Unavailable. The API cannot process the request at the moment."


class Http504Error(HttpClientException):
    """Class to handle 504 Gateway Timeout exception."""

    message = "Gateway Timeout. An internal call timed-out and the API was not able to finish your request."


class AccessTokenMissing(HttpClientException):
    """Class to handle Access Token Missing exception."""

    message = "Access token is missing, unable to authenticate in dev mode"
