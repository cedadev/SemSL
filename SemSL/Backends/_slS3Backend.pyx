import boto3

from _slBackend import slBackend
from SemSL._slExceptions import slIOException, slAPIException

class slS3Backend(slBackend):
    """Class for the S3 SemSL backend.
       This class uses boto3 to connect to a S3 object store / AWS.
    """
    def __init__(self):
        pass

    def connect(self, endpoint, credentials):
        """Create connection to object store / AWS, using the supplied
        credentials."""
        try:
            s3c = boto3.client("s3", endpoint_url=endpoint,
                               aws_access_key_id=credentials['access_key'],
                               aws_secret_access_key=credentials['secret_key'])
        except Exception as e:
            raise slIOException("Could not connect to S3 endpoint {} {}",
                                endpoint, e)
        return s3c

    def close(conn):
        """Static function to close a connection passed in."""
        # boto3 doesn't have a close connection method!
        pass

    def get_id(self):
        return ("slS3Backend")
