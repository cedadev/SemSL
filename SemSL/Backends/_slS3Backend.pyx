import boto3
from botocore.exceptions import ClientError
from botocore.client import Config

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
        #config = Config(connect_timeout=10, retries={'max_attempts': 0})
        try:
            s3c = boto3.client("s3", endpoint_url=endpoint,
                               aws_access_key_id=credentials['access_key'],
                               aws_secret_access_key=credentials['secret_key'])
        except Exception as e:
            raise slIOException("Could not connect to S3 endpoint {} {}",
                                endpoint, e)
        return s3c

    def close(self):
        """Static function to close a connection passed in."""
        # boto3 doesn't have a close connection method!
        pass

    def get_id(self):
        return ("slS3Backend")

    def download(self,conn,bucket,key,cacheloc):
        ''' Downloads file from OS to prescribed place in cache.
        '''
        try:
            conn.download_file(bucket,key, cacheloc)# only works with boto3 client objects
        except ClientError:
            raise ValueError('File not found')
    def create_bucket(self,conn,bucket):
        ''' Creates bucket.

        :param bucket:
        :return:
        '''
        conn.create_bucket(Bucket=bucket)

    def upload(self,conn,cloc,bucket,fname):
        ''' uploads file to backend

        :param conn:
        :param cloc:
        :param bucket:
        :param fname:
        :return:
        '''
        conn.upload_file(cloc,bucket,fname)

    def list_buckets(self,conn):
        return conn.list_buckets()['Buckets']

    def get_object_size(self,conn,bucket,fname):
        response = conn.head_object(Bucket=bucket,Key=fname)
        return response['ContentLength']

    def object_exists(self,conn,bucket,fname):
        try:
            conn.head_object(Bucket=bucket,Key=fname)
            return True
        except:
            return False