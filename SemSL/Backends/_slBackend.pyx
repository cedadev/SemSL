__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

class slBackend(object):
    """Base class for the SemSL backends.
       This class should not be used, only classes that inherit from this class.
    """
    def __init__(self):
        raise NotImplementedError

    def connect(self, url, credentials):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

    def get_id(self):
        return ("slBackend")

    def download(self):
        raise NotImplementedError

    def remove(self):
        raise NotImplementedError

    def upload(self):
        raise NotImplementedError

    def open(self):
        raise NotImplementedError

    def list_buckets(self):
        raise NotImplementedError

    def get_object_size(self):
        raise NotImplementedError

    def get_head(self,fid):
        raise NotImplementedError

    def get_patial(self,fid,start,stop):
        raise NotImplementedError