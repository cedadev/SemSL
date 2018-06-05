
class slBackend(object):
    """Base class for the SemSL backends.
       This class should not be used, only classes that inherit from this class.
    """
    def __init__(self):
        raise NotImplementedError

    def connect(self, url, credentials):
        raise NotImplementedError

    def get_id(self):
        return ("slBackend")
