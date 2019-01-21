# Exception classes to indicate they come from the sl component of the library
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

class slIOException(BaseException):
    pass


class slAPIException(BaseException):
    pass

class slInterfaceException(BaseException):
    pass

class slCacheException(BaseException):
    pass

class slConfigFileException(BaseException):
    pass

class slDBException(BaseException):
    pass

class slNetCDFException(BaseException):
    pass