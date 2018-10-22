# Exception classes to indicate they come from the s3 component of the library
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

class s3IOException(BaseException):
    pass


class s3APIException(BaseException):
    pass