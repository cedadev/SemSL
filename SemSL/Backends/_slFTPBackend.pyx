__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import ftplib

from _slBackend import slBackend
from SemSL._slExceptions import slIOException, slAPIException

class slFTPBackend(slBackend):
    """class for the FTP backends.
       This class uses FTPlib to connect to a FTP server.
    """
    def __init__(self):
        pass

    def connect(self, endpoint, credentials):
        """Create connection to FTP using the supplied credentials."""
        try:
            ftp = ftplib.FTP(endpoint,
                             user=credentials["user"],
                             passwd=credentials["password"])
        except Exception as e:
            raise slIOException("Could not connect to FTP endpoint {} {}",
                                endpoint, e)
        return ftp

    def close(conn):
        """Static function to close a connection passed in."""
        conn.quit()

    def get_id(self):
        return ("slFTPBackend")
