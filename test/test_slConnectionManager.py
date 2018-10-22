__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager

def test_slConnectionManager():
    # create config manager
    sl_config = slConfig()

    print ("===== S3 =====")
    # test S3
    conn_man = slConnectionManager(sl_config)
    conn1 = conn_man.open("s3://minio")
    conn2 = conn_man.open("s3://minio")
    print ("Total conns: {}\nOpen conns: {}\n".format(
           conn_man.total_connections("s3://minio"),
           conn_man.open_connections("s3://minio")))
    conn2.release()
    print ("Total conns: {}\nOpen conns: {}\n".format(
           conn_man.total_connections("s3://minio"),
           conn_man.open_connections("s3://minio")))
    conn2 = conn_man.open("s3://minio")
    print ("Total conns: {}\nOpen conns: {}\n".format(
           conn_man.total_connections("s3://minio"),
           conn_man.open_connections("s3://minio")))
    conn3 = conn_man.open("s3://minio")
    print ("Total conns: {}\nOpen conns: {}\n".format(
           conn_man.total_connections("s3://minio"),
           conn_man.open_connections("s3://minio")))
    conn1.close()
    conn2.close()
    conn3.close()
    print ("Total conns: {}\nOpen conns: {}\n".format(
           conn_man.total_connections("s3://minio"),
           conn_man.open_connections("s3://minio")))

    #print ("===== FTP =====")
    # test FTP
    #ftp1 = conn_man.open("ftp://vagrant")
    #ftp2 = conn_man.open("ftp://vagrant")
    #print ("Total conns: {}\nOpen conns: {}".format(
    #       conn_man.total_connections("ftp://vagrant"),
    #       conn_man.open_connections("ftp://vagrant")))

if __name__ == "__main__":
    test_slConnectionManager()
