''' This file tests the SemSL netcdf4 frontend.
'''
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import unittest
import numpy as np
import os
import sys
from glob import glob

from SemSL.Frontends.SL_netCDF4 import slDataset as Dataset
from SemSL._slCacheManager import slCacheManager
from SemSL._slConfigManager import slConfig
from SemSL._slConnectionManager import slConnectionManager

from SemSL._slCacheDB import slCacheDB_lmdb as slCacheDB

import logging

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler()) # Writes to console
logger.setLevel(logging.DEBUG)
logging.getLogger('boto3').propagate=False
logging.getLogger('botocore').propagate=False
logging.getLogger('s3transfer').propagate=False
logging.getLogger('urllib3').propagate=False

FNAME = './testfile.nc'
VARNAME = 'var'
DIMSIZE = 20



if __name__ == '__main__':
    unittest.main()