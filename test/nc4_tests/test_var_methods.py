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


class test_set3_Methods_posix_cfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        f = Dataset('./testnc_methods.nc', 'w')
        f.setncattr('test','Created for SemSL tests')

        dim1 = f.createDimension('T', DIMSIZE)
        dim1d = f.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = f.createDimension('Z', DIMSIZE)
        dim2d = f.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        dim3 = f.createDimension('Y', DIMSIZE)
        dim3d = f.createVariable('Y', 'i4', ('Y',))
        dim3d[:] = range(DIMSIZE)
        dim4 = f.createDimension('X', DIMSIZE)
        dim4d = f.createVariable('X', 'i4', ('X',))
        dim4d[:] = range(DIMSIZE)
        dim1d.axis = "T"
        dim2d.axis = "Z"
        dim3d.axis = "Y"
        dim4d.axis = "X"
        var = f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        #print('IN TEST: {}'.format(self.var._cfa_file))
        #print('IN TEST: {}'.format(type(self.var)))
        #print('INTEST: {}'.format(self.var.setncattr))

        np.random.seed(0)
        var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        var.setncattr('units','test unit')
        f.close()

        f = Dataset('./testnc_methods.nc', 'r')
        var = f.variables['var']
        atr = var.getncattr('units')
        assert atr == 'test unit'
        f.close()

        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        #print('IN SETUP TEST SET3 {}'.format(f.variables['var']))
        atr = f.variables['var'].getncattr('units')
        assert atr == 'test unit'
        #print(self.f.variables['var'])
        f.close()

    def tearDown(self):
        # remove test file

        os.remove('./testnc_methods.nc')
        subfiles = glob('./testnc_methods/*.nc')
        #print('IN TEARDOWN {}'.format(subfiles))
        for f in subfiles:
            os.remove(f)
        os.rmdir('./testnc_methods')
        self.assertFalse(os.path.exists('./testnc_methods/'))


    def test_var_setncattr(self):
        f = Dataset('testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr('newtest','newtestvalue')
        v.settest = 'settestvalue'
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.newtest, 'newtestvalue')
        self.assertEqual(v.getncattr('settest'), 'settestvalue')
        f.close()
        # now check a subfile...
        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.newtest, 'newtestvalue')
        self.assertEqual(v.getncattr('settest'), 'settestvalue')
        f.close()

    def test_var_ncattrs(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['cf_role', 'cf_dimensions', 'cfa_array', 'units'])
        f.close()
        # now check a subfile...
        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['cf_role', 'cf_dimensions', 'cfa_array', 'units'])
        f.close()

    def test_var_setncattr_string(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()
        # now check a subfile...
        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncatts(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncatts({'newtest': 'newtestvalue', 'secondnew': 'secondnewval'})
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()
        # now check a subfile...
        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_var_getncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'), 'test unit')
        f.close()
        # now check a subfile...
        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'), 'test unit')
        f.close()

    def test_var_delncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.delncattr('units')
        f.close()
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()
        # now check a subfile...
        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()

    def test_get_var_chunk_cache(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.get_var_chunk_cache(), (1048576, 521, 0.75))
        f.close()

    def test_set_var_chunk_cache(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.set_var_chunk_cache(size=10485760)

        self.assertEqual(v.get_var_chunk_cache(), (10485760, 521, 0.75))
        f.close()

    def test_var_renameAttribute(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.renameAttribute('units', 'renamedattr')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()
        # now check a subfile...
        f = Dataset('./testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()

    # These don't work for cfa files either... TODO
    # def test_var_set_auto_chartostring(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].chartostring)
    #     v.set_auto_chartostring(False)
    #     self.assertFalse(f.variables['var'].chartostring)
    #     f.close()
    #
    # def test_var_set_auto_mask(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].mask)
    #     v.set_auto_mask(False)
    #     self.assertFalse(f.variables['var'].mask)
    #     f.close()
    #
    # def test_var_set_auto_maskandscale(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].mask)
    #     self.assertTrue(f.variables['var'].scale)
    #     v.set_auto_maskandscale(False)
    #     self.assertFalse(f.variables['var'].mask)
    #     self.assertFalse(f.variables['var'].scale)
    #     f.close()
    #
    # def test_var_set_auto_scale(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].scale)
    #     v.set_auto_scale(False)
    #     self.assertFalse(f.variables['var'].scale)
    #     f.close()

class test_set2_Methods_posix_noncfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        self.f = Dataset('./testnc_methods.nc', 'w',format='NETCDF4')
        self.f.setncattr('test','Created for SemSL tests')

        dim1 = self.f.createDimension('T', DIMSIZE)
        dim1d = self.f.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = self.f.createDimension('Z', DIMSIZE)
        dim2d = self.f.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        dim3 = self.f.createDimension('Y', DIMSIZE)
        dim3d = self.f.createVariable('Y', 'i4', ('Y',))
        dim3d[:] = range(DIMSIZE)
        dim4 = self.f.createDimension('X', DIMSIZE)
        dim4d = self.f.createVariable('X', 'i4', ('X',))
        dim4d[:] = range(DIMSIZE)
        dim1d.axis = "T"
        dim2d.axis = "Z"
        dim3d.axis = "Y"
        dim4d.axis = "X"
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        self.var.units = 'test unit'
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.f.close()

    def tearDown(self):
        # remove test file
        os.remove('./testnc_methods.nc')

    def test_var_ncattrs(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['units'])
        f.close()

    def test_var_setncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr('newtest','newtestvalue')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncattr_string(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncatts(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncatts({'newtest': 'newtestvalue', 'secondnew': 'secondnewval'})
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_var_getncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'),'test unit')
        f.close()

    def test_var_delncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.delncattr('units')
        f.close()
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()

    def test_get_var_chunk_cache(self):
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.get_var_chunk_cache(), (1048576, 521, 0.75))
        f.close()

    def test_set_var_chunk_cache(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.set_var_chunk_cache(size=10485760)

        self.assertEqual(v.get_var_chunk_cache(), (10485760, 521, 0.75))
        f.close()

    def test_var_renameAttribute(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        v.renameAttribute('units', 'renamedattr')
        f.close()
        f = Dataset('./testnc_methods.nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()

    def test_var_set_auto_chartostring(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].chartostring)
        v.set_auto_chartostring(False)
        self.assertFalse(f.variables['var'].chartostring)
        f.close()

    def test_var_set_auto_mask(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        v.set_auto_mask(False)
        self.assertFalse(f.variables['var'].mask)
        f.close()

    def test_var_set_auto_maskandscale(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_maskandscale(False)
        self.assertFalse(f.variables['var'].mask)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_var_set_auto_scale(self):
        f = Dataset('./testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_scale(False)
        self.assertFalse(f.variables['var'].scale)
        f.close()


class test_set5_Methods_s3_cfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        self.f = Dataset('s3://minio/databucket/testnc_methods.nc', 'w')
        self.f.setncattr('test','Created for SemSL tests')

        dim1 = self.f.createDimension('T', DIMSIZE)
        dim1d = self.f.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = self.f.createDimension('Z', DIMSIZE)
        dim2d = self.f.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        dim3 = self.f.createDimension('Y', DIMSIZE)
        dim3d = self.f.createVariable('Y', 'i4', ('Y',))
        dim3d[:] = range(DIMSIZE)
        dim4 = self.f.createDimension('X', DIMSIZE)
        dim4d = self.f.createVariable('X', 'i4', ('X',))
        dim4d[:] = range(DIMSIZE)
        dim1d.axis = "T"
        dim2d.axis = "Z"
        dim3d.axis = "Y"
        dim4d.axis = "X"
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        self.var.setncattr('units','test unit')
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.f.close()

    def tearDown(self):
        # remove test file
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        # construct list of subfiles
        subfiles = glob('{}/testnc_methods/*.nc'.format(cache_loc))
        subfiles = ['testnc_methods/'+file.split('/')[-1] for file in subfiles]

        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='testnc_methods.nc')
        for file in subfiles:
            #print(file)
            s3.delete_object(Bucket='databucket', Key=file)

        s3.delete_bucket(Bucket='databucket')


    def test_var_setncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr('newtest','newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()
        # now check a subfile...
        f = Dataset('s3://minio/databucket/testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_ncattrs(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['cf_role', 'cf_dimensions', 'cfa_array', 'units'])
        f.close()
        # now check a subfile...
        f = Dataset('s3://minio/databucket/testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['cf_role', 'cf_dimensions', 'cfa_array', 'units'])
        f.close()

    def test_var_setncattr_string(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()
        # now check a subfile...
        f = Dataset('s3://minio/databucket/testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncatts(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncatts({'newtest': 'newtestvalue', 'secondnew': 'secondnewval'})
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()
        # now check a subfile...
        f = Dataset('s3://minio/databucket/testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_var_getncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'), 'test unit')
        f.close()
        # now check a subfile...
        f = Dataset('s3://minio/databucket/testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'), 'test unit')
        f.close()

    def test_var_delncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.delncattr('units')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()
        # now check a subfile...
        f = Dataset('s3://minio/databucket/testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()

    def test_get_var_chunk_cache(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.get_var_chunk_cache(), (1048576, 521, 0.75))
        f.close()

    def test_set_var_chunk_cache(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.set_var_chunk_cache(size=10485760)

        self.assertEqual(v.get_var_chunk_cache(), (10485760, 521, 0.75))
        f.close()

    def test_var_renameAttribute(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.renameAttribute('units', 'renamedattr')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()
        # now check a subfile...
        f = Dataset('s3://minio/databucket/testnc_methods/testnc_methods_var_[0].nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()

    # These don't work for cfa files either... TODO
    # def test_var_set_auto_chartostring(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].chartostring)
    #     v.set_auto_chartostring(False)
    #     self.assertFalse(f.variables['var'].chartostring)
    #     f.close()
    #
    # def test_var_set_auto_mask(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].mask)
    #     v.set_auto_mask(False)
    #     self.assertFalse(f.variables['var'].mask)
    #     f.close()
    #
    # def test_var_set_auto_maskandscale(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].mask)
    #     self.assertTrue(f.variables['var'].scale)
    #     v.set_auto_maskandscale(False)
    #     self.assertFalse(f.variables['var'].mask)
    #     self.assertFalse(f.variables['var'].scale)
    #     f.close()
    #
    # def test_var_set_auto_scale(self):
    #     f = Dataset('./testnc_methods.nc', 'a')
    #     v = f.variables['var']
    #     self.assertTrue(f.variables['var'].scale)
    #     v.set_auto_scale(False)
    #     self.assertFalse(f.variables['var'].scale)
    #     f.close()

class test_set4_Methods_s3_noncfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        self.f = Dataset('s3://minio/databucket/testnc_methods.nc', 'w', format='NETCDF4')
        self.f.setncattr('test','Created for SemSL tests')

        dim1 = self.f.createDimension('T', DIMSIZE)
        dim1d = self.f.createVariable('T', 'i4', ('T',))
        dim1d[:] = range(DIMSIZE)
        dim2 = self.f.createDimension('Z', DIMSIZE)
        dim2d = self.f.createVariable('Z', 'i4', ('Z',))
        dim2d[:] = range(DIMSIZE)
        dim3 = self.f.createDimension('Y', DIMSIZE)
        dim3d = self.f.createVariable('Y', 'i4', ('Y',))
        dim3d[:] = range(DIMSIZE)
        dim4 = self.f.createDimension('X', DIMSIZE)
        dim4d = self.f.createVariable('X', 'i4', ('X',))
        dim4d[:] = range(DIMSIZE)
        dim1d.axis = "T"
        dim2d.axis = "Z"
        dim3d.axis = "Y"
        dim4d.axis = "X"
        self.var = self.f.createVariable(VARNAME, 'f8', ('T', 'Z', 'Y', 'X'), contiguous=True)
        self.var.units = 'test unit'
        np.random.seed(0)
        self.var[:] = np.random.rand(DIMSIZE, DIMSIZE, DIMSIZE, DIMSIZE)
        self.f.close()

    def tearDown(self):
        # remove test file
        sl_cache = slCacheManager()
        sl_config = slConfig()
        slDB = slCacheDB()
        cache_loc = sl_config['cache']['location']
        conn_man = slConnectionManager(sl_config)
        conn = conn_man.open("s3://minio")
        sl_cache._clear_cache()
        s3 = conn.get()
        s3.delete_object(Bucket='databucket', Key='testnc_methods.nc')
        s3.delete_bucket(Bucket='databucket')

    def test_var_ncattrs(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        attrs = v.ncattrs()
        self.assertEqual(attrs, ['units'])
        f.close()

    def test_var_setncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr('newtest','newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncattr_string(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncattr_string('newtest', 'newtestvalue')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        f.close()

    def test_var_setncatts(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.setncatts({'newtest': 'newtestvalue', 'secondnew': 'secondnewval'})
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.getncattr('newtest'), 'newtestvalue')
        self.assertEqual(v.getncattr('secondnew'), 'secondnewval')
        f.close()

    def test_var_getncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertEqual(v.getncattr('units'),'test unit')
        f.close()

    def test_var_delncattr(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.delncattr('units')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        test_bool = False
        try:
            units = v.getncattr('units')
        except AttributeError:
            test_bool = True

        self.assertTrue(test_bool)
        f.close()

    def test_get_var_chunk_cache(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        self.assertEqual(v.get_var_chunk_cache(), (1048576, 521, 0.75))
        f.close()

    def test_set_var_chunk_cache(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.set_var_chunk_cache(size=10485760)

        self.assertEqual(v.get_var_chunk_cache(), (10485760, 521, 0.75))
        f.close()

    def test_var_renameAttribute(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        v.renameAttribute('units', 'renamedattr')
        f.close()
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
        v = f.variables['var']
        test_bool = False
        try:
            attr = f.getncattr('units')
        except AttributeError:
            test_bool = True
        self.assertTrue(test_bool)
        self.assertEqual(v.getncattr('renamedattr'), 'test unit')
        f.close()

    def test_var_set_auto_chartostring(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].chartostring)
        v.set_auto_chartostring(False)
        self.assertFalse(f.variables['var'].chartostring)
        f.close()

    def test_var_set_auto_mask(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        v.set_auto_mask(False)
        self.assertFalse(f.variables['var'].mask)
        f.close()

    def test_var_set_auto_maskandscale(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].mask)
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_maskandscale(False)
        self.assertFalse(f.variables['var'].mask)
        self.assertFalse(f.variables['var'].scale)
        f.close()

    def test_var_set_auto_scale(self):
        f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
        v = f.variables['var']
        self.assertTrue(f.variables['var'].scale)
        v.set_auto_scale(False)
        self.assertFalse(f.variables['var'].scale)
        f.close()

if __name__ == '__main__':
    unittest.main()