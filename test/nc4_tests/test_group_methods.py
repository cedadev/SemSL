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

class test_groups_posix_noncfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        f = Dataset('./testnc_methods.nc', 'w',format='NETCDF4')
        f.setncattr('test', 'Created for SemSL tests')

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
        f.close()

    def tearDown(self):
        # remove test file
        os.remove('./testnc_methods.nc')

    def test_cmptypes(self):
        pass

    def test_createCompoundType(self):
        pass

    def test_createDimension(self):
        f = Dataset('./testnc_methods.nc','a')
        g = f.createGroup('testgroup')
        g.createDimension('extradim',10)
        f.close()

        f = Dataset('./testnc_methods.nc', 'r')
        for i,j in zip(f.dimensions.keys(),['T','Z','Y','X']):
            self.assertEqual(i,j)
        g = f.groups['testgroup']
        for i,j in zip(g.dimensions.keys(),['extradim']):
            self.assertEqual(i,j)
        f.close()

    def test_createEnumType(self):
        pass

    def test_createGroup(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.createVariable('tesgroupvar','f8')
        sg = g.createGroup('nestedgroup')
        sg.createVariable('nestedgroupvar','f8')

        f.close()

        f = Dataset('./testnc_methods.nc', 'a')

        g = f.groups['testgroup']

        sg = g.groups['nestedgroup']

        self.assertEqual(g.path,'/testgroup')
        self.assertEqual(sg.path,'/testgroup/nestedgroup')
        f.close()

    def test_createVLType(self):
        pass

    def test_data_model(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        self.assertEqual(g.data_model,'NETCDF4')
        f.close()

    def test_delncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr','testattrval')
        f.close()

        f = Dataset('./testnc_methods.nc', 'a')
        g = f.groups['testgroup']
        self.assertEqual(g.testattr,'testattrval')
        g.delncattr('testattr')
        self.assertEqual(g.ncattrs(),[])

        f.close()

    def test_dimensions(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.createDimension('extradim', 10)
        for i,j in zip(g.dimensions.keys(),['extradim']):
            self.assertEqual(i,j)

        f.close()

    def test_disk_format(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8')
        v[:] = 1
        self.assertEqual(g.disk_format, None)
        f.close()

    def test_enumtypes(self):
        pass

    def test_file_format(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        self.assertEqual(g.file_format,'NETCDF4')
        f.close()

    def test_filepath(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        self.assertEqual(g.filepath(), './testnc_methods.nc')
        f.close()

    def test_get_variables_by_attributes(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('testgroupvar', 'f8')
        v.testattr = 'val'
        v2 = g.createVariable('testgroupvar2', 'f8')
        v2.testattr = 'val'
        v3 = g.createVariable('tesgroupvar3', 'f8')
        sg = g.createGroup('nestedgroup')
        sv = sg.createVariable('nestedgroupvar', 'f8')
        sv.testattr = 'val'

        varlist = [x.name for x in g.get_variables_by_attributes(testattr='val')]

        self.assertEqual(varlist,['testgroupvar','testgroupvar2'])

        f.close()

    def test_getncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr','val')
        f.close()

        f = Dataset('./testnc_methods.nc', 'r')
        g = f.groups['testgroup']
        self.assertEqual(g.getncattr('testattr'),'val')
        f.close()

    def test_groups(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        sg = g.createGroup('nestedgroup')
        self.assertEqual([x for x in g.groups], ['nestedgroup'])
        f.close()

    def test_isopen(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        self.assertFalse(g.isopen())
        f.close()
        self.assertFalse(g.isopen())

    def test_keepweakref(self):
        pass

    def test_name(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        self.assertEqual(g.name, 'testgroup')
        f.close()

    def test_ncattrs(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr', 'val')
        g.setncattr('testattr2', 'val2')
        self.assertEqual(g.ncattrs(),['testattr','testattr2'])
        f.close()

    def test_parent(self):
        # TODO returns netcdf4-python object not semsl object
        # f = Dataset('./testnc_methods.nc', 'a')
        # g = f.createGroup('testgroup')
        # self.assertEqual(g.parent,f)
        # f.close()
        pass

    def test_path(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        self.assertEqual(g.path, '/testgroup')
        f.close()

    def test_renameAttribute(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr', 'val')
        g.setncattr('testattr2', 'val2')
        self.assertEqual(g.ncattrs(), ['testattr', 'testattr2'])
        f.close()
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.groups['testgroup']
        g.renameAttribute('testattr','renamedattr')
        self.assertEqual(g.ncattrs(), ['renamedattr', 'testattr2'])
        f.close()

    def test_renameDimension(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.createDimension('testdim',50)
        self.assertEqual([x for x in g.dimensions.keys()],['testdim'])
        g.renameDimension('testdim','renameddim')
        self.assertEqual([x for x in g.dimensions.keys()],['renameddim'])
        f.close()

    def test_renameGroup(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        gg = g.createGroup('subgroup')
        self.assertEqual([x for x in g.groups.keys()],['subgroup'])
        g.renameGroup('subgroup','renamedgroup')
        self.assertEqual([x for x in g.groups.keys()],['renamedgroup'])
        f.close()

    def test_renameVariable(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.createVariable('var','f8')
        self.assertEqual([x for x in g.variables.keys()], ['var'])
        g.renameVariable('var','renamedvar')
        self.assertEqual([x for x in g.variables.keys()], ['renamedvar'])
        f.close()

    def test_set_always_mask(self):
        # TODO
        pass

    def test_set_auto_chartostring(self):
        # TODO
        pass

    def test_set_auto_mask(self):
        # TODO
        pass

    def test_set_auto_maskandscale(self):
        # TODO
        pass

    def test_set_auto_scale(self):
        # TODO
        pass

    def test_set_fill_off(self):
        # TODO
        pass

    def test_set_fill_on(self):
        # TODO
        pass

    def test_setncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr','val')
        self.assertEqual(g.ncattrs(),['testattr'])
        f.close()

    def test_setncattr_string(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr', 'val')
        self.assertEqual(g.ncattrs(), ['testattr'])
        f.close()

    def test_setncatts(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncatts({'testattr':'val','at2':'val2'})
        self.assertEqual(g.ncattrs(), ['testattr','at2'])
        f.close()

    def test_sync(self):
        # TODO
        pass

    def test_vltypes(self):
        # TODO
        pass

    def test_variables(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.createVariable('var', 'f8')
        self.assertEqual([x for x in g.variables.keys()], ['var'])
        f.close()

class test_groups_posix_cfa(unittest.TestCase):
    def setUp(self):
        # Create test dataset
        f = Dataset('./testnc_methods.nc', 'w')
        f.setncattr('test', 'Created for SemSL tests')

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
        f.close()

    def tearDown(self):
        # remove test file
        os.remove('./testnc_methods.nc')
        subfiles = glob('./testnc_methods/*.nc')
        # print('IN TEARDOWN {}'.format(subfiles))
        for f in subfiles:
            os.remove(f)
        os.rmdir('./testnc_methods')
        self.assertFalse(os.path.exists('./testnc_methods/'))
        # raise ValueError
        # pass

    # def test_cmptypes(self):
    #     #TODO
    #     pass
    #
    # def test_createCompoundType(self):
    #     # TODO
    #     pass

    def test_createDimension(self):
        f = Dataset('./testnc_methods.nc','a')
        g = f.createGroup('testgroup')
        g.createDimension('extradim',10)
        g.createVariable('extradim','i4',('extradim',))
        v = g.createVariable('var','i4',('T','extradim',))
        v[:] = np.zeros((20,10))
        f.close()

        f = Dataset('./testnc_methods.nc', 'r')
        for i,j in zip(f.dimensions.keys(),['T','Z','Y','X']):
            self.assertEqual(i,j)
        g = f.groups['testgroup']

        for i,j in zip(g.dimensions.keys(),['extradim']):
            self.assertEqual(i,j)
        f.close()

        f = Dataset('./testnc_methods/testnc_methods_testgroup_var_[0].nc','r')
        for i, j in zip(f.dimensions.keys(), ['T', 'Z', 'Y', 'X']):
            self.assertEqual(i, j)
        g = f.groups['testgroup']

        for i, j in zip(g.dimensions.keys(), ['extradim']):
            self.assertEqual(i, j)
        f.close()

    # def test_createEnumType(self):
    #     pass

    def test_createGroup(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('testgroupvar','f8',('T','Z'))
        #sg = g.createGroup('nestedgroup') # TODO nested groups might not get supported...
        #v2 = sg.createVariable('nestedgroupvar','f8',('Y','X'))
        v[:] = np.zeros((20,20))
        #v2[:] = np.zeros((20,20))
        f.close()

        f = Dataset('./testnc_methods.nc', 'a')

        g = f.groups['testgroup']

        #sg = g.groups['nestedgroup']

        self.assertEqual(g.path,'/testgroup')
        #self.assertEqual(sg.path,'/testgroup/nestedgroup')
        f.close()

        f = Dataset('./testnc_methods/testnc_methods_testgroup_testgroupvar_[0].nc', 'r')
        self.assertEqual(f.groups['testgroup'].path, '/testgroup')

        f.close()

    # def test_createVLType(self):
    #     pass

    def test_single_dimension_variable(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr', 'testattrval')
        test_pass = False
        try:
            v = g.createVariable('var', 'f8', ('T'))
            test_pass = True

        except AttributeError:
            pass

        self.assertTrue(test_pass)
        v[:] = np.zeros(20)
        f.close()

    def test_data_model(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual(g.data_model,'NETCDF4')
        f.close()
        f = Dataset('./testnc_methods/testnc_methods_testgroup_var_[0].nc', 'r')
        self.assertEqual(f.groups['testgroup'].data_model, 'NETCDF4')

        f.close()

    def test_delncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr','testattrval')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        f.close()

        f = Dataset('./testnc_methods.nc', 'a')
        sf = Dataset('./testnc_methods/testnc_methods_testgroup_var_[0].nc', 'r')
        g = f.groups['testgroup']
        self.assertEqual(g.testattr,'testattrval')
        g.delncattr('testattr')
        f.close()
        sf.close()

        f = Dataset('./testnc_methods.nc', 'a')
        sf = Dataset('./testnc_methods/testnc_methods_testgroup_var_[0].nc', 'r')
        g = f.groups['testgroup']
        self.assertEqual(g.ncattrs(),[])

        f.close()
        sf.close()

    def test_dimensions(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        g.createDimension('extradim', 10)
        for i,j in zip(g.dimensions.keys(),['extradim']):
            self.assertEqual(i,j)

        f.close()

    def test_disk_format(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual(g.disk_format, None)
        f.close()

    # def test_enumtypes(self):
    #     pass

    def test_file_format(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual(g.file_format,'NETCDF4')
        f.close()

    def test_filepath(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual(g.filepath(), './testnc_methods.nc')
        f.close()

    def test_get_variables_by_attributes(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('testgroupvar', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        v.testattr = 'val'
        v2 = g.createVariable('testgroupvar2', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        v2.testattr = 'val'
        v3 = g.createVariable('tesgroupvar3', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        sg = g.createGroup('nestedgroup')
        sv = sg.createVariable('nestedgroupvar', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        sv.testattr = 'val'

        varlist = [x.name for x in g.get_variables_by_attributes(testattr='val')]

        self.assertEqual(varlist,['testgroupvar','testgroupvar2'])

        f.close()

    def test_getncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        g.setncattr('testattr','val')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        f.close()

        f = Dataset('./testnc_methods.nc', 'r')
        g = f.groups['testgroup']
        self.assertEqual(g.getncattr('testattr'),'val')
        f.close()

    def test_groups(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        sg = g.createGroup('nestedgroup')
        self.assertEqual([x for x in g.groups], ['nestedgroup'])
        f.close()

    def test_isopen(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertFalse(g.isopen())
        f.close()
        self.assertFalse(g.isopen())

    # def test_keepweakref(self):
    #     pass

    def test_name(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual(g.name, 'testgroup')
        f.close()

    def test_ncattrs(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        g.setncattr('testattr', 'val')
        g.setncattr('testattr2', 'val2')
        self.assertEqual(g.ncattrs(),['testattr','testattr2'])
        f.close()

    # def test_parent(self):
    #     # TODO returns netcdf4-python object not semsl object
    #     # f = Dataset('./testnc_methods.nc', 'a')
    #     # g = f.createGroup('testgroup')
    #     # self.assertEqual(g.parent,f)
    #     # f.close()
    #     pass

    def test_path(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual(g.path, '/testgroup')
        f.close()

    def test_renameAttribute(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        g.setncattr('testattr', 'val')
        g.setncattr('testattr2', 'val2')
        self.assertEqual(g.ncattrs(), ['testattr', 'testattr2'])
        f.close()
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.groups['testgroup']
        g.renameAttribute('testattr','renamedattr')
        self.assertEqual(g.ncattrs(), ['renamedattr', 'testattr2'])
        f.close()

    def test_renameDimension(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        g.createDimension('testdim',50)
        self.assertEqual([x for x in g.dimensions.keys()],['testdim'])
        g.renameDimension('testdim','renameddim')
        self.assertEqual([x for x in g.dimensions.keys()],['renameddim'])
        f.close()

    def test_renameGroup(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        gg = g.createGroup('subgroup')
        self.assertEqual([x for x in g.groups.keys()],['subgroup'])
        g.renameGroup('subgroup','renamedgroup')
        self.assertEqual([x for x in g.groups.keys()],['renamedgroup'])
        f.close()

    def test_renameVariable(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual([x for x in g.variables.keys()], ['var'])
        g.renameVariable('var','renamedvar')
        self.assertEqual([x for x in g.variables.keys()], ['renamedvar'])
        f.close()

    # def test_set_always_mask(self):
    #     # TODO
    #     pass
    #
    # def test_set_auto_chartostring(self):
    #     # TODO
    #     pass
    #
    # def test_set_auto_mask(self):
    #     # TODO
    #     pass
    #
    # def test_set_auto_maskandscale(self):
    #     # TODO
    #     pass
    #
    # def test_set_auto_scale(self):
    #     # TODO
    #     pass
    #
    # def test_set_fill_off(self):
    #     # TODO
    #     pass
    #
    # def test_set_fill_on(self):
    #     # TODO
    #     pass

    def test_setncattr(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var','f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        g.setncattr('testattr','val')
        self.assertEqual(g.ncattrs(),['testattr'])
        f.close()

    def test_setncattr_string(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        g.setncattr('testattr', 'val')
        self.assertEqual(g.ncattrs(), ['testattr'])
        f.close()

    def test_setncatts(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        g.setncatts({'testattr':'val','at2':'val2'})
        self.assertEqual(g.ncattrs(), ['testattr','at2'])
        f.close()

    # def test_sync(self):
    #     # TODO
    #     pass
    #
    # def test_vltypes(self):
    #     # TODO
    #     pass

    def test_variables(self):
        f = Dataset('./testnc_methods.nc', 'a')
        g = f.createGroup('testgroup')
        v = g.createVariable('var', 'f8',('T','Y','X'))
        v[:] = np.zeros((20,20,20))
        self.assertEqual([x for x in g.variables.keys()], ['var'])
        f.close()

# class test_groups_s3_noncfa(unittest.TestCase):
#     def setUp(self):
#         # Create test dataset
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'w',format='NETCDF4')
#         f.setncattr('test', 'Created for SemSL tests')
#
#         dim1 = f.createDimension('T', DIMSIZE)
#         dim1d = f.createVariable('T', 'i4', ('T',))
#         dim1d[:] = range(DIMSIZE)
#         dim2 = f.createDimension('Z', DIMSIZE)
#         dim2d = f.createVariable('Z', 'i4', ('Z',))
#         dim2d[:] = range(DIMSIZE)
#         dim3 = f.createDimension('Y', DIMSIZE)
#         dim3d = f.createVariable('Y', 'i4', ('Y',))
#         dim3d[:] = range(DIMSIZE)
#         dim4 = f.createDimension('X', DIMSIZE)
#         dim4d = f.createVariable('X', 'i4', ('X',))
#         dim4d[:] = range(DIMSIZE)
#         dim1d.axis = "T"
#         dim2d.axis = "Z"
#         dim3d.axis = "Y"
#         dim4d.axis = "X"
#         f.close()
#
#     def tearDown(self):
#         # remove test file
#         os.remove('./testnc_methods.nc')
#
#     def test_cmptypes(self):
#         pass
#
#     def test_createCompoundType(self):
#         pass
#
#     def test_createDimension(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc','a')
#         g = f.createGroup('testgroup')
#         g.createDimension('extradim',10)
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
#         for i,j in zip(f.dimensions.keys(),['T','Z','Y','X']):
#             self.assertEqual(i,j)
#         g = f.groups['testgroup']
#         for i,j in zip(g.dimensions.keys(),['extradim']):
#             self.assertEqual(i,j)
#         f.close()
#
#     def test_createEnumType(self):
#         pass
#
#     def test_createGroup(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.createVariable('tesgroupvar','f8')
#         sg = g.createGroup('nestedgroup')
#         sg.createVariable('nestedgroupvar','f8')
#
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#
#         g = f.groups['testgroup']
#
#         sg = g.groups['nestedgroup']
#
#         self.assertEqual(g.path,'/testgroup')
#         self.assertEqual(sg.path,'/testgroup/nestedgroup')
#         f.close()
#
#     def test_createVLType(self):
#         pass
#
#     def test_data_model(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         self.assertEqual(g.data_model,'NETCDF4')
#         f.close()
#
#     def test_delncattr(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr','testattrval')
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.groups['testgroup']
#         self.assertEqual(g.testattr,'testattrval')
#         g.delncattr('testattr')
#         self.assertEqual(g.ncattrs(),[])
#
#         f.close()
#
#     def test_dimensions(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.createDimension('extradim', 10)
#         for i,j in zip(g.dimensions.keys(),['extradim']):
#             self.assertEqual(i,j)
#
#         f.close()
#
#     def test_disk_format(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var','f8')
#         v[:] = 1
#         self.assertEqual(g.disk_format, None)
#         f.close()
#
#     def test_enumtypes(self):
#         pass
#
#     def test_file_format(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         self.assertEqual(g.file_format,'NETCDF4')
#         f.close()
#
#     def test_filepath(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         self.assertEqual(g.filepath(), './testnc_methods.nc')
#         f.close()
#
#     def test_get_variables_by_attributes(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('testgroupvar', 'f8')
#         v.testattr = 'val'
#         v2 = g.createVariable('testgroupvar2', 'f8')
#         v2.testattr = 'val'
#         v3 = g.createVariable('tesgroupvar3', 'f8')
#         sg = g.createGroup('nestedgroup')
#         sv = sg.createVariable('nestedgroupvar', 'f8')
#         sv.testattr = 'val'
#
#         varlist = [x.name for x in g.get_variables_by_attributes(testattr='val')]
#
#         self.assertEqual(varlist,['testgroupvar','testgroupvar2'])
#
#         f.close()
#
#     def test_getncattr(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr','val')
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
#         g = f.groups['testgroup']
#         self.assertEqual(g.getncattr('testattr'),'val')
#         f.close()
#
#     def test_groups(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         sg = g.createGroup('nestedgroup')
#         self.assertEqual([x for x in g.groups], ['nestedgroup'])
#         f.close()
#
#     def test_isopen(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         self.assertFalse(g.isopen())
#         f.close()
#         self.assertFalse(g.isopen())
#
#     def test_keepweakref(self):
#         pass
#
#     def test_name(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         self.assertEqual(g.name, 'testgroup')
#         f.close()
#
#     def test_ncattrs(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr', 'val')
#         g.setncattr('testattr2', 'val2')
#         self.assertEqual(g.ncattrs(),['testattr','testattr2'])
#         f.close()
#
#     def test_parent(self):
#         # TODO returns netcdf4-python object not semsl object
#         # f = Dataset('./testnc_methods.nc', 'a')
#         # g = f.createGroup('testgroup')
#         # self.assertEqual(g.parent,f)
#         # f.close()
#         pass
#
#     def test_path(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         self.assertEqual(g.path, '/testgroup')
#         f.close()
#
#     def test_renameAttribute(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr', 'val')
#         g.setncattr('testattr2', 'val2')
#         self.assertEqual(g.ncattrs(), ['testattr', 'testattr2'])
#         f.close()
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.groups['testgroup']
#         g.renameAttribute('testattr','renamedattr')
#         self.assertEqual(g.ncattrs(), ['renamedattr', 'testattr2'])
#         f.close()
#
#     def test_renameDimension(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.createDimension('testdim',50)
#         self.assertEqual([x for x in g.dimensions.keys()],['testdim'])
#         g.renameDimension('testdim','renameddim')
#         self.assertEqual([x for x in g.dimensions.keys()],['renameddim'])
#         f.close()
#
#     def test_renameGroup(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         gg = g.createGroup('subgroup')
#         self.assertEqual([x for x in g.groups.keys()],['subgroup'])
#         g.renameGroup('subgroup','renamedgroup')
#         self.assertEqual([x for x in g.groups.keys()],['renamedgroup'])
#         f.close()
#
#     def test_renameVariable(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.createVariable('var','f8')
#         self.assertEqual([x for x in g.variables.keys()], ['var'])
#         g.renameVariable('var','renamedvar')
#         self.assertEqual([x for x in g.variables.keys()], ['renamedvar'])
#         f.close()
#
#     # def test_set_always_mask(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_chartostring(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_mask(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_maskandscale(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_scale(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_fill_off(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_fill_on(self):
#     #     # TODO
#     #     pass
#
#     def test_setncattr(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr','val')
#         self.assertEqual(g.ncattrs(),['testattr'])
#         f.close()
#
#     def test_setncattr_string(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr', 'val')
#         self.assertEqual(g.ncattrs(), ['testattr'])
#         f.close()
#
#     def test_setncatts(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncatts({'testattr':'val','at2':'val2'})
#         self.assertEqual(g.ncattrs(), ['testattr','at2'])
#         f.close()
#
#     # def test_sync(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_vltypes(self):
#     #     # TODO
#     #     pass
#
#     def test_variables(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.createVariable('var', 'f8')
#         self.assertEqual([x for x in g.variables.keys()], ['var'])
#         f.close()
#
# class test_groups_s3_cfa(unittest.TestCase):
#     def setUp(self):
#         # Create test dataset
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'w')
#         f.setncattr('test', 'Created for SemSL tests')
#
#         dim1 = f.createDimension('T', DIMSIZE)
#         dim1d = f.createVariable('T', 'i4', ('T',))
#         dim1d[:] = range(DIMSIZE)
#         dim2 = f.createDimension('Z', DIMSIZE)
#         dim2d = f.createVariable('Z', 'i4', ('Z',))
#         dim2d[:] = range(DIMSIZE)
#         dim3 = f.createDimension('Y', DIMSIZE)
#         dim3d = f.createVariable('Y', 'i4', ('Y',))
#         dim3d[:] = range(DIMSIZE)
#         dim4 = f.createDimension('X', DIMSIZE)
#         dim4d = f.createVariable('X', 'i4', ('X',))
#         dim4d[:] = range(DIMSIZE)
#         dim1d.axis = "T"
#         dim2d.axis = "Z"
#         dim3d.axis = "Y"
#         dim4d.axis = "X"
#         f.close()
#
#     def tearDown(self):
#         # remove test file
#         os.remove('./testnc_methods.nc')
#         subfiles = glob('./testnc_methods/*.nc')
#         # print('IN TEARDOWN {}'.format(subfiles))
#         for f in subfiles:
#             os.remove(f)
#         os.rmdir('./testnc_methods')
#         self.assertFalse(os.path.exists('./testnc_methods/'))
#         # raise ValueError
#         # pass
#
#     # def test_cmptypes(self):
#     #     #TODO
#     #     pass
#     #
#     # def test_createCompoundType(self):
#     #     # TODO
#     #     pass
#
#     def test_createDimension(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc','a')
#         g = f.createGroup('testgroup')
#         g.createDimension('extradim',10)
#         g.createVariable('extradim','i4',('extradim',))
#         v = g.createVariable('var','i4',('T','extradim',))
#         v[:] = np.zeros((20,10))
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
#         for i,j in zip(f.dimensions.keys(),['T','Z','Y','X']):
#             self.assertEqual(i,j)
#         g = f.groups['testgroup']
#
#         for i,j in zip(g.dimensions.keys(),['extradim']):
#             self.assertEqual(i,j)
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods_testgroup_var_[0].nc','r')
#         for i, j in zip(f.dimensions.keys(), ['T', 'Z', 'Y', 'X']):
#             self.assertEqual(i, j)
#         g = f.groups['testgroup']
#
#         for i, j in zip(g.dimensions.keys(), ['extradim']):
#             self.assertEqual(i, j)
#         f.close()
#
#     # def test_createEnumType(self):
#     #     pass
#
#     def test_createGroup(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('tesgroupvar','f8',('T','Z'))
#         sg = g.createGroup('nestedgroup')
#         v2 = sg.createVariable('nestedgroupvar','f8',('Y','X'))
#         v[:] = np.zeros((20,20))
#         v2[:] = np.zeros((20,20))
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#
#         g = f.groups['testgroup']
#
#         sg = g.groups['nestedgroup']
#
#         self.assertEqual(g.path,'/testgroup')
#         self.assertEqual(sg.path,'/testgroup/nestedgroup')
#         f.close()
#
#     # def test_createVLType(self):
#     #     pass
#
#     def test_data_model(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var','f8',('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual(g.data_model,'NETCDF4')
#         f.close()
#
#     def test_delncattr(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr','testattrval')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.groups['testgroup']
#         self.assertEqual(g.testattr,'testattrval')
#         g.delncattr('testattr')
#         self.assertEqual(g.ncattrs(),[])
#
#         f.close()
#
#     def test_dimensions(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         g.createDimension('extradim', 10)
#         for i,j in zip(g.dimensions.keys(),['extradim']):
#             self.assertEqual(i,j)
#
#         f.close()
#
#     def test_disk_format(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var','f8',('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual(g.disk_format, None)
#         f.close()
#
#     # def test_enumtypes(self):
#     #     pass
#
#     def test_file_format(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var','f8',('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual(g.file_format,'NETCDF4')
#         f.close()
#
#     def test_filepath(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual(g.filepath(), 's3://minio/databucket/testnc_methods.nc')
#         f.close()
#
#     def test_get_variables_by_attributes(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('testgroupvar', 'f8',('T'))
#         v[:] = np.zeros(20)
#         v.testattr = 'val'
#         v2 = g.createVariable('testgroupvar2', 'f8',('T'))
#         v2[:] = np.zeros(20)
#         v2.testattr = 'val'
#         v3 = g.createVariable('tesgroupvar3', 'f8',('T'))
#         v3[:] = np.zeros(20)
#         sg = g.createGroup('nestedgroup')
#         sv = sg.createVariable('nestedgroupvar', 'f8',('T'))
#         sv[:] = np.zeros(20)
#         sv.testattr = 'val'
#
#         varlist = [x.name for x in g.get_variables_by_attributes(testattr='val')]
#
#         self.assertEqual(varlist,['testgroupvar','testgroupvar2'])
#
#         f.close()
#
#     def test_getncattr(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         g.setncattr('testattr','val')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         f.close()
#
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'r')
#         g = f.groups['testgroup']
#         self.assertEqual(g.getncattr('testattr'),'val')
#         f.close()
#
#     def test_groups(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         sg = g.createGroup('nestedgroup')
#         self.assertEqual([x for x in g.groups], ['nestedgroup'])
#         f.close()
#
#     def test_isopen(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         self.assertFalse(g.isopen())
#         f.close()
#         self.assertFalse(g.isopen())
#
#     # def test_keepweakref(self):
#     #     pass
#
#     def test_name(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual(g.name, 'testgroup')
#         f.close()
#
#     def test_ncattrs(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         g.setncattr('testattr', 'val')
#         g.setncattr('testattr2', 'val2')
#         self.assertEqual(g.ncattrs(),['testattr','testattr2'])
#         f.close()
#
#     # def test_parent(self):
#     #     # TODO returns netcdf4-python object not semsl object
#     #     # f = Dataset('./testnc_methods.nc', 'a')
#     #     # g = f.createGroup('testgroup')
#     #     # self.assertEqual(g.parent,f)
#     #     # f.close()
#     #     pass
#
#     def test_path(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual(g.path, '/testgroup')
#         f.close()
#
#     def test_renameAttribute(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         g.setncattr('testattr', 'val')
#         g.setncattr('testattr2', 'val2')
#         self.assertEqual(g.ncattrs(), ['testattr', 'testattr2'])
#         f.close()
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.groups['testgroup']
#         g.renameAttribute('testattr','renamedattr')
#         self.assertEqual(g.ncattrs(), ['renamedattr', 'testattr2'])
#         f.close()
#
#     def test_renameDimension(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         g.createDimension('testdim',50)
#         self.assertEqual([x for x in g.dimensions.keys()],['testdim'])
#         g.renameDimension('testdim','renameddim')
#         self.assertEqual([x for x in g.dimensions.keys()],['renameddim'])
#         f.close()
#
#     def test_renameGroup(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         gg = g.createGroup('subgroup')
#         self.assertEqual([x for x in g.groups.keys()],['subgroup'])
#         g.renameGroup('subgroup','renamedgroup')
#         self.assertEqual([x for x in g.groups.keys()],['renamedgroup'])
#         f.close()
#
#     def test_renameVariable(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var','f8', ('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual([x for x in g.variables.keys()], ['var'])
#         g.renameVariable('var','renamedvar')
#         self.assertEqual([x for x in g.variables.keys()], ['renamedvar'])
#         f.close()
#
#     # def test_set_always_mask(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_chartostring(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_mask(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_maskandscale(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_auto_scale(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_fill_off(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_set_fill_on(self):
#     #     # TODO
#     #     pass
#
#     def test_setncattr(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         g.setncattr('testattr','val')
#         self.assertEqual(g.ncattrs(),['testattr'])
#         f.close()
#
#     def test_setncattr_string(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         g.setncattr('testattr', 'val')
#         self.assertEqual(g.ncattrs(), ['testattr'])
#         f.close()
#
#     def test_setncatts(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         g.setncatts({'testattr':'val','at2':'val2'})
#         self.assertEqual(g.ncattrs(), ['testattr','at2'])
#         f.close()
#
#     # def test_sync(self):
#     #     # TODO
#     #     pass
#     #
#     # def test_vltypes(self):
#     #     # TODO
#     #     pass
#
#     def test_variables(self):
#         f = Dataset('s3://minio/databucket/testnc_methods.nc', 'a')
#         g = f.createGroup('testgroup')
#         v = g.createVariable('var', 'f8', ('T'))
#         v[:] = np.zeros(20)
#         self.assertEqual([x for x in g.variables.keys()], ['var'])
#         f.close()

if __name__ == '__main__':
    unittest.main()