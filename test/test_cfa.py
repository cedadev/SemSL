
from netCDF4 import Dataset as ncDataset
from SemSL.CFA._CFAClasses import *
from SemSL.CFA._CFAParsers import read_netCDF, write_netCDF
from SemSL.CFA._CFASplitter import CFASplitter

import numpy as np

TEST_DATASET="/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/cru_ts3.24.01.2011.2015.tmp.dat.nca"
OUT_TEST_DATASET="/Users/dhk63261/Archive/cru/data/cru_ts/cru_ts_3.24.01/data/tmp/reconstructed.dat.nca"

def test_creation():

    cfa_dataset = CFADataset(format='NETCDF4')

    cfa_dataset.createGroup("root")
    cfa_dataset.createGroup("group1")
    cfa_dataset.createGroup("group2")

    print (cfa_dataset.getGroups())


def test_parsing(filepath):
    #
    nc_dataset = ncDataset(filepath)
    cfa_dataset = read_netCDF(nc_dataset)
    nc_dataset.close()
    return cfa_dataset


def test_writing(cfa_dataset, filepath):
    #
    format = cfa_dataset.getFormat()
    nc_dataset = ncDataset(filepath, 'w', format=format)
    write_netCDF(cfa_dataset, nc_dataset)

def test_splitting():
    shape = np.array([60, 1, 145, 192])
    max_subarray_size = 1024 * 8
    axis_types = ['T', 'Z', 'Y', 'X']
    cfa_splitter = CFASplitter(shape, max_subarray_size, axis_types)
    subarray_shape = cfa_splitter.calculateSubarrayShape()

def test_createArray():
    cfa_dataset = CFADataset(name="fake_cru", format='NETCDF4')
    cfa_group = cfa_dataset.createGroup('root')
    time_dim = cfa_group.createDimension(
                  dim_name='time',
                  dim_len=60,
                  axis_type='T',
                  metadata={'standard_name' : 'time',
                            'units' : 'days since 01-01-1900'}
              )
    level_dim = cfa_group.createDimension(
                  dim_name='level',
                  dim_len=1,
                  axis_type='Z',
                  metadata={'standard_name' : 'height',
                            'units' : 'm'}
              )
    lat_dim = cfa_group.createDimension(
                  dim_name='latitude',
                  dim_len=178,
                  axis_type='Y',
                  metadata={'standard_name' : 'latitude',
                            'units' : 'degrees North'}
              )
    lon_dim = cfa_group.createDimension(
                  dim_name='longitude',
                  dim_len=359,
                  axis_type='X',
                  metadata={'standard_name' : 'longitude',
                            'units' : 'degrees East'}
              )

    cfa_var = cfa_group.createVariable(
                "tmp",
                np.dtype('f4'),
                dim_names=['time', 'level', 'latitude', 'longitude'],
                max_subarray_size=1024*256,
              )
    # print(cfa_dataset.root)
    # print(cfa_group.tmp)
    # print(cfa_group.longitude)
    # print(cfa_group.tmp.shape())
    return cfa_dataset

#cfa_dataset = test_parsing(TEST_DATASET)
#print (cfa_dataset['root']['tmp'][:])
#test_writing(cfa_dataset, OUT_TEST_DATASET)
#test_splitting()
cfa_dataset = test_createArray()
#print (cfa_dataset['root']['tmp'][:])
print (cfa_dataset['root']['tmp'][2:4,0,11:20,0:40])
#test_createArray()
