""" SemSL interface to netCDF files"""
__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

#This module inherits from the standard netCDF4 implementation
# import as UniData netCDF4 to avoid confusion with the S3 module

import netCDF4._netCDF4 as netCDF4
from SemSL._slnetCDFIO import get_netCDF_file_details
from SemSL._slExceptions import *
from SemSL._CFAClasses import *
from SemSL._CFAFunctions import *
from psutil import virtual_memory

from SemSL._slConfigManager import slConfig
from SemSL._baseInterface import _baseInterface as interface
from SemSL._slCacheDB import slCacheDB_lmdb as slCacheDB
#from SemSL._slCacheDB import slCacheDB_lmdb_nest as slCacheDB
#from SemSL._slCacheDB import slCacheDB_lmdb_obj as slCacheDB
#from SemSL._slCacheDB import slCacheDB_sql as slCacheDB
from SemSL._slCacheManager import slCacheManager as slCache
from SemSL._slExceptions import slConfigFileException, slIOException, slNetCDFException


import SemSL._slUtils as slU

import os
import itertools
from collections import OrderedDict, deque

# these are class attributes that only exist at the python level (not in the netCDF file).
# the _private_atts list from netCDF4._netCDF4 will be extended with these
_sl_private_atts = [
 # member variables
 '_file_details', '_cfa_variables', '_sl_config', 'filename', 'slC', '_changed_attrs','groups'
]
netCDF4._private_atts.extend(_sl_private_atts)

class slDataset(object):
    """
       Inherit the UniData netCDF4 Dataset class and override some key member functions to allow the
       read and write of netCDF file to an object store accessed via an AWS S3 HTTP API.
    """

    def __init__(self, filename, mode='r', clobber=True, format='DEFAULT',
                 diskless=False, persist=False, keepweakref=False, memory=None,
                 **kwargs):
        """
        **`__init__(self, filename, mode="r", clobber=True, diskless=False,
           persist=False, keepweakref=False, format='NETCDF4')`**

        `S3netCDF4.Dataset` constructor
        See `netCDF4.Dataset` for full details of all the keywords
        """

        # we've passed all the details of detecting whether this is an S3 or POSIX file to the function
        # get_netCDFFilename(filename).  Diskless == always_stream


        # get the file details
        self._changed_attrs = False # False if not attrs changed, true if subfiles need
        self.slC = slCache()
        self.filename = filename
        self._file_details = get_netCDF_file_details(filename, mode, diskless, persist)
        self._cfa_variables = OrderedDict()
        self._variables_overwritten_by_cfa = OrderedDict()

        # get the s3ClientConfig for paths to the cache and max file size
        self._sl_config = slConfig()

        # list of subfiles accessed since intialistion
        self.subfiles_accessed = deque()

        DB = slCacheDB()
        slC = slCache()

        self.mode = mode

        # switch on the read / write / append mode
        if mode == 'r' or mode == 'a' or mode == 'r+':             # read
            # check whether the memory has been set from get_netCDF_file_details (i.e. the file is streamed to memory)

            c_file = slC.open(filename, mode)
            #self.ncD = netCDF4.Dataset(c_file, mode)
            if self._file_details.memory != "" or diskless:
                # we have to first create the dummy file (name held in file_details.memory) - check it exists before creating it
                if not os.path.exists(self._file_details.filename):
                    temp_file = netCDF4.Dataset(c_file, 'w', format=self._file_details.format).close()
                    # create the netCDF4 dataset from the data, using the temp_file
                    self.ncD = netCDF4.Dataset( self._file_details.filename, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=True, persist=False,
                                         keepweakref=keepweakref, memory=self._file_details.memory, **kwargs)
                    self.variables = self.ncD.variables
            else:
                # not in memory but has been streamed to disk
                self.ncD = netCDF4.Dataset(c_file, mode=mode, clobber=clobber,
                                         format=self._file_details.format, diskless=False, persist=persist,
                                         keepweakref=keepweakref, memory=None, **kwargs)
                self.variables = self.ncD.variables

            # check if file is a CFA file, for standard netCDF files
            try:
                cfa = "CFA" in self.getncattr("Conventions")
                self._file_details.cfa_file = 'CFA'
            except:
                cfa = False

            if cfa:
                # Get the host name in order to get the specific settings
                try:
                    host_name = slU._get_hostname(self._file_details.filename)
                    obj_size = self._sl_config['hosts'][host_name]['object_size']
                    read_threads = self._sl_config['hosts'][host_name]['read_connections']
                    write_threads = self._sl_config['hosts'][host_name]['write_connections']
                except slConfigFileException:
                    obj_size = 0
                    read_threads = 1
                    write_threads = 1
                # Parse the CFA metadata from this class' metadata
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.parse(self)
                self._file_details.cfa_file.format = self._file_details.format
                # recreate the variables as s3Variables and attach the cfa data

                for v in self.variables:
                    if v in self._file_details.cfa_file.cfa_vars:
                        self._cfa_variables[v] = slVariable(self.variables[v],
                                                            self._file_details.cfa_file,
                                                            self._file_details.cfa_file.cfa_vars[v],
                                                            {'cache_location' : self._sl_config['cache']['location'],
                                                             'max_object_size_for_memory' : obj_size,
                                                             'read_threads' : read_threads,
                                                             'write_threads' : write_threads,
                                                             'mode': mode})

                        self._variables_overwritten_by_cfa[v] = self.variables[v]

                        self.variables[v] = self._cfa_variables[v]

            else:
                self._file_details.cfa_file = None

        elif mode == 'w':           # write
            # check the format for writing - allow CFA4 in arguments and default to it as well
            # we DEFAULT to CFA4 for writing to S3 object stores so as to distribute files across objects

            if format == 'CFA4' or format == 'DEFAULT':
                self._file_details.format = 'NETCDF4'
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.format = self._file_details.format
                cfa = 'CFA4'
                self._file_details.cfa_file = 'CFA4'
            elif format == 'CFA3':
                self._file_details.format = 'NETCDF3_CLASSIC'
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.format = self._file_details.format
                cfa = 'CFA3'
                self._file_details.cfa_file = 'CFA3'
            else:

                self._file_details.format = format
                self._file_details.cfa_file = None
                cfa = None

            # if the file is diskless and an S3 file then we have to persist so that we can upload the file to S3
            if self._file_details.s3_uri != "" and diskless:
                persist = True
            c_file = slC.open(filename,mode)
            self.ncD = netCDF4.Dataset(c_file, mode=mode, clobber=clobber,
                                     format=self._file_details.format, diskless=diskless, persist=persist,
                                     keepweakref=keepweakref, memory=None, **kwargs)
            self.variables = self.ncD.variables

            if cfa:
                # Get the host name in order to get the specific settings
                try:
                    host_name = slU._get_hostname(self._file_details.filename)
                    obj_size = self._sl_config['hosts'][host_name]['object_size']
                    read_threads = self._sl_config['hosts'][host_name]['read_connections']
                    write_threads = self._sl_config['hosts'][host_name]['write_connections']
                except slConfigFileException:
                    obj_size = 0
                    read_threads = 1
                    write_threads = 1

                # Parse the CFA metadata from this class' metadata
                self._file_details.cfa_file = CFAFile()
                self._file_details.cfa_file.format = self._file_details.format
                # recreate the variables as s3Variables and attach the cfa data
                for v in self.variables:
                    if v in self._file_details.cfa_file.cfa_vars:
                        self._cfa_variables[v] = slVariable(self.variables[v],
                                                            self._file_details.cfa_file,
                                                            self._file_details.cfa_file.cfa_vars[v],
                                                            {'cache_location' : self._sl_config['cache']['location'],
                                                             'max_object_size_for_memory' : obj_size,
                                                             'read_threads' : read_threads,
                                                             'write_threads' : write_threads,
                                                             'mode' : mode})
                        self.variables[v] = self._cfa_variables[v]
            else:
                self._file_details.cfa_file = None

        else:
            # no other modes are supported
            raise slIOException("Mode " + mode + " not supported.")
        if self._file_details.cfa_file:
            self._file_details.cfa_file.groups = dict(self.groups)


    def __enter__(self):
        """Allows objects to be used with a `with` statement."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Allows objects to be used with a `with` statement."""
        self.close()

    def getVariable(self, name):
        """Get an s3 / cfa variable or just a standard netCDF4 variable,
           depending on its type.
           For a CFA-netCDF file, the dimension variables are standard netCDF4.Variables,
             and the field variables are s3Variables.
           For a netCDF file (no CFA splitting), all variables are standard netCDF4.Variables
        """
        if name in self._cfa_variables:
            return self._cfa_variables[name]
        else:
            return self.variables[name]

    def createDimension(self, dimname, size=None):
        """Overloaded version of createDimension that records the dimension info into a CFADim instance"""
        ncd = netCDF4.Dataset.createDimension(self.ncD, dimname, size)
        if self._file_details.cfa_file is not None:
            # add to the dimensions
            self._file_details.cfa_file.cfa_dims[dimname] = CFADim(dim_name=dimname, dim_len=size)
        return ncd

    def createVariable(self, varname, datatype, dimensions=(), zlib=False,
            complevel=4, shuffle=True, fletcher32=False, contiguous=False,
            chunksizes=None, endian='native', least_significant_digit=None,
            fill_value=None, chunk_cache=None):
        """Overloaded version of createVariable that has the following behaviour:
           For standard netCDF files (non CFA split) just pass through to the base method.
           For CF-netCDF files, create the variable with no dimensions, and create the
           required CFA metadata."""

        slC = slCache()
        if self._file_details.cfa_file is None:
            var = netCDF4.Dataset.createVariable(self.ncD, varname, datatype, dimensions, zlib,
                    complevel, shuffle, fletcher32, contiguous,
                    chunksizes, endian, least_significant_digit,
                    fill_value, chunk_cache)
            return var
        else:
            # get the variable shape, so we can determine the partitioning scheme
            # (and whether there should even be a partitioning scheme)
            var_shape = []
            for d in dimensions:
                var_shape.append(self.dimensions[d].size)

            # is the variable name in the dimensions?
            if varname in self.dimensions or var_shape == []:
                # it is so create the Variable with dimensions
                var = netCDF4.Dataset.createVariable(self.ncD, varname, datatype, dimensions, zlib,
                        complevel, shuffle, fletcher32, contiguous,
                        chunksizes, endian, least_significant_digit,
                        fill_value, chunk_cache)
                return var
            else:
                # it is not so create a dimension free version
                # We need to check that the dimension variables exist before we can create the variable which uses them
                for d in dimensions:
                    try:
                        dvar = self.variables[d]
                    except KeyError as e:
                        raise slNetCDFException('{}\n\nError: The dimension variables need to created before a '
                                         'variable which relies on them.'.format(e))


                # Get the host name in order to get the obj size, otherwise refer to default
                try:
                    host_name = slU._get_hostname(self._file_details.filename)
                    obj_size = self._sl_config['hosts'][host_name]['object_size']
                    read_threads = self._sl_config['hosts'][host_name]['read_connections']
                    write_threads = self._sl_config['hosts'][host_name]['write_connections']
                except slConfigFileException:
                    obj_size = 0
                    obj_size = self._sl_config['system']['default_object_size']
                    read_threads = 1
                    write_threads = 1

                # create the partitions, i.e. a list of CFAPartition, and get the partition shape
                # get the max file size from the s3ClientConfig

                base_filename = self.filename.replace('.nc','')
                pmshape, partitions, subarrayshape = create_partitions(base_filename, self, dimensions,
                                                        varname, var_shape, np.arange(1,dtype=datatype),
                                                        max_file_size=obj_size,
                                                        format="netCDF")
                # The "np.arange(1,dtype=datatype)" above is a hack to get around the fact that the function expects
                # var.dtype, but the variable hasn't been created yet!

                # Check whether the chunksize is larger than the subfile size
                # Raising this exception here means that the variable will not be created if the chunksize is not valid
                # whereas if netcdf4 python is left to throw the exception, the master variable gets created but the
                # sub files do not.
                if chunksizes is not None:
                    for i,sv_el in enumerate(subarrayshape):
                        if chunksizes[i] > sv_el:
                            raise slNetCDFException('The chunksize {} is incompatible with the subarray shape {}, please'
                                             ' make the chunksize smaller than the subarray shape.'.format
                                                                                                    (chunksizes,
                                                                                                     subarrayshape))

                # Create the master variable
                var = netCDF4.Dataset.createVariable(self.ncD, varname, datatype, (), zlib,
                        complevel, shuffle, fletcher32, contiguous,
                        chunksizes, endian, least_significant_digit,
                        fill_value, chunk_cache)

                # create the CFAVariable here
                self._file_details.cfa_file.cfa_vars[varname] = CFAVariable(varname,
                                                                cf_role="cfa_variable", cfa_dimensions=list(dimensions),
                                                                pmdimensions=list(dimensions), pmshape=pmshape,
                                                                base="", partitions=partitions)
                # add the metadata to the variable
                cfa_var_meta = self._file_details.cfa_file.cfa_vars[varname].dict()
                for k in cfa_var_meta:
                    if k == "cfa_array":        # convert the cfa_array metadata to json
                        var.setncattr(k, json.dumps(cfa_var_meta[k]))
                    else:
                        var.setncattr(k, cfa_var_meta[k])

                # check whether we need to copy the dimension values and metadata into the variable
                for d in dimensions:
                    if len(self._file_details.cfa_file.cfa_dims[d].values) == 0:
                        # values
                        self._file_details.cfa_file.cfa_dims[d].values = np.array(self.variables[d][:])
                        # metadata
                        md = {k: self.variables[d].getncattr(k) for k in self.variables[d].ncattrs()}
                        self._file_details.cfa_file.cfa_dims[d].metadata = md

                # keep the calling parameters in a dictionary, and add the parameters from the client config
                parameters = {'varname' : varname, 'datatype' : datatype, 'dimensions' : dimensions, 'zlib' : zlib,
                              'complevel' : complevel, 'shuffle' : shuffle, 'fletcher32' : fletcher32,
                              'contiguous' : contiguous, 'chunksizes' : chunksizes, 'endian' : endian,
                              'least_significant_digit' : least_significant_digit,
                              'fill_value' : fill_value, 'chunk_cache' : chunk_cache,
                              'cache_location' : self._sl_config['cache']['location'],
                              'max_object_size_for_memory' : obj_size,
                              'write_threads' : write_threads,
                              'read_threads' : read_threads,
                              'mode':self.mode}

                # create the s3Variable which is a reimplementation of the netCDF4 variable
                self._cfa_variables[varname] = slVariable(var, self._file_details.cfa_file,
                                                          self._file_details.cfa_file.cfa_vars[varname],
                                                          parameters)
                return self._cfa_variables[varname]

    def close(self):
        """Close the netCDF file.  If it is a S3 file and the mode is write then upload to the storage."""
        # for each variable, get the accessed subfiles

        # TODO do we want to check if has been sync'd before re uploading everything?? could use attr flag

        sl_config = slConfig()

        subfiles = self.get_subfiles_accessed()

        if (self._file_details.filemode == 'w' or
                self._file_details.filemode == "r+" or
                self._file_details.filemode == 'a'):
            if self._file_details.cfa_file is not None:
                try:
                    conv_attrs = self.getncattr("Conventions")
                    self.setncattr("Conventions", conv_attrs + " CFA-0.4")
                except:
                    self.setncattr("Conventions", "CFA-0.4")
        netCDF4.Dataset.close(self.ncD)
        slC = slCache()
        if self._file_details.s3_uri == '':
            slC.close(self._file_details.filename,self.mode,subfiles)
        else:
            slC.close(self._file_details.s3_uri,self.mode,subfiles)

    def flush(self):
        return self.sync()

    def update_cfa_meta(self,subfiles):
        # Update the attributes of the subfiles
        # get global attrs from master file
        changed_files = subfiles.copy()

        globattrs_list = self.ncattrs()  # this is a list, it needs to be a dict
        globattrs = {}
        for att in globattrs_list:
            globattrs[att] = self.getncattr(att)
        for varname in self._cfa_variables.keys():
            if self._cfa_variables[varname]._changed_attrs:
                varattrs_list = self.variables[varname].ncattrs()  # this is a list, it needs to be a dict
                varattrs = {}
                for att in varattrs_list:
                    varattrs[att] = self.variables[varname].getncattr(att)
                # print(varattrs)
                svs, sfs, sgs, open_files, subfiles_attr = self.return_subvars(varname=varname)
                # add any subfiles into the list of files to upload

                for subfile in subfiles_attr:
                    if slU._get_alias(subfile):  # only append if the file is in a backend
                        changed_files.append(subfile)
                # print('IN FILE CLOSE: VAR ATTS {}'.format(varattrs))
                for sv in svs:
                    sv.setncatts(varattrs)
                    if not varattrs_list == sv.ncattrs():
                        for old_att in sv.ncattrs():
                            if not old_att in varattrs_list:
                                sv.delncattr(old_att)
                for sf in sfs:
                    sf.setncatts(globattrs)
                    if not globattrs_list == sf.ncattrs():
                        for old_att in sf.ncattrs():
                            if not old_att in globattrs_list:
                                sf.delncattr(old_att)

                for sg in sgs:
                    gname = sg.name
                    mastergroup = self.groups[gname]
                    gattrs = mastergroup.ncattrs()
                    if not gattrs == sg.ncattrs():
                        for old_att in sg.ncattrs():
                            if not old_att in gattrs:
                                sg.delncattr(old_att)

                self.close_subfiles(sfs)
        return changed_files

    def sync_subfiles(self):
        for varname in self._cfa_variables.keys():
            svs, sfs, sgs, open_files, subfiles = self.return_subvars(varname=varname)
            for sf in sfs:
                sf.sync()
            self.close_subfiles(sfs)

    def get_subfiles_accessed(self):
        for v in self._cfa_variables.values():
            self.subfiles_accessed.extend(v._accessed_subfiles())

        # Add subfiles to the accessed subfiles
        subfiles = self.subfiles_accessed

        # update meta data in cfa files
        updated_files = self.update_cfa_meta(subfiles)
        # add the changed files to the list of subfiles accessed
        subfiles.extend(updated_files)

        # remove duplicates from the subfiles list
        try:
            subfiles = list(set(subfiles))
        except TypeError:
            subfiles = subfiles[0]

        return subfiles

    def sync(self):
        """ Overloads the netcdf4 method which syncs to disk.
            Syncs the open dataset to disk and backend as required.
        """

        subfiles = self.get_subfiles_accessed()

        if not self._file_details.s3_uri == '':
            # sync main file
            self.ncD.sync()
            self.sync_subfiles()

        elif not self._file_details.cfa_file:
            self.ncD.sync()
        else:
            self.ncD.sync()
            self.sync_subfiles()
            self.slC.close(self._file_details.s3_uri,self.mode,subfiles)

        # All subfiles are up to date to clear the list of accessed files
        self.subfiles_accessed = []

    @property
    def cmptypes(self):
        return self.ncD.cmptypes

    def createCompoundType(self, datatype, datatype_name):
        return self.ncD.createCompoundType(datatype, datatype_name)

    def createEnumType(self,datatype,datatype_name,enum_dict):
        return self.ncD.createEnumType(datatype,datatype_name,enum_dict)

    def createGroup(self,groupname):
        _group = self.ncD.createGroup(groupname)

        return slGroup(groupname,_group,self._file_details,self.dimensions,self.ncD,self.mode)

    def createVLType(self, datatype, datatype_name):
        return self.ncD.createVLType(datatype,datatype_name)

    @property
    def VLTypes(self):
        return self.ncD.vltypes

    @property
    def data_model(self):
        return self.ncD.data_model

    def delncattr(self,name):
        return self.ncD.delncattr(name)

    @property
    def dimensions(self):
        return self.ncD.dimensions

    @property
    def disk_format(self):
        return self.ncD.disk_format

    @property
    def enumtypes(self):
        return self.ncD.enumtypes

    @property
    def file_format(self):
        return self.ncD.file_format

    def filepath(self,encoding=None):
        return self.filename

    def get_variables_by_attribute(self,**kwargs):
        return self.ncD.get_variables_by_attributes(**kwargs)

    def getncattr(self,name):
        return self.ncD.getncattr(name)

    @property
    def groups(self):
        return self.ncD.groups

    def ncattrs(self):
        return self.ncD.ncattrs()

    @property
    def parent(self):
        return self.ncD.parent

    @property
    def path(self):
        return self.ncD.path

    def renameAttribute(self,oldname,newname):
        return self.ncD.renameAttribute(oldname,newname)

    def close_subfiles(self,file_list):
        for file in file_list:
            file.close()

    def upload_subfiles(self,file_list):
        # upload sub files in bulk, won't do anything if posix files are passed to it
        self.slC.bulk_upload(file_list)

    def return_subvars(self,varname=False,newname=False):
        # returns a list of the sub varible objects to update, and the posix files and cache files
        # to enable closing

        # if files not in cache then need to download in bulk from backend BEFORE updating files
        # for all subfiles, check the cache, and create list of all ones not in there
        # the size of the total files also needs to be caculated to make sure it is not larger than the
        # max cache size
        # get the list of subfiles from the master file

        # if the variable name is not supplied, get all the variables in the dataset
        if not varname:
            vars = self._cfa_variables
        else:
            vars = [varname]
        svs = []
        sfs = []
        sgs = []
        files_for_download = []
        cache_locs = []
        posix_files = []
        all_open_files = []
        subfiles = []

        for var in vars:
            if not newname:
                varobj = self.variables[var]
            else:
                 varobj = self.variables[newname]
            varpartionsdict = varobj.getncattr('cfa_array')
            attr_split = [el.split('"') for el in varpartionsdict.split('file')]
            for slist in attr_split:
                [subfiles.append(i) for i in slist if ".nc" in i]

            for file in subfiles:
                if self.slC._check_whether_posix(file, 'a') == 'Alias exists':
                    # Build download list
                    if not self.slC.DB.check_cache(file):
                        files_for_download.append(file)
                    # create list of the paths for each file in the cache
                    cache_loc = self.slC.DB.get_cache_loc(file)
                    cache_locs.append(cache_loc)
                    all_open_files.append(cache_loc)
                # if it is a posix file might as well append
                elif self.slC._check_whether_posix(file, 'a'):
                    posix_files.append(file)
                    all_open_files.append(file)
                elif not self.slC._check_whether_posix:
                    raise slIOException("Alias doesn't exist, and the lib doesn't "
                                     "think the files path is POSIX for {}".format(file))

            if len(files_for_download)>0:
                self.slC.bulk_download(files_for_download)

            # add any posix files
            for file in posix_files:
                sf = netCDF4.Dataset(file,'a')
                sfs.append(sf)
                svs.append(sf.variables[var])
                # check for groups and group vars
                for sg in sf.groups:
                    sgs.append(sg)
                    svs.append(sg.variables[var])

            # now add the downloaded vars
            for file in cache_locs:
                # need to open each subfile
                sf = netCDF4.Dataset(file,'a')
                sfs.append(sf)
                svs.append(sf.variables[var])
                # check for groups and group vars
                for sg in sf.groups:
                    sgs.append(sg)
                    svs.append(sg.variables[var])

        return svs, sfs, sgs, all_open_files, subfiles

    def renameDimension(self,oldname,newname):
        self.ncD.renameDimension(oldname,newname)
        self.ncD.renameVariable(oldname,newname)

        if self._file_details.cfa_file is not None:
            svs, sfs, sgs, open_files, subfiles = self.return_subvars()
            # loop through variables
            for sf in sfs:
                sf.renameDimension(oldname, newname)
                sf.renameVariable(oldname, newname)

            self.close_subfiles(sfs)
            self.subfiles_accessed.extend(subfiles)

    def renameGroup(self,oldname,newname):
        self.ncD.renameGroup(oldname,newname)

        if self._file_details.cfa_file is not None:
            svs, sfs, sgs, open_files, subfiles = self.return_subvars()
            # loop through variables
            for sf in sfs:
                sf.renameGroup(oldname,newname)
            self.close_subfiles(sfs)
            self.subfiles_accessed.extend(subfiles)

    def rename_cfa_files(self, open_files, oldname, newname):
        # rename the cfa subfiles and update the cache DB
        for file in open_files:
            new_path_name = file.replace(oldname,newname)
            os.rename(file, new_path_name)

    def renameVariable(self,oldname,newname):
        # rename the required variable
        self.ncD.renameVariable(oldname,newname) # this doesn't work for cfa variables

        if self._file_details.cfa_file is not None:

            svs, sfs, sgs, open_files, subfiles = self.return_subvars(varname=oldname,newname=newname)
            # loop through variables
            for sf in sfs:
                sf.renameVariable(oldname,newname)
            self.close_subfiles(sfs)

            # Need to rename the files in cache or if posix files
            # new files is a list of the changed subfiles for posix and and backend path
            # openfiles is the list of posix and files in cache ie 'real' files
            self.rename_cfa_files(open_files, oldname, newname)

            # rename the cfa files
            new_cfa_files = [x.replace(oldname,newname) for x in subfiles]

            # rename entry in cacheDB
            if not self._file_details.s3_uri == '':
                self.slC.DB.rename_entry(subfiles, new_cfa_files)
            # Need to remove the old named files from the backend
            self.slC.remove_from_backend(subfiles)

            # Now upload the new files
            self.subfiles_accessed.extend(new_cfa_files)

    def set_auto_chartostring(self,True_or_False):
        return self.ncD.set_auto_chartostring(True_or_False)

    def set_auto_mask(self,True_or_False):
        return  self.ncD.set_auto_mask(True_or_False)

    def set_auto_maskandscale(self,True_or_False):
        return self.ncD.set_auto_maskandscale(True_or_False)

    def set_auto_scale(self,True_or_False):
        return self.ncD.set_auto_scale(True_or_False)

    def set_fill_off(self):
        return self.ncD.set_fill_off()

    def set_fill_on(self):
        return self.ncD.set_fill_on()

    def setncattr(self,name,value):
        return self.ncD.setncattr(name,value)

    def setncattr_string(self,name,value):
        return self.ncD.setncattr_string(name,value)

    def setncattrs(self,attdict):
        return self.ncD.setncatts(attdict)

    @property
    def vltypes(self):
        return self.ncD.vltypes

class slGroup(object):
    """
     Reimplement the netcdf4 group class to enable groups to be used with cfa files.

     I think this should work the same way as the file object, except on create variable, where the sub file gets an
     attribute to say which group it is in, for rebuilds?
    """

    def __init__(self, groupname, _group, _file_details,_dims,_parent, mode):
        self.groupname = groupname
        self._group = _group
        self._file_details = _file_details
        self._cfa_variables = OrderedDict()
        self._nc_dims = _dims
        self._nc_parent = _parent
        self._sl_config = slConfig()
        self.mode = mode



    def createVariable(self, varname, datatype, dimensions=(), zlib=False,
                       complevel=4, shuffle=True, fletcher32=False, contiguous=False,
                       chunksizes=None, endian='native', least_significant_digit=None,
                       fill_value=None, chunk_cache=None):
        """Overloaded version of createVariable that has the following behaviour:
           For standard netCDF files (non CFA split) just pass through to the base method.
           For CF-netCDF files, create the variable with no dimensions, and create the
           required CFA metadata."""

        slC = slCache()
        if self._file_details.cfa_file is None:
            var = self._group.createVariable(varname, datatype, dimensions, zlib,
                                                 complevel, shuffle, fletcher32, contiguous,
                                                 chunksizes, endian, least_significant_digit,
                                                 fill_value, chunk_cache)
            return var
        else:
            # get the variable shape, so we can determine the partitioning scheme
            # (and whether there should even be a partitioning scheme)
            var_shape = []
            for d in dimensions:
                try:
                    var_shape.append(self._nc_dims[d].size)
                except KeyError:
                    var_shape.append(self._group.dimensions[d].size)

            # is the variable name in the dimensions?
            if varname in self._group.dimensions or varname in self._nc_parent.dimensions or var_shape == []:
                # it is so create the Variable with dimensions
                var = self._group.createVariable(varname, datatype, dimensions, zlib,
                                                     complevel, shuffle, fletcher32, contiguous,
                                                     chunksizes, endian, least_significant_digit,
                                                     fill_value, chunk_cache)
                return var
            else:
                # it is not so create a dimension free version
                # We need to check that the dimension variables exist before we can create the variable which uses them
                for d in dimensions:
                    try:
                        dvar = self.variables[d]
                    except KeyError as e:
                        try:
                            dvar = self._nc_parent.variables[d]
                        except KeyError as e:
                            raise slNetCDFException('{}\n\nError: The dimension variables need to created before a '
                                             'variable which relies on them.'.format(e))

                # Get the host name in order to get the obj size, otherwise refer to default
                try:
                    host_name = slU._get_hostname(self._file_details.filename)
                    obj_size = self._sl_config['hosts'][host_name]['object_size']
                    read_threads = self._sl_config['hosts'][host_name]['read_connections']
                    write_threads = self._sl_config['hosts'][host_name]['write_connections']
                except slConfigFileException:
                    obj_size = 0
                    obj_size = self._sl_config['system']['default_object_size']
                    read_threads = 1
                    write_threads = 1

                # create the partitions, i.e. a list of CFAPartition, and get the partition shape
                # get the max file size from the s3ClientConfig

                base_filename = self._file_details.filename.replace('.nc', '')
                pmshape, partitions, subarrayshape = create_partitions(base_filename, self._group, dimensions,
                                                                       varname, var_shape,
                                                                       np.arange(1, dtype=datatype),
                                                                       max_file_size=obj_size,
                                                                       format="netCDF",group=self.groupname, parent = self._nc_parent)

                # The "np.arange(1,dtype=datatype)" above is a hack to get around the fact that the function expects
                # var.dtype, but the variable hasn't been created yet!

                # Check whether the chunksize is larger than the subfile size
                # Raising this exception here means that the variable will not be created if the chunksize is not valid
                # whereas if netcdf4 python is left to throw the exception, the master variable gets created but the
                # sub files do not.
                if chunksizes is not None:
                    for i, sv_el in enumerate(subarrayshape):
                        if chunksizes[i] > sv_el:
                            raise slNetCDFException('The chunksize {} is incompatible with the subarray shape {}, please'
                                             ' make the chunksize smaller than the subarray shape.'.format
                                             (chunksizes,
                                              subarrayshape))

                # Create the master variable
                var = self._group.createVariable(varname, datatype, (), zlib,
                                                     complevel, shuffle, fletcher32, contiguous,
                                                     chunksizes, endian, least_significant_digit,
                                                     fill_value, chunk_cache)

                # create the CFAVariable here
                self._file_details.cfa_file.cfa_vars[varname] = CFAVariable(varname,
                                                                            cf_role="cfa_variable",
                                                                            cfa_dimensions=list(dimensions),
                                                                            pmdimensions=list(dimensions),
                                                                            pmshape=pmshape,
                                                                            base="", partitions=partitions)
                # add the metadata to the variable
                cfa_var_meta = self._file_details.cfa_file.cfa_vars[varname].dict()
                for k in cfa_var_meta:
                    if k == "cfa_array":  # convert the cfa_array metadata to json
                        var.setncattr(k, json.dumps(cfa_var_meta[k]))
                    else:
                        var.setncattr(k, cfa_var_meta[k])

                # check whether we need to copy the dimension values and metadata into the variable
                for d in dimensions:
                    if len(self._file_details.cfa_file.cfa_dims[d].values) == 0:
                        # values
                        try:
                            self._file_details.cfa_file.cfa_dims[d].values = np.array(self._nc_parent.variables[d][:])
                        except KeyError:
                            self._file_details.cfa_file.cfa_dims[d].values = np.array(self._group.variables[d][:])
                        # metadata
                        try:
                            md = {k: self._nc_parent.variables[d].getncattr(k) for k in self._nc_parent.variables[d].ncattrs()}
                        except KeyError:
                            md = {k: self._nc_parent.variables[d].getncattr(k) for k in self._group.variables[d].ncattrs()}
                        self._file_details.cfa_file.cfa_dims[d].metadata = md

                # keep the calling parameters in a dictionary, and add the parameters from the client config
                parameters = {'varname': varname, 'datatype': datatype, 'dimensions': dimensions, 'zlib': zlib,
                              'complevel': complevel, 'shuffle': shuffle, 'fletcher32': fletcher32,
                              'contiguous': contiguous, 'chunksizes': chunksizes, 'endian': endian,
                              'least_significant_digit': least_significant_digit,
                              'fill_value': fill_value, 'chunk_cache': chunk_cache,
                              'cache_location': self._sl_config['cache']['location'],
                              'max_object_size_for_memory': obj_size,
                              'write_threads': write_threads,
                              'read_threads': read_threads,
                              'mode': self.mode,
                              'nc_parent': self._nc_parent}

                self._file_details.cfa_file.groups = dict(self._nc_parent.groups)
                # create the s3Variable which is a reimplementation of the netCDF4 variable
                self._cfa_variables[varname] = slVariable(var, self._file_details.cfa_file,
                                                          self._file_details.cfa_file.cfa_vars[varname],
                                                          parameters)
                return self._cfa_variables[varname]

    def close(self):
        return self._group.close()

    def cmptypes(self):
        return self._group.cmptypes()

    def createCompoundType(self):
        raise NotImplementedError

    def createDimension(self,dimname, size):
        ncd = self._group.createDimension(dimname, size)
        if self._file_details.cfa_file is not None:
            # add to the dimensions
            self._file_details.cfa_file.cfa_dims[dimname] = CFADim(dim_name=dimname, dim_len=size)

    def createEnumType(self):
        raise NotImplementedError

    def createGroup(self,groupname):
        _group = self._group.createGroup(groupname)

        return slGroup(groupname, _group, self._file_details, self._nc_parent.dimensions, self._nc_parent, self.mode)

    def createVLType(self):
        pass

    @property
    def data_model(self):
        return self._group.data_model

    def delncattr(self,name):
        return self._group.delncattr(name)

    @property
    def dimensions(self):
        return self._group.dimensions

    @property
    def disk_format(self):
        return self._group.disk_format

    def enumtypes(self):
        raise NotImplementedError

    @property
    def file_format(self):
        return self._group.file_format

    def filepath(self,enconding=None):
        return self._group.filepath(enconding)

    def get_variables_by_attributes(self,**kwargs):
        return self._group.get_variables_by_attributes(**kwargs)

    def getncattr(self,name):
        return self._group.getncattr(name)

    @property
    def groups(self):
        return self._group.groups

    def isopen(self):
        return self._group.isopen()

    def keepweakref(self):
        pass

    @property
    def name(self):
        return self._group.name

    def ncattrs(self):
        return self._group.ncattrs()

    @property
    def parent(self):
        return self._group.parent

    @property
    def path(self):
        return self._group.path

    def renameAttribute(self,oldname,newname):
        return self._group.renameAttribute(oldname,newname)

    def renameDimension(self,oldname,newname):
        return self._group.renameDimension(oldname,newname)

    def renameGroup(self,oldname,newname):
        return  self._group.renameGroup(oldname,newname)

    def renameVariable(self,oldname,newname):
        return  self._group.renameVariable(oldname,newname)

    def set_always_mask(self):
        # TODO
        raise NotImplementedError

    def set_auto_chartostring(self):
        # TODO
        raise NotImplementedError

    def set_auto_mask(self):
        # TODO
        raise NotImplementedError

    def set_auto_maskandscale(self):
        # TODO
        raise NotImplementedError

    def set_auto_scale(self):
        # TODO
        raise NotImplementedError

    def set_fill_off(self):
        # TODO
        raise NotImplementedError

    def set_fill_on(self):
        # TODO
        raise NotImplementedError

    def setncattr(self,name,value):
        return self._group.setncattr(name,value)

    def setncattr_string(self,name,value):
        return self._group.setncattr_string(name, value)

    def setncatts(self,attrdict):
        return self._group.setncatts(attrdict)

    def sync(self):
        # TODO
        raise NotImplementedError

    def vltypes(self):
        # TODO
        raise NotImplementedError

    @property
    def variables(self):
        return self._group.variables


class slVariable(object):
    """
      Reimplement the UniData netCDF4 Variable class and override some key methods so as to enable CFA and S3 functionality
    """

    _private_atts = ["_cfa_var", "_nc_var", "_cfa_file", "_init_params","subfiles_accessed",
                     "slC","_varid", "chartostring","_changed_attrs"]

    def __init__(self, nc_var, cfa_file, cfa_var, init_params = {}):
        """Keep a reference to the nc_file, nc_var and cfa_var"""
        self._nc_var  = nc_var
        self._cfa_file = cfa_file
        self._init_params = init_params
        self._cfa_var = cfa_var
        self.subfiles_accessed = []
        self.slC = slCache()
        self._varid = nc_var._varid
        self.chartostring = nc_var.chartostring
        self._changed_attrs = False

    def _accessed_subfiles(self):
        return self.subfiles_accessed

    def __repr__(self):
        return str(self._nc_var)

    def __array__(self):
        return self[:]

    def __unicode__(self):
        return self._nc_var.__unicode__()

    def close_subfiles(self,file_list):
        for file in file_list:
            file.close()

    def upload_subfiles(self,file_list):
        # upload sub files in bulk, won't do anything if posix files are passed to it
        self.slC.bulk_upload(file_list)

    def check_whether_posix(self,file):
        # for the sub var return check the slcache function doesn't work, because can't supply a read
        # mode. We need another way to check
        # Assume for now that if there is not a matching alias, that the file points to a POSIX path
        # TODO update this with proper check
        if slU._get_alias(file) is not None:
            return 'Alias exists'
        else:
            return True

    def return_subvars(self, var):
        # returns a list of the sub varible objects to update, and the posix files and cache files
        # to enable closing

        # if files not in cache then need to download in bulk from backend BEFORE updating files
        # for all subfiles, check the cache, and create list of all ones not in there
        # the size of the total files also needs to be caculated to make sure it is not larger than the
        # max cache size
        # get the list of subfiles from the master file
        varpartionsdict = self.getncattr('cfa_array')
        subfiles = []
        attr_split = [el.split('"') for el in varpartionsdict.split('file')]
        for slist in attr_split:
            [subfiles.append(i) for i in slist if ".nc" in i]
        svs = []
        sfs = []
        files_for_download = []
        cache_locs = []
        posix_files = []
        all_open_files = []
        for file in subfiles:
            if self.check_whether_posix(file) == 'Alias exists':
                # Build download list
                if not self.slC.DB.check_cache(file):
                    files_for_download.append(file)
                # create list of the paths for each file in the cache
                cache_loc = self.slC.DB.get_cache_loc(file)
                cache_locs.append(cache_loc)
                all_open_files.append(cache_loc)
            # if it is a posix file might as well append
            elif self.check_whether_posix(file):
                posix_files.append(file)
                all_open_files.append(file)

            elif not self.check_whether_posix:
                raise slIOException("Alias doesn't exist, and the lib doesn't "
                                 "think the files path is POSIX for {}".format(file))

        if len(files_for_download)>0:
            self.slC.bulk_download(files_for_download)

        # the check for posix files doesn't work if the files don't already exist, i.e on write!
        # add any posix files
        for file in posix_files:
            sf = netCDF4.Dataset(file,'a')
            sfs.append(sf)
            svs.append(sf.variables[var])

        # now add the downloaded vars
        for file in cache_locs:
            # need to open each subfile
            sf = netCDF4.Dataset(file,'a')
            sfs.append(sf)
            svs.append(sf.variables[var])

        return svs, sfs, all_open_files

    @property
    def name(self):
        return self._nc_var.name
    @name.setter
    def name(self, value):
        raise AttributeError("name cannot be altered")

    @property
    def datatype(self):
        return self._nc_var.datatype

    @property
    def shape(self):
        return self._shape()

    def _shape(self):
        # get the shape from the list of dimensions in the _cfa_var and the
        # size of the dimensions from the _cfa_file
        shp = []
        for cfa_dim in self._cfa_var.cfa_dimensions:
            d = self._cfa_file.cfa_dims[cfa_dim]
            if d.dim_len == -1:         # check for unlimited dimension
                shp.append(d.values.shape[0])
            else:
                shp.append(d.dim_len)
        return shp

    def _size(self):
        return np.prod(self._shape())

    def _dimensions(self):
        # get the dimensions from the _cfa_var
        return self._cfa_var.cfa_dimensions

    def group(self):
        return self._nc_var.group()

    def ncattrs(self):
        return self._nc_var.ncattrs()

    def setncattr(self, name, value):
        self._nc_var.setncattr(name, value)
        if self._cfa_file is not None:
            self._changed_attrs = True

    def setncattr_string(self, name, value):
        self._nc_var.setncattr(name, value)

        if self._cfa_file is not None:
            self._changed_attrs = True

    def setncatts(self, attdict):
        # copy to the cfa_var
        self._cfa_var.metadata = attdict
        # copy to the netCDF var
        self._nc_var.setncatts(attdict)

        if self._cfa_file is not None:
            self._changed_attrs = True

    def getncattr(self, name, encoding='utf-8'):
        return self._nc_var.getncattr(name, encoding)

    def delncattr(self, name):
        self._nc_var.delncattr(name)

        if self._cfa_file is not None:
            self._changed_attrs = True

    def filters(self):
        return self._nc_var.filters()

    def endian(self):
        return self._nc_var.endian()

    def chunking(self):
        return self._nc_var.chunking()

    def get_var_chunk_cache(self):
        return self._nc_var.get_var_chunk_cache()

    def set_var_chunk_cache(self, size=None, nelems=None, preemption=None):
        self._nc_var.set_var_chunk_cache(size, nelems, preemption)

    def __delattr__(self, name):
        self._nc_var.__delattr__(name)

    def __setattr__(self, name, value):
        if name in slVariable._private_atts:
            self.__dict__[name] = value
        elif name == "dimensions":
            raise AttributeError("dimensions cannot be altered")
        elif name == "shape":
            raise AttributeError("shape cannot be altered")
        else:
            self.setncattr(name,value)

    def __getattr__(self, name):
        # check whether it is _nc_var or _cfa_var
        if name in slVariable._private_atts:
            return self.__dict__[name]
        elif name == "dimensions":
            return tuple(self._dimensions())
        elif name == "shape":
            return tuple(self._shape())
        elif name == "size":
            return self._size()
        # create work around for returning the _varid private attr
        elif name == '_varid':
                return self._va
        # if name in _private_atts, it is stored at the python
        # level and not in the netCDF file.
        elif name.startswith('__') and name.endswith('__'):
            # if __dict__ requested, return a dict with netCDF attributes.
            if name == '__dict__':
                names = self._nc_var.ncattrs()
                values = []
                for name in names:
                    values.append(_get_att(self._nc_var.group(), self._nc_var._varid, name))
                return OrderedDict(zip(names, values))
            else:
                raise AttributeError
        elif name in netCDF4._private_atts:

            return self._nc_var.__dict__[name]
        else:
            return self._nc_var.getncattr(name)

    def renameAttribute(self, oldname, newname):
        self._nc_var.renameAttribute(oldname, newname)

        if self._cfa_file is not None:
            self._changed_attrs = True

    def __len__(self):
        return self._nc_var.__len__()

    def assignValue(self, val):
        self._nc_var.assignValue(val)

    def getValue(self):
        return self._nc_var.getValue()

    def set_auto_chartostring(self, chartostring):
        self._nc_var.set_auto_chartostring(chartostring)

    def set_auto_maskandscale(self, maskandscale):
        self._nc_var.set_auto_maskandscale(maskandscale)

    def set_auto_scale(self, scale):
        self._nc_var.set_auto_scale(scale)

    def set_auto_mask(self, mask):
        self._nc_var.set_auto_mask(mask)

    def __reduce__(self):
        return self._nc_var.__reduce__()

    def __getitem__(self, elem):
        """Overload the [] operator for getting values from the netCDF variable"""
        # get the filled slices
        elem_slices = fill_slices(self.shape, elem)
        # get the partitions from the slice - created the subset of partitions
        subset_parts = []
        # determine which partitions overlap with the indices
        for p in self._cfa_var.partitions:
            if partition_overlaps(p, elem_slices):
                subset_parts.append(p)
        # create the target shape from the elem slices and the size (number of elements)
        subset_size = 1
        subset_shape = []
        for s in elem_slices:
            dim_size = (s.stop-s.start+1)/s.step
            subset_shape.append(int(dim_size))
            subset_size *= dim_size

        # calculate the size (in bytes) of the subset of the array
        subset_size *= self._nc_var.dtype.itemsize

        # create the target array
        # this will be a memory mapped array if it is greater than the user_config["max_object_size_for_memory"] or
        # the available memory
        if subset_size > self._init_params['max_object_size_for_memory'] or subset_size > virtual_memory().available:
            # create a memory mapped array in the cache for the output array
            mmap_name = self._init_params['cache_location'] + "/" + os.path.basename(subset_parts[0].subarray.file) + "_{}".format(int(numpy.random.uniform(0,1e8)))
            tgt_arr = numpy.memmap(mmap_name, dtype=self._nc_var.dtype, mode='w+', shape=tuple(subset_shape))
        else:
            # create a normal array with no memory map
            tgt_arr = numpy.zeros(subset_shape, dtype=self._nc_var.dtype)

        # create the interface for reading, pass in the target array
        read_interface = interface()
        read_interface.set_read_params(tgt_arr,
                                       self._init_params['read_threads'])

        # use the interface to read the data in
        read_interface.read(subset_parts, elem_slices)
        return tgt_arr


    def __setitem__(self, elem, data):
        """Overload the [] operator for setting values for the netCDF variable"""
        # get the filled slices
        elem_slices = fill_slices(self.shape, elem)
        # get the partitions from the slice - created the subset of partitions
        subset_parts = deque()
        # determine which partitions overlap with the indices
        for p in self._cfa_var.partitions:
            if partition_overlaps(p, elem_slices):
                subset_parts.append(p)

        # create the interface for writing, pass in the target array
        write_interface = interface()
        # pass in the required parameters for writing
        write_interface.set_write_params(data, self._nc_var, self._cfa_var, self._cfa_file,
                                         self._init_params['write_threads'], self._init_params, self.group())
        pret = write_interface.write(subset_parts, elem_slices)

        self.subfiles_accessed.extend(pret)


