"""
   Classes containing the structure of CFA-netCDF files (master array) and the CF-netcdf subarray files.
   See:
     http://www.met.reading.ac.uk/~david/cfa/0.4/index.html
   for the specification of the CFA conventions.

   Only a subset of the CFA-netCDF specification is implemented - just what we use to fragment the files
   to store as multiple objects on the object storage.

   The classes here are organised to reflect the implied hierarchy in the CFA conventions :
   (NC = netCDF)

   +-------------------------------+                +-------------------------------+
   | CFAFile                       |        +------>| CFADim                        |
   +-------------------------------+        |       +-------------------------------+
   | cfa_dims        {CFADim}      |--------+       | dim_name         string       |
   | format          string        |                | metadata         {}           |
   | cfa_metadata    {}            |                | dim_len          [int]        |
   | cfa_vars        {CFAVariable} |--------+       | values           [float]      |
   +-------------------------------+        |       +-------------------------------+
                                            |
                                            |       +-------------------------------+
                                            +------>| CFAVariable                   |
                                                    +-------------------------------+
                                                    | var_name       string         |
                                                    | metadata       {}             |
                                                    | cf_role        string         |
                                                    | pmdimensions   [string]       |
                                                    | pmshape        [int]          |
                                                    | base           string         |
                                            +-------| partitions     [CFAPartition] |
                                            |       +-------------------------------+
   +-------------------------------+        |
   | CFAPartition                  |<-------+
   +-------------------------------+
   | index           [int]         |
   | location        [int]         |
   | subarray        CFASubArray   |--------+
   +-------------------------------+        |
                                            |       +-------------------------------+
                                            +------>| CFASubarray                   |
                                                    +-------------------------------+
                                                    | ncvar          string         |
                                                    | file           string         |
                                                    | format         string         |
                                                    | shape          [int]          |
                                                    +-------------------------------+
"""

__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import numpy as np
cimport numpy as np
import json


class CFAException(BaseException):
    pass


cdef class CFAFile:
    """
       Class containing details of a CFAFile (master array)
    """

    cdef public dict cfa_dims
    cdef public dict cfa_metadata
    cdef public dict cfa_vars
    cdef public basestring format

    def __init__(self, cfa_dims = {},
                 cfa_metadata = {}, cfa_vars = {},
                 format="NETCDF4"):
        """Initialise the CFAFile class"""
        self.cfa_dims = dict(cfa_dims)
        self.cfa_metadata = dict(cfa_metadata)
        self.cfa_vars = dict(cfa_vars)
        self.format = format


    cpdef parse(self, nc_dataset):
        """Parse a netCDF dataset to create the CFA class structures"""
        # check this is a CFA file
        if not "Conventions" in nc_dataset.ncattrs():
            raise CFAException("Not a CFA file.")
        if not "CFA" in nc_dataset.getncattr("Conventions"):
            raise CFAException("Not a CFA file.")

        # next parse the dimensions
        for d in nc_dataset.dimensions:
            # get the dimension's associated variable
            dim_var = nc_dataset.variables[d]
            # get the metadata
            md = {k: dim_var.getncattr(k) for k in dim_var.ncattrs()}
            # create the dimension and append to list of cfa_dims
            self.cfa_dims[d] = CFADim(dim_name=d, dim_len=dim_var.shape[0], metadata = md)

        # next get the variables
        self.cfa_vars = {}
        for v in nc_dataset.variables:
            # check that this variable has a cf_role
            if "cf_role" in nc_dataset.variables[v].ncattrs():
                # create and append the CFAVariable
                cfa_var = CFAVariable()
                cfa_var.parse(nc_dataset.variables[v])
                self.cfa_vars[v] = cfa_var


cdef class CFADim:
    """
       Class containing details of a dimension in a CFAFile
    """

    cdef public basestring dim_name
    cdef public int dim_len
    cdef public np.ndarray values
    cdef public dict metadata

    def __init__(self, dim_name = None, dim_len = None, metadata = {}):
        """Initialise the CFADim object"""
        self.dim_name = dim_name
        if dim_len is None:
            self.dim_len = -1
        else:
            self.dim_len = dim_len
        self.values = np.array([], dtype='f')
        self.metadata = dict(metadata)


    cpdef dict(self):
        """Return a dictionary representation of the CFADim"""
        return {"dim_name" : self.dim_name,
                "dim_len"  : self.dim_len,
                "values"   : self.values,
                "metadata" : self.metadata}


    cpdef setValues(self, values = None, dtype = 'f'):
        """Set the values of the dimension"""
        if values != None:
            self.values = np.array(values, dtype=dtype)


cdef class CFAVariable:
    """
       Class containing details of the variables in a CFAFile
    """

    cdef public basestring var_name
    cdef public dict metadata
    cdef public basestring cf_role
    cdef public list cfa_dimensions
    cdef public list pmdimensions
    cdef public np.ndarray pmshape
    cdef public basestring base
    cdef public list partitions

    def __init__(self, var_name = "",
                 cf_role = "cfa_variable", cfa_dimensions = [],
                 pmdimensions = [], pmshape = [],
                 base = "", partitions = []):
        """Initialise the CFAVariable object"""
        self.var_name = var_name
        self.cf_role = cf_role
        # no point in creating empty data
        if cfa_dimensions != []:
            self.cfa_dimensions = list(cfa_dimensions)
        if pmdimensions != []:
            self.pmdimensions = list(pmdimensions)
        if pmshape != []:
            self.pmshape = np.array(pmshape, dtype='i')
        self.base = base
        self.partitions = list(partitions)


    cpdef parse(self, nc_var):
        """Parse a netCDF variable that contains CFA metadata"""
        self.var_name = nc_var.name

        # check that it is a CFAVariable - i.e. the metadata is correctly defined
        nc_var_atts = nc_var.ncattrs()
        if not "cf_role" in nc_var_atts:
            raise CFAException("cf_role not defined in %s metadata" % nc_var.name)
        if not ("cfa_dimensions" in nc_var_atts or "cf_dimensions" in nc_var_atts):
            raise CFAException("cfa_dimensions or cf_dimensions not defined in %s metadata" % nc_var.name)
        if not "cfa_array" in nc_var_atts:
            raise CFAException("cfa_array not defined in %s metadata" % nc_var.name)

        for k in nc_var_atts:
            # cf_role
            if k == "cf_role":
                self.cf_role = nc_var.getncattr(k)
            # cfa_dimensions
            elif k == "cfa_dimensions" or k == "cf_dimensions":
                self.cfa_dimensions = nc_var.getncattr(k).split()
            # cfa_array
            elif k == "cfa_array":
                # cfa is a chunk of JSON so load it
                cfa_json = json.loads(nc_var.getncattr(k))
                # check that the partitions are defined in the JSON
                if not "Partitions" in cfa_json:
                    raise CFAException("Partitions not defined in %s:cfa_array metadata" % nc_var.name)
                # load all the data for this class - if it exists
                if "base" in cfa_json:
                    self.base = cfa_json["base"]
                if "pmshape" in cfa_json:
                    self.pmshape = np.array(cfa_json["pmshape"], dtype='i')
                if "pmdimensions" in cfa_json:
                    self.pmdimensions = cfa_json["pmdimensions"]
                for p in cfa_json["Partitions"]:
                    cfa_part = CFAPartition()
                    cfa_part.parse(p)
                    self.partitions.append(cfa_part)


    cpdef dict(self):
        """Return the a dictionary representation of the CFAVariable so it can be
           added to the metadata for the variable later."""
        cfa_array_dict = {}
        if self.base != "":
            cfa_array_dict["base"] = self.base
        if self.pmshape != []:
            cfa_array_dict["pmshape"] = self.pmshape.tolist()
        if self.pmdimensions != []:
            cfa_array_dict["pmdimensions"] = self.pmdimensions
        cfa_array_dict["Partitions"] = [p.dict() for p in self.partitions]
        return {"cf_role"        : self.cf_role,
                "cf_dimensions"  : " ".join(self.cfa_dimensions),
                "cfa_array"      : cfa_array_dict}


cdef class CFAPartition:
    """
       Class containing details of the partitions in a CFAVariable
    """

    cdef public np.ndarray index
    cdef public np.ndarray location
    cdef public CFASubarray subarray

    def __init__(self, index = [], location = [], subarray = None):
        """Initialise the CFAPartition object"""
        self.index = np.array(index, dtype='i')
        self.location = np.array(location, dtype='i')
        self.subarray = subarray


    cpdef parse(self, part):
        """Parse a partition definition from the metadata."""
        # Check that the "subarray" item exists in the metadata
        if not "subarray" in part:
            raise CFAException("subarray not defined in cfa_array:Partition metadata")
        # Check index and subarray in JSON / metadata
        if "index" in part:
            self.index = np.array(part["index"], 'i')
        if "location" in part:
            self.location = np.array(part["location"], 'i')
        cfa_subarray = CFASubarray()
        cfa_subarray.parse(part["subarray"])
        self.subarray = cfa_subarray


    cpdef dict(self):
        """Return the partition represented as a dictionary so it can be
           converted to a JSON string later."""
        return {"index"    : self.index.tolist(),
                "location" : self.location.tolist(),
                "subarray" : self.subarray.dict()}


cdef class CFASubarray:
    """
       Class containing details of a subarray in a CFAPartition
    """

    cdef public basestring ncvar
    cdef public basestring file
    cdef public basestring format
    cdef public np.ndarray shape

    def __init__(self, ncvar = "", file = "", format = "", shape = []):
        """Initialise the CFASubarray object"""
        self.ncvar = ncvar
        self.file = file
        self.format = format
        self.shape = np.array(shape, dtype='i')


    cpdef parse(self, subarray):
        """Parse the cfa_subarray member of the Partition metadata"""
        # the only item which has to be present is shape
        if not "shape" in subarray:
            raise CFAException("shape not defined in Partition:subarray metadata")
        if "ncvar" in subarray:
            self.ncvar = subarray["ncvar"]
        if "file" in subarray:
            self.file = subarray["file"]
        if "format" in subarray:
            self.format = subarray["format"]
        self.shape = np.array(subarray["shape"], 'i')


    def dict(self):
        """Return a string containing the JSON representation of the CFASubarray"""
        return {"ncvar"  : self.ncvar,
                "file"   : self.file,
                "format" : self.format,
                "shape"  : self.shape.tolist()}


cdef class CFASlice:
    """
       Class containing a read / write slice and conversion to Python slice
    """
    cdef public int start
    cdef public int stop
    cdef public int step

    def __init__(self, start=0, stop=-1, step=1):
        self.start = start
        self.stop = stop
        self.step = step

    def to_pyslice(self):
        return slice(self.start, self.stop, self.step)

    def __str__(self):
        return "[{}, {}, {}]".format(self.start, self.stop, self.step)

    def __repr__(self):
        return "[{}, {}, {}]".format(self.start, self.stop, self.step)
