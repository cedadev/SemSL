""" test untility functions of SemSL"""

import SemSL._slArrayInterp as slArr

class SLFile(object):

    def __init__(self,fname,mode):
        # parse master file


        # Assert that sub files are netcdf!


        Dataset.__init__(self,fname,mode)

    def getVariable(self,varname):
        return self.variables[varname]




    def close(self):
        # push all changes if required
        raise NotImplementedError

    def get_varaible(self,varname):
        return _slVariable(varname)

    def get_attr(self,attrname):
        raise NotImplementedError

    def get_subfiles(self):
        raise NotImplementedError

class _slVariable(object):
    """ Variable interface to semsl.

        The class manages the variable attr, and slicing to the subfiles.
    """

    def __init__(self,varname):
        pass

    def get_attr(self,attrname):
        raise NotImplementedError

    def get_slice(self,slice):
        raise NotImplementedError

    def _calc_subfiles(self):
        raise NotImplementedError