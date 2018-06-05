"""Initilise the backends, provide functions to interrogate which backends are
available."""
from SemSL.Backends import _slS3Backend, _slFTPBackend

def get_backends():
    """Get a tuple of all the backends that have been added to SemSL"""
    return [_slS3Backend.slS3Backend,
            _slFTPBackend.slFTPBackend]

def get_backend_ids():
    """Get the ids of the backends that have been added to SemSL"""
    return [be.get_id(None) for be in get_backends()]

def get_backend_from_id(id):
    """Get a backend class from the id"""
    index = get_backend_ids().index(id)
    return get_backends()[index]
