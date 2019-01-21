__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

from SemSL._slConfigManager import slConfig
from SemSL import Backends
from SemSL._slExceptions import slConfigFileException

sl_config = slConfig()

def _get_alias(fid):

    # get all host keys
    hosts = sl_config['hosts']
    host_keys = hosts.keys()
    # Iterate through config to get all aliases
    aliases = []
    for host_name in iter(host_keys):
        aliases.append(sl_config['hosts'][host_name]['alias'])

    for alias in aliases:

        if alias in fid:
            return alias
    # return None if alias isn't found in list
    return None

def _get_hostname(fid):

    alias = _get_alias(fid)
    host_name = None
    try:
        hosts = sl_config["hosts"]
        for h in hosts:
            if alias in hosts[h]['alias']:
                host_name = h
    except Exception as e:
        raise slConfigFileException("Error in config file {} {}".format(
            sl_config["filename"],
            e))
    return host_name

def _get_bucket(fid):
    """
    Return the bucketname
    :param fid:
    :return:
    """
    fname = fid[:]
    alias = _get_alias(fid)
    try:
        bucket = fname.replace(alias,'')
    except TypeError:
        bucket = fname[:]
    #assert bucket.split('/')[0] == ''
    return bucket.split('/')[1]

def _get_key(fid):
    """
    Returns the key from the file id.
    :param fid:
    :return:
    """
    alias = _get_alias(fid)
    if not alias == None: # if no alias, just return the file name
        bucket = _get_bucket(fid)
        key = fid[:]
        key = key.replace(alias,'')
        key = key.replace(bucket,'')
        while key[0] == '/':
            key = key[1:]
        return key
    else:
        return fid

def _get_backend(fid):
    """ Get the correct backend object inorder to interact with the backend

    :param fid:
    :return:
    """

    host_name = _get_hostname(fid)

    host_config = sl_config["hosts"][host_name]
    backend_name = host_config['backend']
    backend = Backends.get_backend_from_id(backend_name)

    return backend()