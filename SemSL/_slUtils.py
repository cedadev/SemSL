
from SemSL._slConfigManager import slConfig

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
        else: # return None if alias isn't found in list
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
        raise ValueError("Error in config file {} {}".format(
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
    bucket = fname.replace(alias,'')
    assert bucket.split('/')[0] == ''
    return bucket.split('/')[1]