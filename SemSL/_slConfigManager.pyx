"""
Configuration management for Sem-SL.  Configuration is stored for each user in
a JSON file in their home directory: ~/.sem-sl.json
"""

__copyright__ = "(C) 2012 Science and Technology Facilities Council"
__license__ = "BSD - see LICENSE file in top-level directory"

import os
import json
from _slExceptions import slIOException, slAPIException

def convert_file_size_string(value):
    """Convert a string containing a file size and suffix to an integer number
    of bytes.
    value <string> : string containing integer number and an optional suffix
    """
    # list of file format sizes
    file_format_sizes = ("kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    # dictionary mapping to multiplier
    file_format_scale = {"B" : 1,
                         "kB" : 1e3,
                         "MB" : 1e6,
                         "GB" : 1e9,
                         "TB" : 1e12,
                         "EB" : 1e15,
                         "ZB" : 1e18,
                         "YB" : 1e21}
    if value.endswith(file_format_sizes):
        suffix = value[-2:]
        size = int(value[:-2])
    elif value[-1] == "B":
        suffix = "B"
        size = int(value[:-1])
    else:
        suffix = "B"
        size = int(value)
    # multiply by scalar
    size *= file_format_scale[suffix]
    return size


def interpret_config_file(node, keys_to_convert):
    """Recursively search the dictionary for keys to convert, and convert them
    using the convert_file_size_string function above."""
    # First time entry node == dictionary
    for key, item in node.items():
        if type(item) is dict:
            interpret_config_file(item, keys_to_convert)
        elif key in keys_to_convert:
            # reassign to the dictionary
            node[key] = convert_file_size_string(item)


class slConfig(object):
    """Class to read in config file, interpret it and make the information
    available.
    """

    def __init__(self):
        """Initialise SemSL for this user by reading the config file from their
        home directory.  Config file is called ~/.sem-sl.json"""
        # First read the JSON config file from the user home directory
        # get user home directory
        user_home = os.environ["HOME"]

        # create the path
        sl_config_path = os.path.join(user_home, ".sem-sl.json")

        # open the file
        try:
            fp = open(sl_config_path)
            # deserialize from the JSON
            self._sl_user_config = json.load(fp)
            # add the filename to the config so we can refer to it in error
            # messages
            self._sl_user_config["filename"] = sl_config_path
            keys_to_convert = ["object_size",
                               "cache_size",
                               "object_size_for_memory",
                               "default_object_size"]
            # interpret the config file, converting the above keys
            interpret_config_file(self._sl_user_config, keys_to_convert)
            # close the config file
            fp.close()
        except IOError:
            raise slIOException("User config file does not exist with path: " +
                                sl_config_path)

    def __getitem__(self, name):
        """Get a value from the s3 config"""
        return self._sl_user_config[name]

    @property
    def items(self):
        """Return the items in the dictionary / config definition"""
        return self._sl_user_config.items()

    @items.setter
    def items(self, value):
        raise AttributeError("items cannot be altered")
