# init for SemSL - install Cython and numpy includes for Cython

import pyximport
import numpy as np
import os
os.environ["C_INCLUDE_PATH"] = np.get_include()
pyximport.install(
    setup_args={'include_dirs': np.get_include()},
    language_level=3,
    )

from ._slConfigManager import *
