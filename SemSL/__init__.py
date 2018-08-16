# init for SemSL - install Cython and numpy includes for Cython

import pyximport
import numpy as np
pyximport.install(setup_args={'include_dirs': np.get_include()})

from ._slConfigManager import *
