import pstats
#from pstats import SortKey
import os
pwd = os.getcwd()
p = pstats.Stats('{}/test/profile.out'.format(pwd))
p.strip_dirs().sort_stats('tottime').print_stats()