import pstats
#from pstats import SortKey
import os
pwd = os.getcwd()
p = pstats.Stats('{}/nc4_tests/test_read.out'.format(pwd))
p.strip_dirs().sort_stats('tottime').print_stats()
