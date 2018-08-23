import random
import numpy as np

for i in np.arange(10000000):
    key = "%010d" % random.randint(0,1000000)
    value = "value_for_%s" % key
    print ("%s,%s" %  (key,value))