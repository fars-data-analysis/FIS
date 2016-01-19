import helpers as hps

import settings
import setGenerator
import frequentItemSet

THRESHOLD = 5000
baskets = setGenerator.genAttrSet()
c1 = frequentItemSet.countFrequency(baskets)
print "C1 size:",len(c1)
l1 = frequentItemSet.getFrequentItems(c1, THRESHOLD)
print "L1 size:",len(l1)
c2 = frequentItemSet.createNples(baskets, l1)
print "C2 size:",len(c2)
