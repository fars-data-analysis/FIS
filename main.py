import helpers as hps

import settings
import setGenerator
import frequentItemSet001 as fis

THRESHOLD = 51000
THRES_HOLD = 2000

TH_MIN = 500
TH_MAX = 600

"""
baskets = setGenerator.genAttrSet()
c1 = frequentItemSet.countFrequency(baskets)
print "C1 size:",len(c1)
l1 = frequentItemSet.getFrequentItems(c1, THRESHOLD)
print "L1 size:",len(l1)
c2 = frequentItemSet.createNples(baskets, l1)
print "C2 size:",len(c2)

itemBaskets = setGenerator.genAttrSet()

candidates = fis.countFrequency(itemBaskets)

finalists, simpleFinalistsset = fis.getFrequentItems(candidates,THRESHOLD)
"""
a = [[1, 2, 3, 4, 5],
[1, 2, 4, 5, 6],
[1, 2, 5, 7, 6],
[1, 2, 4, 7, 6],
[1, 2, 5, 7, 6]]

baskets = [set(i) for i in a]

baskets = setGenerator.genAttrSet()

print "THRES_HOLD = ",THRES_HOLD

print " Baskets ready! :) "

#candidates1 = fis.countFrequency(baskets)
#finalists1, simplefinalists1 = fis.getFrequentItems1(candidates1,TH_MIN,TH_MAX)

candidates1 = fis.countFrequency(baskets)

"""
candidates1 = fis.countFrequency(baskets)
finalists1, simplefinalists1 = fis.getFrequentItems(candidates1,THRES_HOLD)
print "round 1 FINITO"
print "C1 size:",len(candidates1)
print "L1 size:",len(finalists1)


candidates2 = fis.createTuples2(baskets,finalists1,simplefinalists1,2)
finalists2, simplefinalists2 = fis.getFrequentItems(candidates2,THRES_HOLD)
print "round 2 FINITO"
print "C1 size:",len(candidates2)
print "L1 size:",len(finalists2)

candidates3 = fis.createTuples2(baskets,finalists2,simplefinalists2,3)
finalists3, simplefinalists3 = fis.getFrequentItems(candidates3,THRES_HOLD)
print "round 3 FINITO"
print "C1 size:",len(candidates3)
print "L1 size:",len(finalists3)

candidates4 = fis.createTuples2(baskets,finalists3,simplefinalists3,4)
finalists4, simplefinalists4 = fis.getFrequentItems(candidates4,THRES_HOLD)
print "round 4 FINITO "
print "C1 size:",len(candidates4)
print "L1 size:",len(finalists4)

candidates5 = fis.createTuples2(baskets,finalists4,simplefinalists4,5)
finalists5, simplefinalists5 = fis.getFrequentItems(candidates5,THRES_HOLD)
print "round 5 FINITO"
print "C1 size:",len(candidates5)
print "L1 size:",len(finalists5)

candidates6 = fis.createTuples2(baskets,finalists5,simplefinalists5,6)
finalists6, simplefinalists6 = fis.getFrequentItems(candidates6,THRES_HOLD)
print "round 6 FINITO"
print "C1 size:",len(candidates6)
print "L1 size:",len(finalists6)

list(finalists6).sort()
"""


"""
candidates1 = fis.countFrequency(baskets)
finalists1, simplefinalists1 = fis.getFrequentItems(candidates1,THRES_HOLD)
print "round 1 FINITO"

candidates2 = fis.createTuples2(baskets,finalists1,simplefinalists1,2)
finalists2, simplefinalists2 = fis.getFrequentItems(candidates2,THRES_HOLD)
print "round 2 FINITO"

candidates3 = fis.createTuples2(baskets,finalists2,simplefinalists2,3)
finalists3, simplefinalists3 = fis.getFrequentItems(candidates3,THRES_HOLD)
print "round 3 FINITO"

candidates4 = fis.createTuples2(baskets,finalists3,simplefinalists3,4)
finalists4, simplefinalists4 = fis.getFrequentItems(candidates4,THRES_HOLD)
print " [END] round 4 FINITO "
"""
