import setGenerator
import itertools


# input: Array of sets, every set(integers) is a basket
# returns Array: storing values of coded attributes (pos=attribute code, value=number of occurrences)
def countFrequency(itemBaskets):
    candidates = {}
    for x in itemBaskets:
        for y in x:
            if candidates.has_key(y):
                candidates[y]+=1
            else:
                candidates[y]=1
    return candidates
"""
    freq = [0]*len(itemBaskets)
    for x in itemBaskets:
        for y in x:
            freq[y]+=1
    return freq
"""

# input: Array: storing values of coded attributes (pos=attribute code, value=number of occurrences, s: threshold integer value
# returns: set of frequent items that are greater than threshold
def getFrequentItems(freqDict,s):
    finalists = set()
    for item,freq in freqDict.iteritems():
        if freq > s:
            finalists.add(item)
    return finalists

"""
    finalists = set()
    for i in range(0, len(freqArr)):
        if freqArr[i] > s:
            finalists.add(i)
    return finalists
"""

def createNples(itemBaskets,finalists):
    candidates = {}
    for x in itemBaskets:
        for t_uple in itertools.combinations(x, 2):
            if finalists.issuperset(t_uple):
                if candidates.has_key(t_uple):
                    candidates[t_uple]+=1
                else:
                    candidates[t_uple]=1
    return candidates
    #itertools.combinations(sentence, len(finalists)):
