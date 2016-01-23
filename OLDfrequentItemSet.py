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

def createNaples(itemBaskets,finalists,n):
    candidates = {}
    for x in itemBaskets:
    #    toRemove = set()
    #    for t_check in itertools.combinations(x, n-1):
    #        [toRemove.add(y) for y in t_check if y not in finalists]
    #    x.difference_update(toRemove)
        candidate_parts = [t for t in intertools.combinations(x, n-1) if t in finalists]
        for t_uple in itertools.combinations(x, n):
            if n == 2:
                parts = [t_uple[0], t_uple[1]]
            else:
                parts = [s for s in itertools.combinations(t_uple, n-1)]
            if finalists.issuperset(parts):
                if candidates.has_key(t_uple):
                    candidates[t_uple]+=1
                else:
                    candidates[t_uple]=1
    return candidates
    #itertools.combinations(sentence, len(finalists)):


def createNples(itemBaskets,finalists,n):
    candidates = {}
    for x in itemBaskets:
        toRemove = set()
        for t_uple in itertools.combinations(x, n):
            if finalists.issuperset(t_uple):
                if candidates.has_key(t_uple):
                    candidates[t_uple]+=1
                else:
                    candidates[t_uple]=1
            else:
                [toRemove.add(t) for t in t_uple]
        x.difference_update(toRemove)
    return candidates
    #itertools.combinations(sentence, len(finalists)):
