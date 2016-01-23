import setGenerator
import itertools


# input: Array of sets, every set(integers) is a basket
# returns Array: storing values of coded attributes (pos=attribute code, value=number of occurrences)
def countFrequency(itemBaskets):
    candidates = {}
    for basket in itemBaskets:
        for element in basket:
            if candidates.has_key(tuple([element])):
                candidates[tuple([element])][1]+=1
            else:
                candidates[tuple([element])]=[set([element]),1]
    return candidates

# input:
#    Array: storing values of coded attributes (pos=attribute code, value=number of occurrences,
#        s: threshold integer value (we select values that appears at least many times as s in baskets)
#  returns:
#       finalists: Set of n-ples that are more frequent than the threshold s
#       simpleFinalistsset: set of frequent singletons containd in finalists (usefull to make n-ples)

def getFrequentItems(candidates,s):
    finalists = set()
    simpleFinalistsset = set()
    for item,freq in candidates.iteritems():
        if freq[1] > s:
            finalists.add(item)
            for i in item:
                simpleFinalistsset.add(i)

    return finalists,simpleFinalistsset


# input:
#    Array: storing values of coded attributes (pos=attribute code, value=number of occurrences,
#        s: lower threshold integer value (we select values that appears many times as s in baskets)
#       ss: upper threshold integer value (we try to discard values thate are too frequent, we belive the do not provide usefull information )
#  returns:
#       finalists: Set of n-ples that are more frequent than the threshold s and less than ss
#       simpleFinalistsset: set of frequent singletons containd in finalists (usefull to make n-ples) redundant

def getFrequentItems1(candidates,s,ss):
    finalists = set()
    simpleFinalistsset = set()
    for item,freq in candidates.iteritems():
        if (freq[1] >= s and freq[1] < ss):
            finalists.add(item)
            for i in item:
                simpleFinalistsset.add(i)

    return finalists,simpleFinalistsset

# input:
#   itemBaskets: array of sets
#     finalists: finalists the set of n-ples (considering the n parameter the actual value should be n-1)
#     simpleFinalistsset: set of frequent singletons containd in finalists (usefull to make n-ples) redundant
#  returns:
#     dictionary: the key of the dictionary is the tuple, the value is an array whit two elements
#               [0]: is a set with elements of the n-ple
#               [1]: the accurences of this n-ple in all baskets

def createTuples2(itemBaskets,finalists,simpleFinalistsset,n):
    candidates = {}                                                # this will be the returned dictionary
    for t_uple in itertools.combinations(simpleFinalistsset, n):   # we generate n-ples
        for element in itertools.combinations(t_uple,n-1):         # for every n-ples we generate all (n-1)ples
            condition = True
            if not finalists.issuperset([element]):                # if element is not in the frequent set we discard them
                break                                              # shortcut to exit the cycle
            else:
                continue
        if condition:                                              # if all subsets are frequent (condition=True) we store the n-ple
            candidates[t_uple] = [set(t_uple),0]                   # as a candidate

    for basket in itemBaskets:                                     # this code counts the occurrences of candidate n-ples in the baskets
        for candidate in candidates.values():
            if candidate[0].issubset(basket):
                candidate[1]+=1
    return candidates
    
