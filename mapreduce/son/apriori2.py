import sys
import copy

from string import atoi
from pyspark import SparkContext
import frequentItemSet as fis

def findFrequentItemsets(input, output, numPartitions, s, sc):
    """
    Find frequent item sets using the SON algorithm in two stages.
    First stage: divide document and find frequent itemsets in each partition.
    Second stage: join local itemset candidates, distribute to workers and
    count actual frequency.

    Args:
        arg1 (string): Location of the data file
        arg2 (string): Where to save itemsets
        arg3 (int): Number of partitions to make. Leave empty for default
        arg4 (float): Threshold
        arg5 (SparkContext): Spark Context

    Returns:
        list: List of all the encountered frequent itemsets. There is no
        guarantee that all frequent itemsets were found. But if something is
        in the list, it must be a frequent itemset.
    """

    data = sc.textFile(input, numPartitions)

    numPartitions = data.getNumPartitions()

    count = data.count()

    threshold = s*count

    #split string baskets into lists of items
    baskets = data.map(lambda line: sorted([int(y) for y in line.strip().split(' ')]))

    #key each basket by partition id, to ensure work is done locally
    #break down transactions into single items, with an id referring to the basket
    itemBasketsByWorker = baskets.mapPartitionsWithIndex(getBreakUp())

    #classic word count
    singleCount = itemBasketsByWorker.map(lambda ((partId,basketId),item): ((partId, tuple([item])),1), True)

    #Add up counts and filter with threshold
    freqLocalSingles = singleCount.reduceByKey(lambda v1, v2: v1+v2).filter(lambda c: c[1]>=threshold/numPartitions) #.map(lambda ((partId, item),count): (partId, item))

    #Remove unfrequent items from baskets
    filteredBaskets = itemBasketsByWorker.map(lambda ((p, t),i): ((p,tuple([i])), t)).leftOuterJoin(freqLocalSingles).filter(lambda ((p, i), (b,j)): j!=None).map(lambda ((p,i),(b,j)): ((p,b),i))

    #combine with itself to generate pairs...transform basket

    #count pairs

    #filter with threshold

    #remove unfrequent pairs from baskets
    return filteredBaskets

def getBreakUp():
    class nonlocal:
        count = 0
    def breakUpWithCount(pid, baskets):
        res = []
        for b in baskets:
            for i in b:
                res.append(((pid, nonlocal.count), i))
            nonlocal.count+=1
        return res
    return breakUpWithCount

    """
    localItemSets = baskets.mapPartitions(lambda x: localApriori(x, threshold/numPartitions), True)

    allItemSets = localItemSets.flatMap(lambda n_itemset: [x for x in n_itemset])

    mergedCandidates = allItemSets.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x).map(lambda (x,y): x)

    mergedCandidates = mergedCandidates.collect()

    candidates = sc.broadcast(mergedCandidates)

    counts = basketSets.flatMap(lambda line: [(x,1) for x in candidates.value if line.issuperset(x)])

    finalItemSets = counts.reduceByKey(lambda v1, v2: v1+v2).filter(lambda (i,v): v>=threshold)

    return finalItemSets
    """

def localApriori(baskets, threshold):
    baskets = list(baskets)
    p = []
    candidates = fis.countFrequency(baskets)
    i = 2
    while len(candidates) > 0:
        finalists, simpleFinalists = fis.getFrequentItems(candidates, threshold)
        if len(finalists) > 0:
            p.append(finalists)
        candidates = fis.createTuples2(baskets,finalists,simpleFinalists,i)
        i+=1

    return p
