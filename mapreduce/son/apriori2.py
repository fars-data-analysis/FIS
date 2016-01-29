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

    threshold = int(s)

    res = {}

    #split string baskets into lists of items
    #Result: ([items])
    res[0] = baskets = data.map(lambda line: sorted([int(y) for y in line.strip().split(' ')]), True)

    #key each basket by partition id, to ensure work is done locally
    #break down transactions into single items, with an id referring to the basket
    #Result: ((partId, basketId), item)
    res[1] = expandedBaskets = baskets.mapPartitionsWithIndex(getBreakUp()).persist()

    #classic word count
    #Result: ((partId, tuple([item])), count)
    res[2] = candidates_1 = expandedBaskets.map(lambda ((partId,basketId),item): ((partId, tuple([item])),1)).reduceByKey(lambda v1, v2: v1+v2)

    #Add up counts and filter with threshold
    #Result: ((partId, tuple([items])), count)
    res[3] = frequent_1 = candidates_1.filter(lambda c: c[1]>=threshold/numPartitions)

    #Remove unfrequent items from baskets
    #Result: ((partId, basketId), tuple([items])
    res[4] = expandedBaskets = expandedBaskets.map(lambda ((p, b),i): ((p,tuple([i])), b)).join(frequent_1).map(lambda ((p,i),(b,count)): ((p,b),i))

    #combine with itself to generate pairs. Transforms the basket into tuples that ocur inside it
    #Result: ((partId, basketId), tuple([items]))
    res[5] = expandedBaskets = expandedBaskets.join(expandedBaskets).filter(lambda ((p,b),(i1,i2)): i1[0]<i2[0]).mapValues(lambda (i1,i2): i1+i2)

    #classic word count
    #Result: ((partId, tuple([items])), count)
    res[6] = candidates_n = expandedBaskets.map(lambda ((p, b), items): ((p, items), 1)).reduceByKey(lambda v1, v2: v1+v2)

    #Determine frequent pairs by counting all pairs and filtering with threshold
    #Result: ((partId, tuple([items])), count)
    res[7] = frequent_n = candidates_n.filter(lambda c: c[1]>=threshold/numPartitions)

    #remove unfrequent pairs from baskets
    #Result: ((partId, basketId), tuple([items]))
    res[8] = expandedBaskets = expandedBaskets.map(lambda ((p,b), i): ((p,i),b)).join(frequent_n).map(lambda ((p,i),(b,count)): ((p,b),i))

    #combine with itself to generate triplets
    #TODO: cogroup instead
    curr_n = 3
    res[9] = expandedBaskets = expandedBaskets.join(expandedBaskets).filter(lambda ((p,b),(t1,t2)): t1[0]<t2[0] and t1[1]<t2[1]).mapValues(lambda (i1, i2): tuple(sorted(set(i1+i2)))).filter(lambda (k,v): len(set(v)) == curr_n)

    #classic word count
    res[10] = candidates_n = expandedBaskets.map(lambda ((p, b), items): ((p, items), 1)).reduceByKey(lambda v1, v2: v1+v2)

    res[11] = frequent_n = candidates_n.filter(lambda c: c[1]>=threshold/numPartitions)

    return res, threshold/numPartitions

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
