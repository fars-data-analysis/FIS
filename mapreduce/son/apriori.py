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
    baskets = data.map(lambda line: sorted([int(y) for y in line.strip().split(' ')])).persist()

    localItemSets = baskets.map(set).mapPartitions(lambda x: localApriori(x, threshold/numPartitions), True)

    allItemSets = localItemSets.flatMap(lambda n_itemset: [x for x in n_itemset])

    freqItemSets = allItemSets.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x).map(lambda (x,y): x)

    return freqItemSets

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
