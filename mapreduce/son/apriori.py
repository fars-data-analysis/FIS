import sys
import copy

from string import atoi
from pyspark import SparkContext, SparkConf
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

    basketSets = baskets.map(set).persist()

    localItemSets = baskets.mapPartitions(lambda x: localApriori(x, threshold/numPartitions), True)

    allItemSets = localItemSets.flatMap(lambda n_itemset: [x for x in n_itemset])

    mergedCandidates = allItemSets.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x).map(lambda (x,y): x)

    mergedCandidates = mergedCandidates.collect()

    candidates = sc.broadcast(mergedCandidates)

    counts = basketSets.flatMap(lambda line: [(x,1) for x in candidates.value if line.issuperset(x)])

    finalItemSets = counts.reduceByKey(lambda v1, v2: v1+v2).filter(lambda (i,v): v>=threshold)

    finalItemSets.saveAsTextFile(output)

    return finalItemSets

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


if __name__ == "__main__":

    APP_NAME = "SON-apriori"

    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")

    sc  = SparkContext(conf=conf)

    f_input = sys.argv[1]
    f_output = sys.argv[2]
    threshold = float(sys.argv[3])

    if len(sys.argv) > 3:
        numPartitions = int(sys.argv[4])
    else:
        numPartitions = None
        
    findFrequentItemsets(f_input, f_output, numPartitions, threshold, sc)
