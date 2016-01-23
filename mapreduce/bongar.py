import sys

from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

def findFrequentItemsets(input, p, n_p, s, r):
    """Finds frequent itemsets contained in a given datafile.

    This function takes n_p samples of size p and runs the bongar algorithm
    on each sample in a distributed fashion using Apache Spark.

    Frequent itemsets will be extracted from each sample using the FP-Growth
    technique to avoid generating candidates. Since we work with random
    samples, we propose a relaxation factor r which decreases the threshold
    for determining if an itemset is frequent or not. In this way false
    negatives are avoided, at the cost of increasing the number of false
    positives. However, this is not a problem, since at a later stage false
    positives will be eliminated. We also propose the use of Negative Borders
    in each sample to further decrease the number of false negatives.

    The candidate itemsets generated from each sample are merged and in another
    stage tested against the whole data. Each worker will receive a chunk of
    the data and the whole itemset collection. They then count the frequency
    of each itemset in this piece of data. The total frequency is then
    calculated and compared against the threshold s to determine the frequent
    itemsets.

    Args:
        arg1 (string): Location of the data file
        arg2 (float): Size of each sample. Expressed as a probability
        arg3 (int): Number of samples to make
        arg4 (int): Threshold
        arg5 (float): Relaxation factor

    Returns:
        list: List of all the encountered frequent itemsets. There is no
        guarantee that all frequent itemsets were found. But if something is
        in the list, it must be a frequent itemset.
    """

    #Read input file
    data = sc.textFile(input)

    #Generate random samples
    samples = []
    for i in range(0, n_p):
        samples += sc.sample(True, p)

    #Calculate itemsets and negative borders
    candidateResults = []
    for sample in Samples:
        model = FPGrowth.train(transactions, minSupport=s*r, numPartitions=10)
        candidateResults += model.freqItemsets()
        #CandidateResults += sample.mapPartitions(findCandidates, True).

    #Merge results
    allCandidates = candidateResults.pop()
    for c in candidateResults:
        allCandidates.union(c)
    allCandidates = allCandidates.distinct().collect()

    #Broadcast candidate itemsets
    finalCandidates = []
    for freqItem in allCandidates:
        finalCandidates += freqItem.items

    finalCandidates = sc.broadcast(finalCandidates)

    #Perform actual count
    def countFrequency(basket):
        count = {}
        basketSet = set(basket)
        for c in finalCandidates:
            if basketSet.superset(c.item):
                if c.item in count:
                    count[c.item] += 1
                else:
                    count[c.item] = 1
        return [(x,count[x]) for x in count]

    mergedItemsets = data.flatMap(countFrequency).reduceByKey(lambda v1, v2: v1+v2)

    #Collect results and filter with threshold
    result = mergedItemsets.filter(lambda (x,y): y>s).collect()

    return result


if __name__=="__main__":

    if len(sys.argv) != 4:
        print("Usage: bongar <input file> <sample size> <num samples> <threshold> <relaxation factor>", file=sys.stderr)
        exit(-1)

    data = sys.argv[1]
    p = sys.arg[2] #Expressed as 0<p<=1, the probability that a given row will be in the sample
    n_p = sys.arg[3] #Number of samples to make
    s = sys.arg[4] #Threshold for the final itemsets
    r = sys.arg[5] #Factor to dicrease the threshold for the samples

    #Start Spark
    sc = SparkContext(appName="FreqItemsetSON")

    findFrequentItemsets(data, p, n_p, s, r)

    #End Spark
    sc.stop()
