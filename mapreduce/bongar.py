import sys

from string import atoi
from pyspark import SparkContext, SparkConf
import mapreduce.fpGrowth.fpGrowth as FPGrowth

def findFrequentItemsets(input, output, p, n_p, s, r, n=None):
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
        arg2  (string): Location to save results
        arg3 (float): Size of each sample. Expressed as a probability
        arg4 (int): Number of samples to make
        arg5 (float): Threshold
        arg6 (float): Relaxation factor
        arg7 (int): Max length of itemset

    Returns:
        list: List of all the encountered frequent itemsets. There is no
        guarantee that all frequent itemsets were found. But if something is
        in the list, it must be a frequent itemset.
    """

    #Read input file
    data = sc.textFile(input).map(lambda x: [int(y) for y in x.strip().split(' ')]).persist()

    size = data.count()

    #Generate random samples
    samples = []
    for i in range(0, n_p):
        samples.append(data.sample(False, p))

    #Calculate itemsets
    candidateResults = []
    for sample in samples:
        candidateResults.append(FPGrowth.runFPGrowth(sample, size*s*r*p).map(lambda (itemset, count): tuple(sorted(itemset) ) ))
        #model = FPGrowth.train(sample, minSupport=s*r)
        #candidateResults.append(model.freqItemsets().map(lambda x: tuple(sorted(x.items))))

    #Merge results from all workers
    mergedResults = candidateResults.pop()
    for c in candidateResults:
        mergedResults = mergedResults.union(c)

    #Broadcast candidate itemsets
    if n!= None:
        finalCandidates = mergedResults.filter(lambda x: len(x)<=n).distinct()
    else:
        finalCandidates = mergedResults.distinct()
    #finalCandidates.map(lambda x: ", ".join(x)).saveAsTextFile(output+"/candidates")

    candidatesBroadcast = sc.broadcast(finalCandidates.collect())

    #Count occurrence of candidates to discard false positives
    def countFrequency(basket):
        emit = []
        for c in candidatesBroadcast.value:
            if basket.issuperset(c):
                emit.append(c)
        return [(x,1) for x in emit]

    mergedItemsets = data.map(lambda x: frozenset(x)).flatMap(countFrequency).reduceByKey(lambda v1, v2: v1+v2).filter(lambda x: x[1]>=size*s)

    #Collect results and filter with threshold
    mergedItemsets.saveAsTextFile(output)

    return mergedItemsets



if __name__=="__main__":

    APP_NAME = "BONGAR"

    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")

    sc  = SparkContext(conf=conf)
    if len(sys.argv) != 7:
        print "Usage: bongar <input file> <output file> <sample size> <num samples> <threshold> <relaxation factor>"
        exit(-1)

    data = sys.argv[1]
    fout = sys.argv[2]
    p = float(sys.argv[3]) #Expressed as 0<p<=1, the probability that a given row will be in the sample
    n_p = int(sys.argv[4]) #Number of samples to make
    s = float(sys.argv[5]) #Threshold for the final itemsets
    r = float(sys.argv[6]) #Factor to dicrease the threshold for the samples


    findFrequentItemsets(data, fout, p, n_p, s, r)

    #End Spark
    sc.stop()
