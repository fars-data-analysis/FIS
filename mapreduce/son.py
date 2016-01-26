from __future__ import print_function

import sys

import numpy as np
import itertools as it

from pyspark import SparkContext

def candidateSubsets(lines):
    threshold = 100
    max_card = 3  #longest length of itemsets generated
    baskets = [[product for product in line.split(' ')] for line in lines]
    counts_1 = {}
    #calculate single term frequency
    for basket in baskets:
        for item in basket:
            if counts_1.has_key(item):
                counts_1[item]+=1
            else:
                counts_1[item]=1

    #calculate frequent items
    freqItems = [x for x in counts_1.keys() if counts_1[x]>=threshold]

    #calculate frequent pairs
    counts_2 = {}
    for basket in baskets:
        for t2_ple in it.combinations(basket, 2):
            if counts_2.has_key(t2_ple):
                counts_2[t2_ple]+=1
            else:
                counts_2[t2_ple]=1

    freqPairs = [x for x in counts_2.keys() if counts_2[x]>=threshold]

    counts_3 = {}
    for basket in baskets:
        for t3_ple in it.combinations(basket,3):
            if (t3_ple[0], t3_ple[1]) in freqPairs and (t3_ple[0], t3_ple[2]) in freqPairs and (t3_ple[1], t3_ple[2]) in freqPairs:
                if counts_3.has_key(t3_ple):
                    counts_3[t3_ple]+=1
                else:
                    counts_3[t3_ple]=1

    freqTriples = [x for x in counts_3.keys() if counts_3[x]>=threshold]

    return freqItems+freqPairs+freqTriples

"""
if __name__=="__main__":

    if len(sys.argv) != 4:
        print("Usage: son <file> <k> <s>", file=sys.stderr)
        exit(-1)

        sc = SparkContext(appName="FreqItemsetSON")
        line = sc.textFile(sys.argv[1])
        candidates = lines.mapPartitions(candidateSubsets).reduceByKey(lambda x: 1)
        output = candidates.collect()
        print candidates
        sc.stop()
"""
