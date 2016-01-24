import sys

from string import atoi
from pyspark import SparkContext

# /home/alexander/Downloads/DataBigData/data/mushroom/mushroom.txt
i_nput = "/home/alexander/Downloads/DataBigData/data/mushroom/mushroom.txt"
parameter = 120

data = sc.textFile(i_nput)
minSupport = parameter

freqItems = getFrequentItems(data, minSupport)
freqItemsets = getFrequentItemsets(data, minSupport, freqItems)

def getFrequentItems(data,minSupport):
    singleItems = data.flatMap(lambda x: [(int(y),1) for y in x.strip().split(' ')])
    freqItems = [x for (x,y) in
    sorted(singleItems.reduceByKey(lambda x,y: x+y)
        .filter(lambda c: c[1]>=minSupport).collect(), key=lambda x: -x[1])]
    return freqItems

def getFrequentItemsets(data,minSupport,freqItems):
    rank = dict([(index, item) for (item,index) in enumerate(freqItems)]) # Ordered list based on the freequncy of items, the first is the most appearing one , the last is not appearing too much.
    numPartitions = data.getNumPartitions()
    dd = data.flatMap(lambda basket: genCondTransactions(basket,rank,numPartitions))    #
    return dd

def genCondTransactions(basket, rank, nPartitions):
    #translate into new id's using rank
    filtered = [rank[int(x)] for x in basket.strip().split(" ") if rank.has_key(int(x))]
    #sort basket in ascending rank
    filtered = sorted(filtered)
    #subpatterns to send to each worker. (part_id, basket_slice)
    output = {}
    for i in range(len(filtered)-1, -1, -1):
        item = filtered[i]
        partition = getPartitionId(item, nPartitions)
        if(!output.has_key(partition)):
            output[partition] = filtered[:i+1]
    return [x for x in output.iteritems()]

def getPartitionId(key, nPartitions):
    return key % nPartitions
