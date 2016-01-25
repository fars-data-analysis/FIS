import sys

from string import atoi
from pyspark import SparkContext
from fpTree import FPTree

# /home/alexander/Downloads/DataBigData/data/mushroom/mushroom.txt

def getFrequentItems(data,minSupport):
    singleItems = data.flatMap(lambda x: [(y,1) for y in x])
    freqItems = [x for (x,y) in
    sorted(singleItems.reduceByKey(lambda x,y: x+y)
        .filter(lambda c: c[1]>=minSupport).collect(), key=lambda x: -x[1])]
    return freqItems

def getFrequentItemsets(data,minSupport,freqItems):
    rank = dict([(index, item) for (item,index) in enumerate(freqItems)]) # Ordered list based on the freequncy of items, the first is the most appearing one , the last is not appearing too much.
    numPartitions = data.getNumPartitions()
    workByPartition = data.flatMap(lambda basket: genCondTransactions(basket,rank,numPartitions))
    emptyTree = FPTree()
    forest = workByPartition.aggregateByKey(emptyTree,lambda tree,transaction: tree.add(transaction,1),lambda tree1,tree2: tree1.merge(tree2))
    itemsets = forest.flatMap(lambda (partId, bonsai): bonsai.extract(minSupport, lambda x: getPartitionId(x,numPartitions) == partId))
    return itemsets

def genCondTransactions(basket, rank, nPartitions):
    #translate into new id's using rank
    filtered = [rank[int(x)] for x in basket if rank.has_key(int(x))]
    #sort basket in ascending rank
    filtered = sorted(filtered)
    #subpatterns to send to each worker. (part_id, basket_slice)
    output = {}
    for i in range(len(filtered)-1, -1, -1):
        item = filtered[i]
        partition = getPartitionId(item, nPartitions)
        if not output.has_key(partition):
            output[partition] = filtered[:i+1]
    return [x for x in output.iteritems()]

def getPartitionId(key, nPartitions):
    return key % nPartitions

def main(sc):
    file = "/home/alexander/Downloads/DataBigData/data/mushroom/mushroom.txt"
    support = 2
    numPartitions = 2
    minSupport = support

    source=[[1, 2, 3, 4, 5],
    [1, 2, 4, 5, 6],
    [1, 2, 5, 7, 6],
    [1, 2, 4, 7, 6],
    [1, 2, 5, 7, 6]]

    data = sc.parallelize(source,2)

    #data = sc.textFile(file, minPartitions=numPartitions).map(lambda x: map(int, x.strip().split(' '))).persist()

    freqItems = getFrequentItems(data, minSupport)
    freqItemsets = getFrequentItemsets(data, minSupport, freqItems)

    return freqItems,freqItemsets
