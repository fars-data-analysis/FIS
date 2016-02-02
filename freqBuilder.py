import sys

from string import atoi
from pyspark import SparkContext, SparkConf

if __name__=="__main__":

    APP_NAME = "FreqBuilder"

    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")

    sc  = SparkContext(conf=conf)

    finput = sys.argv[1]
    foutput = sys.argv[2]

    data = sc.textFile(finput).flatMap(lambda x: [(int(y), 1) for y in x.strip().split(' ')])

    count = data.count()

    sums = data.reduceByKey(lambda x,y: x+y).sortBy(lambda (x,c): -c).collect()

    fopen = open(foutput, 'w')

    for (i,c) in sums:
        fopen.write(str(i)+","+str(c)+"\n")

    #End Spark
    sc.stop()
