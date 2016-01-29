import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    APP_NAME = "Basket-Analyze"

    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")

    sc  = SparkContext(conf=conf)

    f_input = sys.argv[1]
    f_output = sys.argv[2]

    baskets = sc.textFile(input).persist()

    # Number of baskets
    num_baskets = baskets.count()

    # Avg basket length
    remapped = baskets.map(lambda k: len(k))).persist()
    avg_basket_length = remapped.sum()/num_baskets

    # Median basket length
    med_basket_length = remapped.mean()

    # Number of different items
    all_items = baskets.flatMap(lambda b: [(x,1) for x in b]).persist()
    freq_items = all_items.reduceByKey(lambda v1,v2: v1+v2).persist()
    num_items = freq_items.count()

    # Avg item frequency
    remapped = freq_items.map(lambda (k,v): k).persist()
    avg_freq = remapped.sum()/num_items

    # Median item frequency
    med_freq = remapped.mean()

    fout = open(f_output, 'w')

    fout.write("Datafile: "+f_input+"\n")

    fout.write("Num baskets: "+num_baskets+"\n")

    fout.write("Avg basket length: "+avg_basket_length+"\n")

    fout.write("Median basket length: "+med_basket_length+"\n")

    fout.write("Num items: "+num_items+"\n")

    fout.write("Avg item frequency: "+avg_freq+"\n")

    fout.write("Median item frequency: "+med_freq+"\n")
