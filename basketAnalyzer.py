import sys

from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    APP_NAME = "Basket-Analyze"

    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[*]")

    sc  = SparkContext(conf=conf)

    f_input = sys.argv[1]
    f_output = sys.argv[2]

    baskets = sc.textFile(f_input).map(lambda l: [int(x) for x in l.strip().split(' ')]).persist()

    # Number of baskets
    num_baskets = baskets.count()

    # Avg basket length
    remapped = baskets.map(lambda k: len(k)).persist()
    avg_basket_length = remapped.mean()

    length_stdev = remapped.sampleStdev()
    length_var   = remapped.sampleVariance()

    # Median basket length
    #Slower than selection
    sorted_len = remapped.sortBy(lambda x: x)
    collected_len = sorted_len.collect()
    if len(collected_len) % 2 == 0:
        med_basket_length = (collected_len[len(collected_len)/2-1] + collected_len[len(collected_len)/2]) /2
    else:
        med_basket_length = collected_len[len(collected_len)/2]

    # Number of different items
    all_items = baskets.flatMap(lambda b: [(x,1) for x in b]).persist()
    freq_items = all_items.reduceByKey(lambda v1,v2: v1+v2).persist()
    num_items = freq_items.count()

    # Avg item frequency
    remapped = freq_items.map(lambda (item,freq): freq).persist()
    avg_freq = remapped.mean()

    freq_stdev =  remapped.sampleStdev()
    freq_var   = remapped.sampleVariance()

    # Median item frequency
    sorted_freq = remapped.sortBy(lambda x: x)
    collected_freq = sorted_freq.collect()
    if len(collected_freq) % 2 == 0:
        med_freq = (collected_freq[len(collected_freq)/2 -1]+collected_freq[len(collected_freq)/2])/2
    else:
        med_freq = collected_freq[len(collected_freq)/2]

    fout = open(f_output, 'w')

    fout.write("Datafile: "+f_input+"\n")

    fout.write("Num baskets: "+str(num_baskets)+"\n")

    fout.write("Avg basket length: "+str(avg_basket_length)+"\n")

    fout.write("Median basket length: "+str(med_basket_length)+"\n")

    fout.write("Std. dev.: "+str(length_stdev)+", var.: "+str(length_var)+"\n")

    fout.write("Num items: "+str(num_items)+"\n")

    fout.write("Avg item frequency: "+str(avg_freq)+"\n")

    fout.write("Median item frequency: "+str(med_freq)+"\n")

    fout.write("Std. dev.: "+str(freq_stdev)+", var.: "+str(freq_var)+"\n")
