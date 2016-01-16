import csv
import helpers as hps


# creates an empty dictionary
conf_dic = {}

# Open and loads the conf.cfg file:
with open('conf.cfg',"r") as config:
	cfg = config.readlines()

# fills the config dictionary
[hps.createDict(conf_dic,a.split(":")) for a in cfg]
print(conf_dic['data'])

# Opens the data file
csv_file = open(conf_dic['data'],'r')

# creates an csv Object
csvObj = csv.DictReader(csv_file,delimiter=',')

# This python type will be usefull to represent the all attributes set
item_set = set()

for row in csvObj:
	#print row
	row.pop('CASENUM')  # removes the column from the record
	print len(row)
	for name,value in row.iteritems():
		item_set.add(name+':'+value)	# add new attributes if not yet in the set

print len(item_set)
print item_set
