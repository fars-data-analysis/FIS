import csv
import helpers as hps

import settings

threshold = 55555

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
	#print len(row)
	for name,value in row.iteritems():
		item_set.add(name+':'+value)	# add new attributes if not yet in the set

print len(item_set)
# print item_set

n_occ = {}

[hps.createDict(n_occ,[a,0]) for a in item_set]

csv_file = open(conf_dic['data'],'r')
csvObj = csv.DictReader(csv_file,delimiter=',')

for row in csvObj:
        #print row
        row.pop('CASENUM')  # removes the column from the record
        #print len(row)
        for name,value in row.iteritems():
		#print name+":"+value
                n_occ[name+":"+value] += 1    # add new attributes if not yet in the set


counter = 0
selected = set()
new = set()

for name,val in n_occ.iteritems():
	if (val >= threshold):
		selected.add(name)
		counter+=1
		print name, val
print counter

while (len(selected)>0):
	sel = selected.pop()
	for i in selected:
		#print type(selected),type(i)
		new.add((sel,i))

print new
