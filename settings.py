import helpers as hps


conf_dic = {}

# Open and loads the conf.cfg file:
with open('conf.cfg',"r") as config:
	cfg = config.readlines()

# fills the config dictionary
[hps.createDict(conf_dic,a.split(":")) for a in cfg]

def print_conf_dic():
	for i in conf_dic.iteritems():
		print i
