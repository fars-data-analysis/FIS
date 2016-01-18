import settings as stg
import pickle
import csv


#stg.print_conf_dic()


paths = []
openedFiles = []
csvObjects = []
item_set = set()
ignoreList = []
keyLookup = [] # for each number it contains the string value of attributes
valueLookup = {} # for each key it stores the numeric value of it
# itemBaskets = [set() for i in range(0,int(stg.conf_dic['baskets'][0])] # Array(/list) of sets (actually our baskets)

itemBaskets = {}

def genAttrSet():

    # GENERATES ALL FILE PATSH
    for subfolder in stg.conf_dic['subfolders']:
        for fileName in stg.conf_dic['filenames']:
            paths.append(stg.conf_dic['folder'][0]+subfolder+fileName+".csv")

    # OPEN ALL FILES IN READ-ONLY MODE
    for filePath in paths:
        openedFiles.append(open(filePath,'r'))

    # GENERATES A CSV Object TO HANDLE CDV EASYLY
    for openedFile in openedFiles:
        csvObjects.append(csv.DictReader(openedFile,delimiter=','))

    # OPEN ALL IGNORE FILE
    openedIgnoreFiles = []
    for fileName in stg.conf_dic['filenames']:
        openedIgnoreFiles.append(open(stg.conf_dic['ignore'][0]+fileName,'r'))

    # GENERATE A LIST FOR EVERY INGOREFILE
    for f in openedIgnoreFiles:
        ignoreList.append([x.strip("\n") for x in f.readlines()])

    # CREATES THE SET
    productId = 0
    for i in range(0,len(csvObjects)):
        csvObj = csvObjects[i]
        for row in csvObj:
            ignoreCols = ignoreList[i % len(ignoreList)]
            #print row

            joinerValue = row.pop(stg.conf_dic['joiner'][0])
            #print arr
            for j in ignoreCols:
                try:
                    if (j!= stg.conf_dic['joiner'][0]):
                        c = row.pop(j)
                        #print row
                except:
                    print "error line 47 setGenerator, malformed ignorefile."
            for name,value in row.iteritems():
                attr = name+':'+value
                item_set.add(attr)	# add new attributes if not yet in the set
                if attr in valueLookup:
                    index = valueLookup[attr]
                else:
                    valueLookup[attr] = productId
                    keyLookup.append(attr)
                    productId+=1
                if joinerValue in itemBaskets:
                    itemBaskets[joinerValue].add(productId)
                else:
                    itemBaskets[joinerValue] = set([productId])
"""

    pickle.dump(keyLookup, open("out/keyLookup.p", "w"))
    pickle.dump(valueLookup, open("out/valueLookup.p", "w"))
    global keyLookup
    keyLookup = [0]*len(item_set)
    for attr in item_set:
        valueLookup[attr] = i
        keyLookup[i] = attr
        i+=1

"""



def printstats():
    #print "PATHS: "
    #for i in paths:
    #    print i
    #print " NUMBER OF FILES OPENED: ",len(openedFiles)
    #print " NUMBER OF CSV OPENED: ",len(csvObjects)
    #print " FILE BASENAMES:", stg.conf_dic['filenames']
    #print " ignoreList: ",ignoreList
    #print item_set
    #print len(item_set)
    #print "len keyLookup: ",len(keyLookup),keyLookup
    #print "len valueLookup: ",len(valueLookup),valueLookup
    print itemBaskets

def main():
    genAttrSet()
    printstats()




if __name__ == '__main__':
    main()
