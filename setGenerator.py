import settings as stg
import pickle
import csv
import os
import datetime

#stg.print_conf_dic()


paths = []
openedFiles = []
openedIgnoreFiles = []
csvObjects = []
item_set = set()
ignoreList = []
keyLookup = [] # for each number it contains the string value of attributes
valueLookup = {} # for each key it stores the numeric value of it
# itemBaskets = [set() for i in range(0,int(stg.conf_dic['baskets'][0])] # Array(/list) of sets (actually our baskets)

itemBaskets = {}

def genAttrSet():

    # GENERATES ALL FILE PATHS
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
                if value!="":
                    attr = name+':'+value
                    item_set.add(attr)	# add new attributes if not yet in the set
                    if attr in valueLookup:
                        index = valueLookup[attr]   # index is the actual value for attributes (already inserted)
                    else:
                        valueLookup[attr] = productId
                        keyLookup.append(attr)
                        index = productId           # index is a new value for the new attribute, if it is the first time appearing here
                        productId+=1                # Product id is the value assigned to new entries that will come
                    if itemBaskets.has_key(joinerValue):
                        dic = itemBaskets[joinerValue]
                        dic.add(index)
                    else:
                        itemBaskets[joinerValue] = set([index])
    return [x for x in itemBaskets.values()]



#    pickle.dump(itemBaskets, open("out/itemBaskets.p", "w"))

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

def countFrequency(itemBaskets):
    freq = [0]*len(itemBaskets)
    for x in itemBaskets:
        for y in x:
            freq[y]+=1
    return freq


def printstats():
    #print "PATHS: "
    #for i in paths:
    #    print i
    #print " NUMBER OF FILES OPENED: ",len(openedFiles)
    #print " NUMBER OF CSV OPENED: ",len(csvObjects)
    #print " FILE BASENAMES:", stg.conf_dic['filenames']
    #print " ignoreList: ",ignoreList
    #print item_set
    print len(item_set)
    print "len keyLookup: ",len(keyLookup),keyLookup
    print "len valueLookup: ",len(valueLookup),valueLookup
    print itemBaskets


def saveToFile(directory):
    fileData = "data"
    fileTrans = "translation"
    fileRTrans = "r_ranslation"
    fileSummary = "summary"

    try:
        print "Creating directory: ",directory
        os.makedirs(directory)
    except OSError:
        print "already exists"
        if not os.path.isdir(directory):
            raise

    print "Reading csv files..."
    baschetti = genAttrSet()

    print "Writing translated data to: ", directory,"/",fileData
    fData = open(directory+"/"+fileData,'w')
    for b in baschetti:
        fData.write(" ".join([str(i) for i in b])+"\n")
    fData.close()
    print "done"


    print "Writing translation table to: ", directory,"/",fileTrans
    fTrans = open(directory+"/"+fileTrans,'w')
    fTrans.write("Look-up:\n")
    for l in range(0,len(keyLookup)):
        fTrans.write(str(l)+"\t"+keyLookup[l]+"\n")
    fTrans.close()
    print "done"

    print "Writing reverse look-up table to: ", directory, "/", fileRTrans
    fRTrans = open(directory+"/"+fileRTrans,'w')
    fRTrans.write("Reverse look-up table:\n")
    for l in sorted(valueLookup.keys()):
        fRTrans.write(l+"\t"+str(valueLookup[l])+"\n")

    fRTrans.close()
    print "done"

    print "Writing summary file to: ", directory,"/",fileSummary
    fSumm = open(directory+"/"+fileSummary,'w')
    fSumm.write("Generated on "+str(datetime.datetime.now())+"\n\n\n")
    fSumm.write("Files processed:\n\n")
    for i in range(0,len(openedFiles)):
        fSumm.write("\n"+openedFiles[i].name+"\n")
        if len(ignoreList[i]) > 0:
            fSumm.write("Ignored cols: "+", ".join(ignoreList[i])+"\n")

    fSumm.close()
    print "done"

    return

def main():
    baschetti = genAttrSet()
    #printstats()


if __name__ == '__main__':
    main()
