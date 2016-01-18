import settings as stg
import csv


#stg.print_conf_dic()


paths = []
openedFiles = []
csvObjects = []
item_set = set()
ignoreList = []


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

    # OPEN ALL OGNORE FILE
    openedIgnoreFiles = []
    for fileName in stg.conf_dic['filenames']:
        openedIgnoreFiles.append(open(stg.conf_dic['ignore'][0]+fileName,'r'))

    # GENERATE A LIST FOR EVERY INGOREFILE
    for f in openedIgnoreFiles:
        ignoreList.append([x.strip("\n") for x in f.readlines()])

    # CREATES THE SET
    for i in range(0,len(csvObjects)):
        csvObj = csvObjects[i]
        for row in csvObj:
            arr = ignoreList[i % len(ignoreList)]
            #print arr
            for j in arr:
                #print j
                try:
                    c = row.pop(j)
                    #print row
                except:
                    print "error line 47 setGenerator, malformed ignorefile."
        	for name,value in row.iteritems():
                    item_set.add(name+':'+value)	# add new attributes if not yet in the set


def printstats():
    print "PATHS: "
    for i in paths:
        print i
    print " NUMBER OF FILES OPENED: ",len(openedFiles)
    print " NUMBER OF CSV OPENED: ",len(csvObjects)
    print " FILE BASENAMES:", stg.conf_dic['filenames']
    print " ignoreList: ",ignoreList
    print item_set

def main():
    genAttrSet()
    printstats()




if __name__ == '__main__':
    main()
