
class FPTree(object):

    def __init__(self):
        self.root = Node(None, 0, {})
        self.summaries = {}

    def __repr__(self):
        return repr(self.root)

    def add(self, basket, count):
        curr = self.root
        curr.count += count

        for item in basket:
            if self.summaries.has_key(item):
                summary = self.summaries.get(item)
            else:
                summary = Summary(0,set())
                self.summaries[item] = summary
            summary.count += count

            if curr.children.has_key(item):
                child = curr.children.get(item)
            else:
                child = Node(item, 0, {})
                curr.addChild(child)
            summary.nodes.add(child)
            child.count += count
            curr = child
        return self

    def getTransactions(self):
        return [x for x in self.root._getTransactions()]

    def merge(self,tree):
        for t in tree.getTransactions():
            self.add(t[0],t[1])
        return self

    def project(self,itemId):
        newTree = FPTree()
        summaryItem = self.summaries.get(itemId)
        if summaryItem:
            for element in summaryItem.nodes:
                t = []
                curr = element.parent
                while curr.parent:
                    t.insert(0,curr.item)
                    curr = curr.parent
                newTree.add(t,element.count)
        return newTree

    def extract(self, minCount, isResponsible = lambda x:True, maxLength=None):
        for item,summary in self.summaries.iteritems():
            if (isResponsible(item) and summary.count >= minCount):
                yield ([item],summary.count)
                for element in self.project(item).extract(minCount, maxLength=maxLength):
                    if maxLength==None or len(element[0])+1<=maxLength:
                        yield ([item]+element[0],element[1])

class Node(object):

    def __init__(self, item, count, children):
        self.item = item
        self.count = count
        self.children = children #dictionary of children
        self.parent = None

    def __repr__(self):
        return self.toString(0)

    def toString(self, level=0):
        if self.item == None:
            s = "Root("
        else:
            s = "(item="+str(self.item)
            s+= ", count="+str(self.count)
        tabs = "\t".join(['' for i in range(0,level+2)])
        for v in self.children.itervalues():
            s+= tabs+"\n"
            s+= tabs+v.toString(level=level+1)
        s+=")"
        return s

    def addChild(self, node):
        self.children[node.item] = node
        node.parent = self

    def _getTransactions(self):
        count = self.count
        transactions = []
        for child in self.children.itervalues():
            for t in child._getTransactions():
                count-=t[1]
                t[0].insert(0,child.item)
                yield t
        if (count>0):
            yield ([],count)

class Summary(object):

    def __init__(self, count, nodes):
        self.count = count
        self.nodes = nodes
