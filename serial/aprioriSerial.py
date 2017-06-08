from collections import defaultdict




# input:
#    data: List of lists containing numeric values
#     min_sup: Minimum support ti be considered frequent (we select values that appears at least many times as s in baskets)
#	  		steps: Number of steps in case we would like to find couples takes 2 (default 0 terminates when there are no more candidates to compute)
#  returns:
#	  solution: dictionary of sets of frozensets representing frequent itemsets, contains both numeric values and tuples.
#		(be carefull on iterating on it)

def get_frequent_items_sets(data,min_sup,steps=0):

	# we transform the dataset in a list of sets
	transactions = list()

	# Temporary dictionary to count occurrences
	items = defaultdict(lambda: 0)

	# Returned dictionary
	solution = dict()
	L_set = set()

	# Fills transactions and counts "singletons"
	for line in data:
		transaction = set(line)
		transactions.append(transaction)
		for element in transaction:
			items[element]+=1

	# Add to the solution all frequent items
	for item, count in items.iteritems():
		if count >= min_sup:
			L_set.add(frozenset([item]))

	# Generalize the steps it ends when there are no candidates
	# or if the user provided as input the number of parameters
	k = 2
	solution[k-1] = L_set
	while L_set != set([]) and k != steps+1:
		L_set = create_candidates(L_set,k)
		C_set = frequent_items(L_set,transactions,min_sup)
		if C_set != set([]): solution[k]=C_set
		L_set = C_set
		k = k + 1
	return solution





# Creates candidates joining the same set
# input:
#    itemSet: a set of frequent items
#     length: the cardinality of generated combinations
#  returns:
#	   set: a set containing all combinations with cardinality equal to length
#		(be carefull on iterating on it)

def create_candidates(itemSet, length):
        return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])




# Checks occurrences of items in transactions and returns frequent items
# input:
#    items: a set of frequent items (candidates)
#     transactions: list of sets representing baskets
#  returns:
#	   _itemSet: a set containing all frequent candidates (a subset of inputs)

def frequent_items(items, transactions, min_sup):
	_itemSet = set()
	counter = defaultdict(int)
	localDict = defaultdict(int)
	for item in items:
		for transaction in transactions:
			if item.issubset(transaction):localDict[item] += 1

	for item, count in localDict.items():
		if count >= min_sup:
			_itemSet.add(item)
	return _itemSet


# helper to standardize the output (not part of the algorithm)

def please_clean(solution):
	res = []
	for i in solution.itervalues():
		for j in i:
			res.append(j)
	return res
