from collections import defaultdict
from itertools import imap, combinations


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
def create_candidates(itemSet, length):
        return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])

# Checks occurrencies and returns frequent items
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
