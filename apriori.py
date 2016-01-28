from collections import defaultdict
from itertools import imap, combinations

"""

	Compact Pythonian version of Apriori Frequent Itemsets Mining algorithm

"""

# input:
#    transactions: List of lists containing numeric values
#     min_support: Minimum support ti be considered frequent (we select values that appears at least many times as s in baskets)
#	  		steps: Number of steps in case we would like to find couples takes 2 (default 0 terminates when there are no more candidates to compute)
#  returns:
#	  frequent_itemsets: List of frequent itemsets, contains both numeric values and tuples. (be carefull on iterating on it)

def get_frequent_items_sets(transactions,min_support,steps=0):
	# Initialization of the solution list
	frequent_itemsets = []
	# Initialization of candidates dictionary
	items = defaultdict(lambda: 0)
	# Counts the number of occurrences of each singleton
	[inc(items,item,1) for transaction in transactions for item in transaction]
	# Generate a set of singletons that are frequent as required in min_support (>= min_support)
	items = set(item for item, support in items.iteritems()
		if support >= min_support)
	# Appends frequent singletons to the solution
	[frequent_itemsets.append(item) for item in items]
	# Removes non frequent singletons from transactions
	transactions = [set(filter(lambda v: v in items, y)) for y in transactions]
	count = 2
	while len(items) > 0 and count != steps+1:
		# Generates candidates from frequent elements filtered from the previous step
		candidates = combinations([i for i in items],count)
		# Reinitialize the dictionary to count new n-ples
		items = defaultdict(lambda: 0)
		# Finds candidates on baskets and increment the count if needed
		[inc(items,candidate,1) for candidate in candidates for transaction in transactions if transaction.issuperset(candidate)]
		# Appends the solution of the step to the solutions list
		[frequent_itemsets.append(item) for item,support in items.iteritems() if support >= min_support]
		# Casts frequent items in a set of singletons
		items = set(element for tupl in items.iterkeys() for element in tupl)
		count+=1
	return frequent_itemsets

def inc(dic,key,val):
	dic[key]+=val

#[transaction.remove(element) for item,support in items.iteritems() for element in item if support < minimum_support and element in transaction]
