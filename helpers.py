"""

        This file contains only helper functions that are not 
        a fundamental part of the algotithm.

"""


# Given a dictionary and a pair of values, creates the key-value pair in the dictionary.
# Does not return, because in this case variables are passed by reference
def createDict(dic,a):
        dic[a[0]]=a[1].rstrip("\n")
