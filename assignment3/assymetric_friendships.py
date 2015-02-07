import MapReduce
import sys

"""
Find the asymmetric friendships in a social network using the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # key: hash each name then add the result to make the ordering of names not matter (since addition is commutative).
    key = hash(record[0]) + hash(record[1]) # make ordering not matter so passed to same reducer
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    # key: unordered set of two friends
    # list_of_values: if 1 value, then asymmetric friendship. If 2 values, then symmetric friendship
    if len(list_of_values) == 1:
        mr.emit(list_of_values[0])

# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
