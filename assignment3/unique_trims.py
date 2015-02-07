import MapReduce
import sys

"""
Find unique trims of DNA sequences in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # key: sequence id
    # value: string representation of a sequence of nucleotides
    key = record[0]
    value = record[1]
    shortened_value = value[:-10] # remove last 10 nucleotides
    mr.emit_intermediate(shortened_value, 1) # string becomes new key

def reducer(key, list_of_values):
    # key: string of the trimmed DNA sequence
    # value: list of occurrence counts, not used
    # All snipped DNA sequences with same sequence past to same reducer
    # only output one
    mr.emit(key)


# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
