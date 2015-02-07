import MapReduce
import sys

"""
Count the number of friends a person has in a social network using the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # person: person for whom we are counting number of friends
    person = record[0]
    mr.emit_intermediate(person, 1)

def reducer(person, list_of_values):
    # person: person for whom we are counting number of friends
    # value: list of occurrence counts
    friend_count = 0
    for v in list_of_values:
      friend_count += v
    mr.emit((person, friend_count))

# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
