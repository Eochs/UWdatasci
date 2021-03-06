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

def reducer(key, friendships):
    # key: hash of unordered set of two friends
    # friendships: if 1 value, then asymmetric friendship. If 2 or more values, then possibly symmetric friendship, possibly hash collision
    if len(friendships) == 1:
        mr.emit(friendships[0])

    else: # possible hash collision or symmetric friendship
        # inefficient, but multiple asymmetric to same hash unlikely
        for ith, (person1, friend1) in enumerate(friendships):
            asymmetric = True
            # check other friendships to see if this one is symmetric
            for person2, friend2 in friendships[:ith]+friendships[ith+1:]:
                if person1 == friend2 and friend1 == person2:
                    asymmetric = False
            # if no matches found
            if(asymmetric):
                mr.emit([person1,friend1])
            
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
