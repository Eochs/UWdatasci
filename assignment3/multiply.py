import MapReduceTwoStep
import sys

"""
Matrix multiplication in the Simple Python MapReduceTwoStep Framework I created
"""

mr = MapReduceTwoStep.MapReduceTwoStep()

# =============================

def mapper(record):
    # record is ("a", i, j, a_ij) or ("b", j, k, b_jk)
    # Want to map A and B values to machine that will match all a_ij to b_jk
    # for the purposes of eventually making c_ik = sum(a_ij * b_jk) 'over j'
    # So each machine given 'j' as id to map to.
    matrix = record[0]
    row = record[1]
    col = record[2]
    value = record[3]
    if matrix == 'a':
        # map each A value to it's respective column index number
        mr.emit_intermediate_map(col, ('A', row, value))
    else: # matrix == 'B'
        mr.emit_intermediate_map(row, ('B', col, value))

def reducer1(key, values):
    # key: call it j, where C_ik = (Sum over j) a_ij * b_jk
    # value: list of ('A', i, a_ij) and ('B', k, b_jk)
    # match up all a_ij with b_jk where j matches then send to reducer which handles computing c_ik (so new key becomes (i,k))
    list_A = [(i, a_ij) for (M, i, a_ij) in values if M == "A"]
    list_B = [(k, b_jk) for (M, k, b_jk) in values if M == "B"]
    for (i, a_ij) in list_A:
        for (k, b_jk) in list_B:
            mr.emit_intermediate_reduce((i, k), a_ij * b_jk )

def reducer2(key, list_of_values):
    # key: (i, k) index of value c_ik in matrix C = AB
    # value: list of (a_ij * b_jk) for different j's but i and k constant
    c_ik = 0
    for value in list_of_values:
        c_ik += value
    mr.emit([key[0], key[1], c_ik])


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer1, reducer2)
