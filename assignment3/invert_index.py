import MapReduce
import sys

"""
Create an Inverted Index in the Simple Python MapReduce Framework.
Given a set of documents, an inverted index is a dictionary where each word is associated with a list of the document identifiers in which that word appears.
"""

mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # docid: document identifier
    # value: document contents, text of the document
    docid = record[0]
    value = record[1]
    words = list(set(value.split())) # only want unique (word,doc) pairs
    for w in words:
      mr.emit_intermediate(w, docid)

def reducer(word, list_of_docs):
    # total: list of documents word occurs in
    total = []
    for docid in list_of_docs:
      total.append(docid)
    mr.emit((word, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
