import MapReduce
import sys

"""
Joining records using the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================

def mapper(record):
    # table: identifies the table the record originates from.
    #    Can be "line_item" or "order" 
    # rid: record id, used to join line item to orders
    order_id = record[1]
    mr.emit_intermediate(order_id, record)

def reducer(order_id, records):
    # order_id
    # records: list of all records with that order id
    for ith, record in enumerate(records):
        for record_to_join in records[ith:]:
            if record[0] != record_to_join[0]: # if from different tables
                mr.emit(record + record_to_join)


# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
