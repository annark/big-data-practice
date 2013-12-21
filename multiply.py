"""
Anna Russo Kennedy

Assuming two matrices A and B in a sparse matrix format, where each record is of the form
i, j, value, computes matrix multiplication: A x B

Map Input
The input to the map function will be matrix row records formatted as lists.

Reduce Output
The output from the reduce function will also be matrix row records formatted as tuples. Each tuple
will have the format (i, j, value) where each element is an integer.

Usage: python multiply.py matrix.json
"""

import MapReduce
import sys

mr = MapReduce.MapReduce()

def mapper(record):
    # record[0] - Matrix id - a or b
    # record[1] - i position
    # record[2] - j position
    # record[3] - value

    # This code assumes an nxn matrix, where n = 4.
    n = 4

    # Key records by the cell they need to be in for computation
    if record[0] == "a":
        for x in range(0, n + 1):
            mr.emit_intermediate((record[1], x), record)
    else:
        for y in range(0, n + 1):
            mr.emit_intermediate((y, record[2]), record)

def reducer(key, list_of_values):
    # key: Matrix position
    # list_of_values: all entries that need to be multiplied and summed

    # Split list into a and b records.
    a_entries = []
    b_entries = []
    for entry in list_of_values:
        if entry[0] == "a":
            a_entries.append(entry)
        else:
            b_entries.append(entry)

    total = 0
    for a_entry in a_entries:
        for b_entry in b_entries:
            if a_entry[2] == b_entry[1]:
                total = total + a_entry[3] * b_entry[3]

    mr.emit([key[0], key[1], total])

inputdata = open(sys.argv[1])
mr.execute(inputdata, mapper, reducer)
