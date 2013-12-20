import sys
import csv

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)
        

    def emit(self, value):
        self.result.append(value)

    def execute(self, data, mapper, reducer):
        record_count = 0
        for record in data:
            record_count += 1
            mapper(record)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        with open('EDITED_'+ sys.argv[1], 'w') as outfile:
            out = csv.writer(outfile)
            for item in self.result:
                print item
                out.writerow(item)
        print record_count


