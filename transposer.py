# Anna Russo Kennedy
# Script to take Statistics Canada CSV file and output a transposed, edited CSV
# in a format suitable for Google Motion Chart visualizations.
import csv
import sys


def transpose():
    # Input file format:
    # ['Geography', 'Wages', 'Type of work', 'National Occupational Classification for Statistics (NOC-S)', 'Sex', 'Age group', '1997', '1998', '1999', '2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011', '2012']
    # Output file format:
    # ['National Occupational Classification for Statistics (NOC-S)', 'year', 'workers (*1000)', 'wages']

    with open(sys.argv[1], 'rU') as filename:
        headers = []
        try:
            reader = csv.reader(filename, quotechar='"', delimiter = ',')
            headers = reader.next()
            zipped = zip(*reader)
        except csv.Error as e:
            sys.exit('file %s, line %d: %s' % (filename, reader.line_num, e))

    with open('EDITED '+ sys.argv[1], 'w') as outfile:
        out = csv.writer(outfile)
        newHeaders = []
        newHeaders.append(headers[2])
        newHeaders.append('year')
        newHeaders.append('workers (*1000)')
        out.writerow(newHeaders)

        for x in range(0, len(zipped[2])):
            for y in range(4, len(headers)):
                row = []
                row.append(zipped[2][x])
                row.append(headers[y])
                row.append(zipped[y][x])
                out.writerow(row)


def main():
    transpose()

if __name__ == '__main__':
    main()