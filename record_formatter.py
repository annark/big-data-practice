"""
Anna Russo Kennedy

Mapper Input
The input is a 2 element list: [document_id, text]
document_id: document identifier formatted as a string
text: text of the document formatted as a string
The document text may have words in various cases or elements of punctuation. Do not modify the
string, and treat each token as if it was a valid word. (That is, just use value.split())

Reducer Output
The output should be a (word, document ID list) tuple where word is a String and document ID list
is a list of Strings.

Run usig:
python record_formatter.py training_SyncDiagnosis.csv
"""

import MapReduce
import csv
import sys

# Part 1 
mr = MapReduce.MapReduce()

# Part 2 
def mapper(record):
    # key: document identifier
    # value: document contents
    # Column format:
    #   0           1           2           3               4           5           6       7
    #   recordID    PatientId   ICD9 Code   Diagnosis desc. Startyear   Stopyear    Acute   DrID
    patient_id = record[1]

    try:
        diagnosis = float(record[2])
        mr.emit_intermediate(patient_id, diagnosis)
    except ValueError:
        pass

def reducer(key, list_of_values):
    # key: patient_id
    # list_of_values: list of diagnoses for patient
    # List format:
    # 0             1         2         3       4   5    6
    # patient_id    cancers   diabetes  stress  MS  IBS  Arthritis

    # Following are ICD classification codes we are interested in:
    # neoplasms (cancers): 140-239
    # diabetes: 249,250
    # stress (and related): 308, 309
    # MS: 340
    # IBS: 564.1
    # arthritis: 710-719
    list_of_diagnoses = [0 for i in range(7)]
    list_of_diagnoses[0] = key

    found = False
    for diagnosis in list_of_values:
        if diagnosis >= 140.0 and diagnosis < 240.0:
            list_of_diagnoses[1] = 1 # Cancers
            found = True
        elif diagnosis >= 249.0 and diagnosis < 251.0:
            list_of_diagnoses[2] = 1 # Diabetes
            found = True
        elif diagnosis >= 308.0 and diagnosis < 310.0:
            list_of_diagnoses[3] = 1 # Stress & related
            found = True
        elif diagnosis >= 340.0 and diagnosis < 341.0:
            list_of_diagnoses[4] = 1 # MS
            found = True
        elif diagnosis == 564.1:
            list_of_diagnoses[5] = 1 # IBS
            found = True
        elif diagnosis >= 710.0 and diagnosis < 720.0:
            list_of_diagnoses[6] = 1 # Arthritis
            found = True
    if found:
        mr.emit(list_of_diagnoses)

with open(sys.argv[1], 'rU') as input_file:
    headers = []
    reader = csv.reader(input_file, quotechar='"', delimiter = ',')
    headers = reader.next()
    mr.execute(reader, mapper, reducer)
