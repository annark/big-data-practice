-- Anna Russo Kennedy
-- From assignment for Uvic course CSC485C, based on Coursera Big Data course.

-- This Pig script uses data from the Billion Triples Challenge dataset, an RDF dataset
-- that contains ~1 billion triples from the Semantic Web of the form:
--      subject  predicate  object  [context]

-- The script groups tuples by the subject column, creates & stores histogram data showing
-- the distribution of counts per subject.

register s3n://uw-cse-344-oregon.aws.amazon.com/myudfs.jar

-- Load the file into Pig
raw = LOAD 's3n://uw-cse-344-oregon.aws.amazon.com/btc-2010-chunk-*' USING TextLoader as (line:chararray);

-- Parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

-- Group the n-triples by subject column
subjects = group ntriples by (subject) PARALLEL 50;

-- Flatten the subjects out (because group by produces a tuple of each subject
-- in the first column, and we want each subject to be a string, not a tuple),
-- and count the number of tuples associated with each subject
count_by_subject = foreach subjects generate flatten($0), COUNT($1) as count PARALLEL 50;

-- Order the resulting tuples by their count in descending order
count_by_subject_ordered = order count_by_subject by (count) PARALLEL 50;

-- Group the results by these intermediate counts (x-axis values) and compute the final counts (y-axis values).
final_counts = group count_by_subject_ordered by (count) PARALLEL 50;

xy_axis = foreach final_counts generate flatten($0), COUNT($1) as count PARALLEL 50;

xy_axis_ordered = order xy_axis by (count) PARALLEL 50;

-- Store results in corresponding directory.
store xy_axis_ordered into '/user/hadoop/compute-histogram' using PigStorage();
