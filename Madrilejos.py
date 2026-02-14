# Lab1_RDD_Pipeline.py
# Name: Madrilejos, Polly Cristy P.
# Lab No. 1
# Date: 02/14/2026
# Objective: Implement a distributed data processing pipeline using Apache Spark RDDs

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("RDD Pipeline Activity") \
    .getOrCreate()

# Get the SparkContext
sc = spark.sparkContext

# List of sentences (input data)
sentences = [
    "Spark makes big data processing fast",
    "RDD operations are powerful",
    "Spark uses lazy evaluation"
]

# Step 1: Create an initial RDD
rdd = sc.parallelize(sentences)

# Step 2: Split sentences into words (flatMap)
words_rdd = rdd.flatMap(lambda sentence: sentence.split(" "))

# Step 3: Convert words to lowercase (map)
lowercase_rdd = words_rdd.map(lambda word: word.lower())

# Step 4: Filter out short words (length <= 2)
filtered_rdd = lowercase_rdd.filter(lambda word: len(word) > 2)

# Step 5: Remove duplicate words (distinct)
unique_rdd = filtered_rdd.distinct()

# Step 6: Sort words alphabetically (sortBy)
sorted_rdd = unique_rdd.sortBy(lambda word: word)

# Step 7: Collect results to driver
final_result = sorted_rdd.collect()

# Print the final sorted unique words
print(final_result)

# Stop SparkSession
spark.stop()
