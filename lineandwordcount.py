from pyspark import SparkContext

# Initialize Spark Context
sc = SparkContext("local", "Line and Word Character Count")

# Given paragraph
paragraph = """
This is the first line.
And this is the second line.
Here comes the third line.
"""

# Create RDD from paragraph
lines = sc.parallelize(paragraph.strip().split("\n"))

# Count number of lines
line_count = lines.count()
print("Number of lines:", line_count)

# Split lines into words and count characters of each word
word_char_count = lines.flatMap(lambda line: line.split()) \
                       .map(lambda word: (word, len(word)))

# Collect and print result
for word, count in word_char_count.collect():
    print("Word:", word, ", Characters:", count)

# Stop Spark Context
sc.stop()
spark-submit wordcon.py
