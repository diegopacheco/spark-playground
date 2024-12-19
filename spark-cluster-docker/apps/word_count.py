from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# In-memory string
text = ["Hello world", "Hello Spark", "Hello world"]

# Create an RDD from the in-memory string
text_rdd = spark.sparkContext.parallelize(text)

# Perform word count
word_counts = text_rdd.flatMap(lambda line: line.split(" ")) \
                      .map(lambda word: (word, 1)) \
                      .reduceByKey(lambda a, b: a + b)

# Collect and print the results
for word, count in word_counts.collect():
    print(f"{word}: {count}")

# Stop the Spark session
spark.stop()