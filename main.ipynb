from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, date_format

# Initialize Spark session
spark = SparkSession.builder \
    .appName("WebServerLogsAnalysis") \
    .getOrCreate()

# Load the CSV file into a DataFrame
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Task 1: Group by IP and count the number of requests for each IP, show the top 10 most active IPs
top_ips = df.groupBy("ip").count().orderBy(col("count").desc()).limit(10)
print("Top 10 most active IPs:")
top_ips.show()

# Task 2: Group by HTTP method and count the number of requests for each method
method_counts = df.groupBy("method").count()
print("\nHTTP Method Counts:")
method_counts.show()

# Task 3: Filter and count the number of requests with a response code of 404
response_404_count = df.filter(col("response_code") == 404).count()
print(f"\nNumber of requests with response code 404: {response_404_count}")

# Task 4: Group by date and sum the response sizes, sorted by date
df_with_date = df.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
date_response_size = df_with_date.groupBy("date").agg(sum("response_size").alias("total_response_size")).orderBy("date")
print("\nSum of response sizes by date:")
date_response_size.show()

# Stop the Spark session
spark.stop()
