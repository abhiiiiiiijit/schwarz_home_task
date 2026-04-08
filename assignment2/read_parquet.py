from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("ReadParquet") \
    .getOrCreate()

# Path to folder containing parquet files
path = "/home/adminabhi/gitrepo/schwarz_home_task/assignment2/warehouse/sales_dedup2/"

# Read parquet files
df = spark.read.parquet(path)

# Show schema
df.printSchema()

# Display first rows
df.show(20, truncate=False)