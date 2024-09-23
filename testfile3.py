from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Oracle JDBC Example") \
    .getOrCreate()

# JDBC connection properties
jdbc_url = "jdbc:oracle:thin:@//158.101.135.234:1521/ORCL"
connection_properties = {
    "user": "de_dev_tantor",
    "password": "de_dev_tantor",
    "driver": "oracle.jdbc.driver.OracleDriver"
}

# Partitioning options
partition_column = "STUDENT_ID"  # Use the appropriate column from your table
lower_bound = 1          # Set based on your data's minimum value for the partition column
upper_bound = 1000     # Set based on your data's maximum value for the partition column
num_partitions = 5      # Number of partitions

# Read data from the Oracle table into a DataFrame with partitioning
df = spark.read.jdbc(
    url=jdbc_url,
    table="SGS_SCHOOL_NEW_10",
    properties=connection_properties,
    column=partition_column,
    lowerBound=lower_bound,
    upperBound=upper_bound,
    numPartitions=num_partitions
)

# Show the data
df.show(1000)
