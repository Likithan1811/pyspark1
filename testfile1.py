# pyspark --jars /home/ec2-user/ojdbc8.jar

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

# Read data from the Oracle table into a DataFrame
df = spark.read.jdbc(url=jdbc_url, table="SPARKSAMPLE", properties=connection_properties)

# Show the data
df.show()
