from pyspark.sql import SparkSession

# the memory may need to be upped for the full dataset
spark = SparkSession.builder \
    .appName("MongoDBConnection") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Project_Test.Label") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Project_Test.Label") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

Label_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
Label_Data = Label_Data.select("date", "Time", "CoarseLabel")

Hips_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("spark.mongodb.input.uri", "mongodb://localhost:27017/Project_Test.Hips_Motion") \
    .load()

# Optionally, select specific columns from the Hips_Motion collection
Hips_Data = Hips_Data.select("date", "Time", "AccelerationX", "AccelerationY")  # Adjust columns as needed

# Join the two datasets on the common columns (e.g., "date" and "Time")
combined_data = Label_Data.join(Hips_Data, on=["date", "Time"], how="inner")


# Show the combined result
combined_data.show()
