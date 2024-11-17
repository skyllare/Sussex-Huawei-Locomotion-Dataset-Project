from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoDBConnection") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Project_Test.Label") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Project_Test.Label") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

Label_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
Label_Data = Label_Data.select("date", "Time", "CoarseLabel")

# Show the selected columns

Label_Data.show()

Hips_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("spark.mongodb.input.uri", "mongodb://localhost:27017/Project_Test.Hips_Motion") \
    .load()

# Optionally, select specific columns from the Hips_Motion collection
Hips_Data = Hips_Data.select("date", "Time", "AccelerationX", "AccelerationY")  # Adjust columns as needed

Hips_Data.show()

