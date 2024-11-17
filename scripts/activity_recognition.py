from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt
from pyspark.ml.feature import VectorAssembler

def get_data():
    # the memory may need to be upped for the full dataset
    spark = SparkSession.builder \
        .appName("ActivityRecognition") \
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
    Hips_Data = Hips_Data.select("date", "Time", 
                                 "AccelerationX", "AccelerationY", "AccelerationZ",
                                 "MagnetometerX", "MagnetometerY", "MagnetometerZ",
                                 "LinearAccelerationX", "LinearAccelerationy", "LinearAccelerationZ")  # Adjust columns as needed

    # Join the two datasets on the common columns (e.g., "date" and "Time")
    return Label_Data.join(Hips_Data, on=["date", "Time"], how="inner")

# def build_tree(combined_data):
    

combined_data = get_data()
combined_data = combined_data.withColumn(
    "AccelerationMagnitude", 
    sqrt(col("AccelerationX")**2 + col("AccelerationY")**2 + col("AccelerationZ")**2)
)

combined_data = combined_data.withColumn(
    "MagnetometerMagnitude", 
    sqrt(col("MagnetometerX")**2 + col("MagnetometerY")**2 + col("MagnetometerZ")**2)
)

combined_data = combined_data.withColumn(
    "LinearAccelerationMagnitude", 
    sqrt(col("LinearAccelerationX")**2 + col("LinearAccelerationY")**2 + col("LinearAccelerationZ")**2)
)
combined_data = combined_data.dropna() 

assembler = VectorAssembler(
    inputCols=["AccelerationMagnitude", "MagnetometerMagnitude", "LinearAccelerationMagnitude"],
    outputCol="features"
)

combined_data = assembler.transform(combined_data)
combined_data.show()