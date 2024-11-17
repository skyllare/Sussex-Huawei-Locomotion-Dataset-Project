from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, lit, from_unixtime, when

coarse_labels = {
    0: "No Label",
    1: "Still",
    2: "Walk",
    3: "Run",
    4: "Bike",
    5: "Car",
    6: "Bus",
    7: "Train",
    8: "Subway",
}

# Start Spark session
spark = SparkSession.builder \
    .appName("ActivityRecognition") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/ProjectSubset.Label") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/ProjectSubset.Label") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100") \
    .getOrCreate()


Label_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
Label_Data = Label_Data.select("date", "Time", "CoarseLabel", "FineLabel",
                               "RoadLabel", "TrafficLabel", "TunnelsLabel", "SocialLabel", "FoodLabel")

# Generate hour column so users can search by day and hour
Label_Data = Label_Data.withColumn(
    "datetime", from_unixtime(col("Time") / 1000))
Label_Data = Label_Data.withColumn("hour", hour(col("datetime")))

# Function to calculate stats for a given range


def compute_stats(start_date=None, end_date=None, start_time=None, end_time=None, specific_label=None):
    # Create filter conditions
    date_filter = lit(True)
    time_filter = lit(True)

    if start_date and end_date:
        print(f"Date range: {start_date} to {end_date}")
        date_filter = (col("date") >= lit(start_date)) & (
            col("date") <= lit(end_date))
    elif start_date:
        print(f"Date: {start_date}")
        date_filter = col("date") == lit(start_date)

    if start_time and end_time:
        print(f"Time range: {start_time} to {end_time}")
        time_filter = (col("Time") >= lit(start_time)) & (
            col("Time") <= lit(end_time))

    # Apply filters
    filtered_data = Label_Data.filter(date_filter & time_filter)

    # Count instances of each label
    if specific_label is not None:
        label_count = filtered_data.filter(col("CoarseLabel") == lit(specific_label)) \
                                   .count()
        print(
            f"Instances of CoarseLabel {coarse_labels[specific_label]}: {label_count}")
    else:
        label_distribution = filtered_data.groupBy("CoarseLabel").count()
        label_distribution.show()

    # Activity distribution by hour
    hourly_distribution = filtered_data.groupBy("hour").count().orderBy("hour")
    print("Activity distribution by hour:")
    hourly_distribution.show()


# Example
# 1: Filter by a date range
compute_stats(start_date="010317", end_date="010617")

# 2: Filter by date and specific label
compute_stats(start_date="010317", specific_label=2)

# 3: Full range with time filtering
compute_stats(start_date="010317", end_date="010617",
              start_time=1488376625470, end_time=1488377925470)

spark.stop()
