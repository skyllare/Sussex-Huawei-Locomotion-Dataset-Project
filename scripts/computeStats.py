from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, from_unixtime, lit, round, when
from datetime import datetime, timezone

# Start Spark session
spark = SparkSession.builder \
    .appName("ActivityRecognition") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Project.Label") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Project.Label") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.driver.memory", "4g") \
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100") \
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100") \
    .getOrCreate()


Label_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
Label_Data = Label_Data.select("date", "Time", "CoarseLabel", "FineLabel",
                               "RoadLabel", "TrafficLabel", "TunnelsLabel", "SocialLabel", "FoodLabel")

Label_Data.orderBy("Time")

# Function to convert a datetime object to Unix time in milliseconds
def datetime_to_unix_milliseconds(dt):
    unix_time_seconds = int(dt.timestamp())
    unix_time_milliseconds = unix_time_seconds * 1000  # convert to milliseconds
    return unix_time_milliseconds


# Function to convert Unix time in milliseconds to a datetime object
def unix_milliseconds_to_datetime(unix_ms):
    return datetime.fromtimestamp(unix_ms / 1000.0, timezone.utc)


# Function to format time in milliseconds to a human-readable format
def format_duration(milliseconds):
    seconds = milliseconds / 1000.0
    if seconds < 60:
        return f"{seconds:.2f} seconds"
    minutes = seconds / 60.0
    if minutes < 60:
        return f"{minutes:.2f} minutes"
    hours = minutes / 60.0
    return f"{hours:.2f} hours"


# Function to compute statistics for a given date range
def compute_stats(start_time, end_time=None):
    if end_time is None:
        # Convert start_time from milliseconds to a datetime object
        start_datetime = datetime.fromtimestamp(start_time / 1000.0)
        # Set end_time to the end of the day (23:59:59)
        end_datetime = start_datetime.replace(hour=23, minute=59, second=59, microsecond=999000)
        end_time = int(end_datetime.timestamp() * 1000)
        print(f"Checking for times during the day: {start_time} to {end_time}")
    else:
        print(f"Time range: {start_time} to {end_time}")

    time_filter = (col("Time") >= lit(start_time)) & (col("Time") <= lit(end_time))

    # Apply filters
    filtered_data = Label_Data.filter(time_filter)

    # Count total instances in the range
    total_count = filtered_data.count()

    # Debugging: Print the total count
    # print(f"Total count: {total_count}")

    if total_count == 0:
        print("No data found in the specified time range.")
        return

    # Calculate total time in milliseconds
    total_time_ms = total_count * 10  # Each instance represents 10 milliseconds
    readable_time = format_duration(total_time_ms)
    print(f"Total time: {readable_time}")

    # Count instances of each label
    label_distribution = filtered_data.groupBy("CoarseLabel").agg(
        count("CoarseLabel").alias("Count")
    ).withColumn(
        "Percentage", round((col("Count") / total_count) * 100, 2)
    ).withColumn(
        # Each instance represents 10 milliseconds
        "TotalTimeMs", col("Count") * 10
    ).withColumn(
        "Total Time", expr("CASE " +
                                  "WHEN TotalTimeMs < 60000 THEN CONCAT(ROUND(TotalTimeMs / 1000, 2), ' seconds') " +
                                  "WHEN TotalTimeMs < 3600000 THEN CONCAT(ROUND(TotalTimeMs / 60000, 2), ' minutes') " +
                                  "ELSE CONCAT(ROUND(TotalTimeMs / 3600000, 2), ' hours') " +
                                  "END")
    )

    # Add a new column with the label text
    label_distribution = label_distribution.withColumn(
        "Activity",
        when(col("CoarseLabel") == 0, "Still")
        .when(col("CoarseLabel") == 1, "Walking")
        .when(col("CoarseLabel") == 2, "Running")
        .when(col("CoarseLabel") == 3, "Biking")
        .when(col("CoarseLabel") == 4, "Driving")
        .when(col("CoarseLabel") == 5, "On Bus")
        .when(col("CoarseLabel") == 6, "On Train")
        .when(col("CoarseLabel") == 7, "On Subway")
        .otherwise("Unknown")
    )

    label_distribution = label_distribution.orderBy("CoarseLabel")
    label_distribution = label_distribution.select(
        "Activity", "Total Time", "Percentage")
    label_distribution.show()


# Example
# 1: Filter by a date range (3/1/2017 to 7/3/2017)
print("Stats in date range (3/1/2017 to 7/3/2017)")
start_date = datetime(2017, 3, 1, 14, 0, 0)
end_date = datetime(2017, 7, 3, 18, 0, 0)

start_time_unix = datetime_to_unix_milliseconds(start_date)
end_time_unix = datetime_to_unix_milliseconds(end_date)

compute_stats(start_time=start_time_unix, end_time=end_time_unix)

# 2: Filter by single date
print("Stats for 3/1/2017")
compute_stats(start_time=1488376622000)

spark.stop()
