from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, array, expr, size, greatest
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier

# Spark session initialization
spark = SparkSession.builder \
    .appName("ActivityRecognition") \
    .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Project.Label") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Project.Label") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()

### Start of data setup ###
def get_data():
    Label_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    Label_Data = Label_Data.select("date", "Time", "CoarseLabel")

    Hips_Data = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("spark.mongodb.input.uri", "mongodb://localhost:27017/Project_Test.Hips_Motion") \
        .load()

    Hips_Data = Hips_Data.select("date", "Time", 
                                 "AccelerationX", "AccelerationY", "AccelerationZ",
                                 "MagnetometerX", "MagnetometerY", "MagnetometerZ",
                                 "LinearAccelerationX", "LinearAccelerationY", "LinearAccelerationZ")

    return Label_Data.join(Hips_Data, on=["date", "Time"], how="inner")

combined_data = get_data().dropna()

### Feature preparation ###
combined_data = combined_data.dropna()
feature_columns = ["AccelerationX", "AccelerationY", "AccelerationZ", 
                   "MagnetometerX", "MagnetometerY", "MagnetometerZ",
                   "LinearAccelerationX", "LinearAccelerationY", "LinearAccelerationZ"]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(combined_data)

train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)

### Train decision tree ###
def train_decision_tree(train_data):
    dt = DecisionTreeClassifier(featuresCol="features", labelCol="CoarseLabel")
    return dt.fit(train_data)

def make_predictions(model, test_data):
    return model.transform(test_data).select("date", "Time", "prediction")

### Random forest implementation ###
'''
Trains 7 decision trees, using bootstrapping to create a random subset of the training data.
Each bootstrap sample trains a tree and creates test data predictions
Each predicition is stored in a list of df
'''
def random_forest(train_data, test_data, num_trees=5):
    predictions = []
    
    for i in range(num_trees):
        # Bootstrap sampling
        bootstrap_sample = train_data.sample(withReplacement=True, fraction=1.0, seed=i)
        tree_model = train_decision_tree(bootstrap_sample)
        tree_predictions = make_predictions(tree_model, test_data) \
            .withColumnRenamed("prediction", f"prediction_{i}")
        predictions.append(tree_predictions)
    
    # Aggregate predictions
    combined_predictions = predictions[0]
    for i in range(1, len(predictions)):
        combined_predictions = combined_predictions.join(predictions[i], on=["date", "Time"], how="inner")
    
    prediction_cols = [f"prediction_{i}" for i in range(num_trees)]
    combined_predictions = combined_predictions.withColumn(
        "final_prediction", expr(f"array({', '.join(prediction_cols)})")
    ).withColumn("final_prediction", expr("aggregate(final_prediction, 0, (acc, x) -> acc + x)"))

    return combined_predictions

### Run the model ###
final_predictions = random_forest(train_data, test_data, num_trees=5)

# Write final predictions to a file
final_predictions_list = final_predictions.select("date", "Time", "final_prediction").collect()
with open("final_predictions.txt", "w") as file:
    file.write("date, Time, final_prediction\n")
    for row in final_predictions_list:
        file.write(f"{row['date']}, {row['Time']}, {row['final_prediction']}\n")
