from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
import pyspark.sql.functions as F

spark = SparkSession.builder \
        .appName("ActivityRecognition") \
        .config("spark.mongodb.input.uri", "mongodb://localhost:27017/Project_Test.Label") \
        .config("spark.mongodb.output.uri", "mongodb://localhost:27017/Project_Test.Label") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
        .config("spark.driver.memory", "4g") \
        .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:MaxGCPauseMillis=100") \
        .getOrCreate()

### Start of dat set up ###
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

combined_data = get_data()

### Data Preprocessing ###

combined_data = combined_data.dropna()

feature_columns = ["AccelerationX", "AccelerationY", "AccelerationZ", 
                   "MagnetometerX", "MagnetometerY", "MagnetometerZ",
                   "LinearAccelerationX", "LinearAccelerationY", "LinearAccelerationZ"]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(combined_data)

train_data, test_data = data.randomSplit([0.8, 0.2], seed=1234)
###

### End of data set up ###

### Start random forst algorithm ###
def train_decision_tree(train_data):
    dt = DecisionTreeClassifier(featuresCol="features", labelCol="CoarseLabel")
    dt_model = dt.fit(train_data)
    return dt_model

def make_predictions(model, test_data):
    return model.transform(test_data).select("date", "Time", "prediction")


'''
Trains 7 decision trees, using bootstrapping to create a random subset of the training data.
Each bootstrap sample trains a tree and creates test data predictions
Each predicition is stored in a list of df
'''
def random_forest(train_data, test_data, num_trees=7):
    trees = []
    predictions = []

    for i in range(num_trees):
        bootstrap_sample = train_data.sample(withReplacement=True, fraction=1.0, seed=1234)
        tree_model = train_decision_tree(bootstrap_sample)
        trees.append(tree_model)
        tree_predictions = make_predictions(tree_model, test_data).withColumnRenamed("prediction", f"prediction_{i}")
        predictions.append(tree_predictions)
    final_predictions = aggregate_predictions(predictions)

    return final_predictions 

'''
Takes the list of predictions from random_forest and combines them
Outputs a list of the final predictions
That output is what is shown in the output txt file
'''
def aggregate_predictions(predictions):
    combined_predictions = predictions[0]
    for i in range(1, len(predictions)):
        combined_predictions = combined_predictions.join(predictions[i], on=["date", "Time"], how="inner")
    
    prediction_columns = [f"prediction_{i}" for i in range(len(predictions))]
    
    def majority_vote(*preds):
        return max(set(preds), key=preds.count)

    majority_vote_udf = F.udf(majority_vote)
    
    combined_predictions = combined_predictions.withColumn(
        "final_prediction",
        majority_vote_udf(*[combined_predictions[col] for col in prediction_columns])
    )

    return combined_predictions
### End random forst algorithm ###

final_predictions = random_forest(train_data, test_data, num_trees=5)

### Print out results to txt file
final_predictions_list = final_predictions.select("date", "Time", "final_prediction").collect()
with open("final_predictions.txt", "w") as file:
    file.write("date, Time, final_prediction\n")
    for row in final_predictions_list:
        file.write(f"{row['date']}, {row['Time']}, {row['final_prediction']}\n")