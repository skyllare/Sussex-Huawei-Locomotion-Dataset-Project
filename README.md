# Sussex-Huawei-Locomotion-Dataset-Project

**Team Name**: Big Data Energy

**Class**: CPT_S 415 

**Team Members**:  
- Skyllar Estill  
- Caitlin Graves  
- Molly Iverson  
- Sierra Pine  

---

## Project Overview

### Project Statement

We aim to build an **activity analyzer application** that uses data from wearable sensors to deliver a detailed analysis of personal health and fitness, and predict the user's future activities. The application will provide insights into a user’s physical activity by monitoring various metrics and utilizing machine learning for activity recognition and prediction.

### Application Functionalities:

1. **Activity Prediction**:  
   Predict the user's activity based on measurements like acceleration, orientation, linear acceleration, altitude, temperature, and more, using Machine Learning algorithms like Decision Tree and Random Forest.

2. **Location Prediction**:  
   Predict the user's location based on historical GPS data, using a Recurrent Neural Network with an LSTM model.

3. **Anomaly Detection**:  
   Identify unusual patterns or outliers in the dataset using the Isolation Forest machine learning algorithm.

4. **Activity Statistics**:
   Show daily and overall statistics on the duration and distribution of user activities.

---

## Dataset Description

The application leverages the **University of Sussex-Huawei Locomotion and Transportation Dataset (SHL)**, which provides a rich set of multimodal sensor data for investigating users' modes of locomotion and transportation. It is well-suited for machine learning-based activity recognition.

- **Link**: [SHL Dataset](http://www.shl-dataset.org/dataset/)  
- **Number of Files**: 5 zip files  
- **Total Storage Size**: 58 GB  

---

## Programming Tools

- **Programming Languages**: Python, SQL, Matlab
- **Tools**: MongoDB, Apache Spark, Django

---

## Project Setup Instructions

### 1. Clone the Repository

First, clone the repository to your local machine:

```bash
git clone https://github.com/skyllare/Sussex-Huawei-Locomotion-Dataset-Project
cd Sussex-Huawei-Locomotion-Dataset-Project
```

### 2. Download the SHL Dataset

Follow the instructions on the above linked website.

### 3. Configure the config.py File

You'll need to configure the project by setting up a config.py file with the correct directory paths for dataset. Follow the instructions below:

1. Navigate to the /config directory:

```bash
cd config/
```

2. Copy the config_template.py file to config.py:
```bash
cp config/config_template.py config.py
```

3. Update the base_directory variable with the path to your local directory where the release/User1 folder is stored.

### 4. Process and upload the dataset to the MongoDB database using the Python scripts in the scripts folder

Make sure you're in the root folder of the project.

```bash
python -m scripts.fileToCSV
```
```bash
python -m scripts.uploadData
```
```bash
python -m scripts.cleanData
```
```bash
python -m scripts.computeStats
```

### 5. Run the application
* Install Django
```bash
pip install django
```
* Run the app
```bash
python manage.py runserver
```
* Go to ```http://localhost:8000/dashboard/``` in your browser
