import pandas as pd
import os
from config import config

base_directory = config.base_directory

for folder_name in os.listdir(base_directory):
    folder_path = os.path.join(base_directory, folder_name)
    if os.path.isdir(folder_path):  # Check if it is a directory
        for txt_file in os.listdir(folder_path):
            file_names = ["API","Location", "Motion", "Label", "labels_track_food","labels_track_main", "labels_track_road", "labels_track_social",
                          "labels_track_traffic", "labels_track_tunnels"]
            print(txt_file)
            if txt_file.endswith('.txt') and any(name in txt_file for name in file_names):
                input_file_path = os.path.join(folder_path, txt_file)
                if os.path.getsize(input_file_path) == 0:
                    print(f"Skipping empty file: {input_file_path}")
                    continue
                output_file_path = os.path.join(
                    folder_path, f"{os.path.splitext(txt_file)[0]}.csv")

                # Read the TXT file into a DataFrame
                df = pd.read_csv(
                    input_file_path, delim_whitespace=True, header=None)
                if "API" in txt_file:
                    df.columns = ["Time", "Ignore2", "Ignore2",
                                  "StillConfidence", "OnFootConfidence", "WalkingConfidence", "RunningConfidence", "BikeConfidence",
                                  "VehicleConfidence", "TiltingConfidence","UnknownConfidence"]
                elif "Motion" in txt_file:
                    df.columns = ["Time", "AccelerationX", "AccelerationY",
                                  "AccelerationZ", "GyroscopeX", "GyroscopeY", "GyroscopeZ", "MagnetometerX",
                                    "MagnetometerY", "MagnetometerZ", "OrientationW", "OrientationX",
                                    "OrientationY", "OrientationZ", "GravityX", "GravityY", "GravityZ", "LinearAccelerationX",
                                    "LinearAccelerationy", "LinearAccelerationZ", "Pressure", "Altitude", "Temperature"]
                elif "Label" in txt_file:
                    df.columns = ["Time", "CoarseLabel", "FineLabel",
                                    "RoadLabel", "TrafficLabel", "TunnelsLabel", "SocialLabel", "FoodLabel"]
                elif "Location" in txt_file:
                    df.columns = ["Time", "Ignore1", "Ignore2",
                                  "Accuracy", "Latitude", "Longitude", "Altitude"]
                elif "labels_track_food" in txt_file:
                    df.columns = ["StartTime", "EndTime", "Label"]
                elif "labels_track_main" in txt_file:
                    df.columns = ["StartTime", "EndTime", "Activity"]
                elif "labels_track_road" in txt_file:
                    df.columns = ["StartTime", "EndTime", "Label"]
                elif "labels_track_social" in txt_file:
                    df.columns = ["StartTime", "EndTime", "Label"]
                elif "labels_track_traffic" in txt_file:
                    df.columns = ["StartTime", "EndTime", "Label"]
                elif "labels_track_tunnels" in txt_file:
                    df.columns = ["StartTime", "EndTime", "Label"]
                else:
                    continue

                # Save the DataFrame to a CSV file
                df.to_csv(output_file_path, index=False)
