import pandas as pd
import os

base_directory = 'G:/School/Fall 2024/CPT_S 415/Project/Data/test'

for folder_name in os.listdir(base_directory):
    folder_path = os.path.join(base_directory, folder_name)
    if os.path.isdir(folder_path):  # Check if it is a directory
        for txt_file in os.listdir(folder_path):
            print(txt_file)
            if txt_file.endswith('.txt'):  # Process only TXT files
                input_file_path = os.path.join(folder_path, txt_file)
                output_file_path = os.path.join(folder_path, f"{os.path.splitext(txt_file)[0]}.csv")

                # Read the TXT file into a DataFrame
                df = pd.read_csv(input_file_path, delim_whitespace=True, header=None)
                if "API" in txt_file:
                    df.columns = [
                    "Time", "Ignore1", "Ignore2", "StillConfidence", 
                    "OnFootConfidence", "WalkingConfidence", 
                    "RunningConfidence", "OnBicycleConfidence", 
                    "InVehicleConfidence", "TiltingConfidence", 
                    "UnknownConfidence"
                ]
                elif "Battery" in txt_file:
                    df.columns = ["Time", "Ignore1", "Ignore2",
                                  "Lumix", "Temperature"]
                elif "GPS" in txt_file:
                    num_satellite_positions = df.shape[1] - 4  # Number of satellite position columns
                    df.columns = [
                        "Timestamp", "Device_ID", "Reference_ID", "Count"
                    ] + [f"Satellite_Position_{i+1}" for i in range(num_satellite_positions)]
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
                
               
                # Save the DataFrame to a CSV file
                df.to_csv(output_file_path, index=False)
