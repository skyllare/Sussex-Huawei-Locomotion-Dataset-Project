from django.shortcuts import render
from pymongo import MongoClient
from django.conf import settings
import datetime
import json
import os



def home(request):
    return render(request, "dashboard/home.html")


def activity_recognition(request):
    client = MongoClient("mongodb://localhost:27017/")
    db = client.get_database("Project")

    dataset_output = list(db["decision_tree_output"].find({}, {"_id": 0}))

    return render(
        request,
        "dashboard/activity_recognition.html",
        {
            "dataset_output": dataset_output,
        },
    )


def anomaly_detection(request):
    image_directory = os.path.join(settings.STATICFILES_DIRS[0], 'images/anomalies')
    image_files = [f for f in os.listdir(image_directory) if f.endswith('.png')]

    plot_urls = {f.replace('_', ' ').replace('.png', '').title(): f"/static/images/anomalies/{f}" for f in image_files}

    return render(request, "dashboard/anomaly_detection.html", {"plot_urls": plot_urls})


def location_prediction(request):
    # Replace with your actual Google API Key as detailed in the README
    GOOGLE_API_KEY = 'PUT_YOUR_FULL_GOOGLE_API_KEY_HERE'
    
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.get_database("Project")

    # Get the string for what today's date is
    today_date = datetime.datetime.now().date()
    day = today_date.day
    suffix = 'th' if 11 <= day <= 13 else {1: 'st', 2: 'nd', 3: 'rd'}.get(day % 10, 'th')
    formatted_date = today_date.strftime(f"%B {day}{suffix}, %Y")
        
    # Fetch the predicted locations for today's date
    predicted_locations_cursor = db["Predicted_Locations"].find()
    predicted_locations = list(predicted_locations_cursor)

    client.close()
    
    predicted_locations.sort(key=lambda x: x['half_hour_interval'])
    
    # Get the correct display time from the half hour interval
    def half_hour_interval_to_time(hhi):
        total_minutes = hhi * 30
        hours = total_minutes // 60
        minutes = total_minutes % 60
        # Create time string with AM and PM
        formatted_time = f"{(hours % 12) or 12}:{minutes:02d} {'AM' if hours < 12 else 'PM'}"
        return formatted_time
    
    # Ensure there are predicted locations
    if not predicted_locations:
        print("No predicted locations found in MongoDB for today.")
        # Use sample locations or handle this case as needed
        sample_locations = [
            {
                'lat': 50.8231 + i * 0.01,
                'lng': -0.1383 + i * 0.01,
                'half_hour_interval': i,
                'time': half_hour_interval_to_time(i),
            }
            for i in range(48)
        ]
        locations_by_interval = {
            loc['half_hour_interval']: loc
            for loc in sample_locations
        }
    else:
        # Create a mapping of half_hour_interval to locations
        locations_by_interval = {}
        for loc in predicted_locations:
            hhi = loc['half_hour_interval']
            time_str = half_hour_interval_to_time(hhi)
            locations_by_interval[hhi] = {
                'lat': loc['Predicted_Latitude'],
                'lng': loc['Predicted_Longitude'],
                'half_hour_interval': hhi,
                'time': time_str,
            }
    
    # Get current time and calculate the next location
    now = datetime.datetime.now()
    half_hour_index = (now.hour * 2) + (now.minute // 30)
    next_location = locations_by_interval.get(half_hour_index)
    
    # If next location is none inserts dummy info
    if next_location is None:
        print(f"No predicted location for half-hour interval {half_hour_index}. Using default location.")
        next_location = {
            'lat': 50.8231,
            'lng': -0.1383,
            'half_hour_interval': half_hour_index,
            'time': 'N/A',
        }
    
    # Prepare locations list for the template
    locations_list = list(locations_by_interval.values())
    
    context = {
        'google_api_key': GOOGLE_API_KEY,
        'locations': json.dumps(locations_list),
        'next_location': json.dumps(next_location),
        'formatted_date': formatted_date,
    }
    
    return render(request, 'dashboard/location_prediction.html', context)


def activity_statistics(request):
    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client.get_database("Project")

    # Fetch dataset-wide statistics
    dataset_stats = list(db["dataset_stats"].find({}, {"_id": 0}))

    # Fetch all distinct dates for dropdown
    daily_stats_dates = db["daily_stats"].distinct("date")

    # If a date is selected, fetch daily stats
    selected_date = request.GET.get("date", None)
    daily_stats = []
    if selected_date:
        daily_stats = list(
            db["daily_stats"].find({"date": selected_date}, {"_id": 0})
        )

    return render(
        request,
        "dashboard/activity_statistics.html",
        {
            "dataset_stats": dataset_stats,
            "daily_stats": daily_stats,
            "daily_stats_dates": daily_stats_dates,
            "selected_date": selected_date,
        },
    )