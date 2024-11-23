from django.shortcuts import render
from pymongo import MongoClient

# Create your views here.


def index(request):
    return render(request, 'dashboard/index.html')


def charts(request):
    return render(request, 'dashboard/charts.html')


def widgets(request):
    return render(request, 'dashboard/widgets.html')


def tables(request):
    return render(request, "dashboard/tables.html")


def grid(request):
    return render(request, "dashboard/grid.html")


def form_basic(request):
    return render(request, "dashboard/form_basic.html")


def form_wizard(request):
    return render(request, "dashboard/form_wizard.html")


def buttons(request):
    return render(request, "dashboard/buttons.html")


def icon_material(request):
    return render(request, "dashboard/icon-material.html")


def icon_fontawesome(request):
    return render(request, "dashboard/icon-fontawesome.html")


def elements(request):
    return render(request, "dashboard/elements.html")


def gallery(request):
    return render(request, "dashboard/gallery.html")


def invoice(request):
    return render(request, "dashboard/invoice.html")


def chat(request):
    return render(request, "dashboard/chat.html")


def home(request):
    return render(request, "dashboard/home.html")


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
    return render(request, "dashboard/anomaly_detection.html")


def location_prediction(request):
    return render(request, "dashboard/location_prediction.html")


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