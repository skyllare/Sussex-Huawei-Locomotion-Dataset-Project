from django.urls import path
from . import views

app_name = "dashboard"

urlpatterns = [
    path("", views.home, name="home"),
    path("dashboard", views.home, name="home"),
    path("home/", views.home, name="home"),
    path("activity_recognition/", views.activity_recognition, name="activity_recognition"),
    path("anomaly_detection/", views.anomaly_detection, name="anomaly_detection"),
    path("activity_statistics/", views.activity_statistics, name="activity_statistics"),
    path("location_prediction/", views.location_prediction, name="location_prediction"),
]