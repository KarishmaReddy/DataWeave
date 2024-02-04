from django.urls import path
from .views import get_score

urlpatterns = [
    path('get_score/<str:account_id>/', get_score, name='get_score'),
    # Add other endpoints as needed
]
