from django.urls import path
from .views import *

urlpatterns = [
    path('api/v1/white-space/attribute-list', white_space_attribute_list),
    path('api/v1/white-space/report', white_space_report),
    # path('api/v1/card', metric_card),
    path('api/v1/marketshare/report', market_share)

]
