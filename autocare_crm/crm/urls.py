from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('customers/', views.customer_list, name='customer_list'),
    path('customers/<int:pk>/', views.customer_detail, name='customer_detail'),
    path('customers/<int:pk>/call/', views.add_call_record, name='add_call_record'),
    path('call-records/', views.call_records, name='call_records'),
    path('call-records/<int:call_id>/delete/', views.delete_call_record, name='delete_call_record'),
    path('upload/', views.upload_data, name='upload_data'),
    path('call-records/follow-up/', views.add_follow_up, name='add_follow_up'),
    path('api/sidebar-stats/', views.sidebar_stats_api, name='sidebar_stats_api'),  # 추가
]