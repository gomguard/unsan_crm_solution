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
    path('customers/<int:pk>/approve-do-not-call/', views.approve_do_not_call, name='approve_do_not_call'),
    path('do-not-call-requests/', views.do_not_call_requests, name='do_not_call_requests'),
    # 콜 배정 관련 URL 추가
    path('call-assignment/', views.call_assignment, name='call_assignment'),
    path('my-assignments/', views.my_assignments, name='my_assignments'),
    path('assignment/<int:assignment_id>/update/', views.update_assignment_status, name='update_assignment_status'),
]