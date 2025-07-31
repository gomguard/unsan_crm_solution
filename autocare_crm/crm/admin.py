from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from django.utils.html import format_html
from django.urls import reverse
from .models import Customer, CallRecord, UploadHistory, UserProfile, CallFollowUp, CallAssignment


# UserProfile을 User와 함께 표시하기 위한 Inline
class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False
    verbose_name_plural = '사용자 프로필'
    fk_name = 'user'

# User Admin 확장
class UserAdmin(BaseUserAdmin):
    inlines = (UserProfileInline,)
    list_display = ('username', 'email', 'first_name', 'last_name', 'is_staff', 'get_role', 'get_team', 'password_change_link')
    
    def get_role(self, obj):
        if hasattr(obj, 'userprofile'):
            return obj.userprofile.get_role_display()
        return '-'
    get_role.short_description = '역할'
    
    def get_team(self, obj):
        if hasattr(obj, 'userprofile'):
            return obj.userprofile.team or '-'
        return '-'
    get_team.short_description = '팀'
    
    def password_change_link(self, obj):
        """비밀번호 변경 링크 추가"""
        try:
            # Django 버전에 따라 URL 패턴이 다를 수 있음
            url = f'/admin/auth/user/{obj.pk}/password/'
            return format_html('<a href="{}">비밀번호 변경</a>', url)
        except:
            return '-'
    password_change_link.short_description = '비밀번호'
    
    # 비밀번호 변경 권한 추가
    def has_change_permission(self, request, obj=None):
        return super().has_change_permission(request, obj)

# 기존 User Admin 제거하고 새로운 것으로 등록
admin.site.unregister(User)
admin.site.register(User, UserAdmin)

# UserProfile 별도 Admin (선택사항)
@admin.register(UserProfile)
class UserProfileAdmin(admin.ModelAdmin):
    list_display = ('user', 'role', 'team', 'daily_call_target')
    list_filter = ('role', 'team')
    search_fields = ('user__username', 'user__email', 'team')

# Customer Admin
@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    list_display = ('name', 'phone', 'vehicle_number', 'status', 'customer_grade', 'inspection_expiry_date', 'is_do_not_call')
    list_filter = ('status', 'customer_grade', 'is_do_not_call', 'is_inspection_overdue', 'is_frequent_visitor')
    search_fields = ('name', 'phone', 'vehicle_number')
    date_hierarchy = 'inspection_expiry_date'
    
    fieldsets = (
        ('기본 정보', {
            'fields': ('name', 'phone', 'landline', 'email', 'address', 'postal_code')
        }),
        ('차량 정보', {
            'fields': ('vehicle_number', 'vehicle_name', 'vehicle_model', 'vehicle_registration_date', 'chassis_number')
        }),
        ('검사/보험 정보', {
            'fields': ('inspection_expiry_date', 'insurance_expiry_date', 'oil_change_date')
        }),
        ('고객 관리', {
            'fields': ('status', 'customer_grade', 'visit_count', 'priority')
        }),
        ('통화 금지', {
            'fields': ('is_do_not_call', 'do_not_call_reason', 'do_not_call_date')
        }),
    )

# CallRecord Admin
@admin.register(CallRecord)
class CallRecordAdmin(admin.ModelAdmin):
    list_display = ('customer', 'caller', 'call_date', 'call_result', 'interest_type', 'requires_follow_up', 'is_deleted')
    list_filter = ('call_result', 'interest_type', 'requires_follow_up', 'is_deleted', 'call_date')
    search_fields = ('customer__name', 'customer__phone', 'caller__username', 'notes')
    date_hierarchy = 'call_date'
    
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('customer', 'caller')

# UploadHistory Admin
@admin.register(UploadHistory)
class UploadHistoryAdmin(admin.ModelAdmin):
    list_display = ('file_name', 'uploaded_by', 'upload_date', 'total_records', 'new_records', 'updated_records', 'error_count')
    list_filter = ('upload_date', 'uploaded_by')
    search_fields = ('file_name', 'notes')
    date_hierarchy = 'upload_date'

# CallFollowUp Admin
@admin.register(CallFollowUp)
class CallFollowUpAdmin(admin.ModelAdmin):
    list_display = ('call_record', 'created_by', 'created_at', 'action_type', 'scheduled_date')
    list_filter = ('action_type', 'created_at', 'scheduled_date')
    search_fields = ('call_record__customer__name', 'created_by__username', 'notes')
    date_hierarchy = 'created_at'

# CallAssignment Admin
@admin.register(CallAssignment)
class CallAssignmentAdmin(admin.ModelAdmin):
    list_display = ('customer', 'assigned_to', 'assigned_by', 'assigned_at', 'priority', 'status', 'due_date')
    list_filter = ('status', 'priority', 'assigned_at', 'due_date')
    search_fields = ('customer__name', 'customer__phone', 'assigned_to__username', 'notes')
    date_hierarchy = 'assigned_at'
    
    def get_queryset(self, request):
        qs = super().get_queryset(request)
        return qs.select_related('customer', 'assigned_to', 'assigned_by')