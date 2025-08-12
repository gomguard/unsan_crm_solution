# crm/views.py
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.core.paginator import Paginator
from django.utils import timezone
from django.db import transaction
from datetime import datetime, timedelta, date
import csv
import io
import re
import pandas as pd
from django.http import JsonResponse
import json
from django.contrib.auth.models import User

from .models import Customer, CallRecord, UploadHistory, UserProfile, CallFollowUp, CallAssignment
from .forms import CallRecordForm, CustomerUploadForm
from .decorators import manager_required, admin_required, ajax_manager_required
from django.db.models import Q, Count, Prefetch
from django.db import transaction

def get_sidebar_stats():
    """사이드바에 표시할 통계 정보 계산"""
    today = timezone.now().date()
    
    # 오늘 통화 수
    sidebar_today_calls = CallRecord.objects.filter(
        call_date__date=today,
        is_deleted=False
    ).count()
    
    # 미완료 후속조치
    sidebar_pending_followups = CallRecord.objects.filter(
        requires_follow_up=True,
        follow_up_completed=False,
        is_deleted=False
    ).count()
    
    # 검사만료 고객
    sidebar_overdue_customers = Customer.objects.filter(
        inspection_expiry_date__isnull=False,
        inspection_expiry_date__lt=today
    ).count()
    
    return {
        'sidebar_today_calls': sidebar_today_calls,
        'sidebar_pending_followups': sidebar_pending_followups,
        'sidebar_overdue_customers': sidebar_overdue_customers,
    }

@login_required
def dashboard(request):
    """대시보드 - 실시간 통계"""
    today = timezone.now().date()
    
    # 사이드바 통계를 먼저 가져오기
    sidebar_stats = get_sidebar_stats()
    
    # 오늘 통계
    today_calls = CallRecord.objects.filter(call_date__date=today, is_deleted=False)
    today_connected = today_calls.filter(call_result='connected')
    
    # 전체 통계
    total_customers = Customer.objects.count()
    pending_customers = Customer.objects.filter(status='pending').count()
    interested_customers = Customer.objects.filter(status='interested').count()
    converted_customers = Customer.objects.filter(status='converted').count()
    
    # 검사 관련 통계
    three_months_later = today + timedelta(days=90)
    
    due_soon_customers = Customer.objects.filter(
        inspection_expiry_date__isnull=False,
        inspection_expiry_date__gte=today,
        inspection_expiry_date__lte=three_months_later
    ).count()
    
    # 실제 검사일 기준 해피콜 대상 계산
    # 3개월콜 대상 (실제 검사일 기준)
    three_months_ago = today - timedelta(days=90)
    happy_call_3month_total = Customer.objects.filter(
        actual_inspection_date__gte=three_months_ago - timedelta(days=7),
        actual_inspection_date__lte=three_months_ago + timedelta(days=7)
    ).count()

    # 6개월콜 대상 (실제 검사일 기준)
    six_months_ago = today - timedelta(days=180)
    happy_call_6month_total = Customer.objects.filter(
        actual_inspection_date__gte=six_months_ago - timedelta(days=7),
        actual_inspection_date__lte=six_months_ago + timedelta(days=7)
    ).count()

    # 12개월콜 대상 (실제 검사일 기준)
    twelve_months_ago = today - timedelta(days=365)
    happy_call_12month_total = Customer.objects.filter(
        actual_inspection_date__gte=twelve_months_ago - timedelta(days=7),
        actual_inspection_date__lte=twelve_months_ago + timedelta(days=7)
    ).count()

    # 18개월콜 대상 (실제 검사일 기준) - 선택사항
    eighteen_months_ago = today - timedelta(days=548)
    happy_call_18month_total = Customer.objects.filter(
        actual_inspection_date__gte=eighteen_months_ago - timedelta(days=7),
        actual_inspection_date__lte=eighteen_months_ago + timedelta(days=7)
    ).count()

    # 오늘 통화한 고객 ID 목록
    today_called_customer_ids = today_calls.values_list('customer_id', flat=True).distinct()

    # 각 해피콜의 남은 대상자 계산 (실제 검사일 기준)
    happy_call_3month_remaining = Customer.objects.filter(
        actual_inspection_date__gte=three_months_ago - timedelta(days=7),
        actual_inspection_date__lte=three_months_ago + timedelta(days=7)
    ).exclude(id__in=today_called_customer_ids).count()

    happy_call_6month_remaining = Customer.objects.filter(
        actual_inspection_date__gte=six_months_ago - timedelta(days=7),
        actual_inspection_date__lte=six_months_ago + timedelta(days=7)
    ).exclude(id__in=today_called_customer_ids).count()

    happy_call_12month_remaining = Customer.objects.filter(
        actual_inspection_date__gte=twelve_months_ago - timedelta(days=7),
        actual_inspection_date__lte=twelve_months_ago + timedelta(days=7)
    ).exclude(id__in=today_called_customer_ids).count()

    happy_call_18month_remaining = Customer.objects.filter(
        actual_inspection_date__gte=eighteen_months_ago - timedelta(days=7),
        actual_inspection_date__lte=eighteen_months_ago + timedelta(days=7)
    ).exclude(id__in=today_called_customer_ids).count()

    # 완료된 수 계산
    happy_call_3month_completed = happy_call_3month_total - happy_call_3month_remaining
    happy_call_6month_completed = happy_call_6month_total - happy_call_6month_remaining
    happy_call_12month_completed = happy_call_12month_total - happy_call_12month_remaining
    happy_call_18month_completed = happy_call_18month_total - happy_call_18month_remaining

    # 검사만료 고객 (오늘 통화 현황)
    overdue_customers_total = Customer.objects.filter(
        inspection_expiry_date__isnull=False,
        inspection_expiry_date__lt=today
    ).count()
    overdue_customers_remaining = Customer.objects.filter(
        inspection_expiry_date__isnull=False,
        inspection_expiry_date__lt=today
    ).exclude(id__in=today_called_customer_ids).count()
    overdue_customers_completed = overdue_customers_total - overdue_customers_remaining
    
    # 재방문 고객 (2회 이상 방문, 오늘 통화 현황)
    returning_customers_total = Customer.objects.filter(visit_count__gte=2).count()
    returning_customers_remaining = Customer.objects.filter(
        visit_count__gte=2
    ).exclude(id__in=today_called_customer_ids).count()
    returning_customers_completed = returning_customers_total - returning_customers_remaining
    
    # VIP 고객
    vip_customers_total = Customer.objects.filter(customer_grade='vip').count()
    vip_customers_remaining = Customer.objects.filter(
        customer_grade='vip'
    ).exclude(id__in=today_called_customer_ids).count()
    vip_customers_completed = vip_customers_total - vip_customers_remaining
    
    # 오늘의 통화 대상자 통계
    happy_call_targets = (
        happy_call_3month_remaining + 
        happy_call_6month_remaining + 
        happy_call_12month_remaining + 
        happy_call_18month_remaining
    )
    
    # 검사만료 + 재방문 고객 (오늘 통화하지 않은)
    priority_targets = overdue_customers_remaining + returning_customers_remaining
    
    # 전체 오늘 통화 대상 (단순 합계)
    today_total_targets = happy_call_targets + priority_targets
    
    # 오늘 실제 통화한 건수
    today_total_calls_count = today_calls.count()
    
    # 통화 완료된 대상자 수 (중복 제거)
    today_completed_targets = len(today_called_customer_ids)
    
    # 달성률 계산 (실제 통화 건수 / 목표)
    today_target_completion_rate = round((today_completed_targets / today_total_targets * 100) if today_total_targets > 0 else 0)
    
    # 이탈 고객 통계
    first_time_lost = Customer.objects.filter(is_first_time_no_return=True).count()
    long_term_lost = Customer.objects.filter(
        Q(is_long_term_absent=True) | 
        Q(customer_status='possibly_scrapped')
    ).count()
    
    # 단골 고객 수
    frequent_visitors = Customer.objects.filter(visit_count__gte=3).count()
    
    # 후속조치 관련 통계
    followup_required_total = CallRecord.objects.filter(
        requires_follow_up=True,
        is_deleted=False
    ).count()
    
    followup_completed_total = CallRecord.objects.filter(
        requires_follow_up=True,
        follow_up_completed=True,
        is_deleted=False
    ).count()
    
    followup_pending = followup_required_total - followup_completed_total
    
    followup_due_today = CallRecord.objects.filter(
        follow_up_date=today,
        follow_up_completed=False,
        is_deleted=False
    ).count()
    
    followup_overdue = CallRecord.objects.filter(
        follow_up_date__lt=today,
        follow_up_completed=False,
        requires_follow_up=True,
        is_deleted=False
    ).count()
    
    followup_calls_today = CallRecord.objects.filter(
        parent_call__isnull=False,
        call_date__date=today,
        is_deleted=False
    ).count()
    
    followup_completion_rate = 0
    if followup_required_total > 0:
        followup_completion_rate = round(
            (followup_completed_total / followup_required_total) * 100, 1
        )
    
    pending_followup_list = CallRecord.objects.filter(
        requires_follow_up=True,
        follow_up_completed=False,
        is_deleted=False
    ).select_related('customer', 'caller').order_by('follow_up_date')[:10]
    
    # 최근 통화 기록
    recent_calls = CallRecord.objects.filter(
        is_deleted=False
    ).select_related('customer', 'caller').order_by('-call_date')[:10]
    
    # 상담원별 오늘 성과 (팀장만)
    agent_stats = None
    try:
        if hasattr(request.user, 'userprofile') and request.user.userprofile.role == 'manager':
            agent_stats = CallRecord.objects.filter(
                call_date__date=today,
                is_deleted=False
            ).values(
                'caller__username'
            ).annotate(
                total_calls=Count('id'),
                connected_calls=Count('id', filter=Q(call_result='connected'))
            )
    except:
        pass
    
    # 오늘의 목표별 통화 수
    today_overdue_calls = today_calls.filter(
        customer__inspection_expiry_date__isnull=False,
        customer__inspection_expiry_date__lt=today
    ).count()
    
    today_due_soon_calls = today_calls.filter(
        customer__inspection_expiry_date__isnull=False,
        customer__inspection_expiry_date__gte=today,
        customer__inspection_expiry_date__lte=three_months_later
    ).count()
    
    today_vip_calls = today_calls.filter(
        customer__customer_grade='vip'
    ).count()
    
    # Context 생성
    context = {
        'total_customers': total_customers,
        'pending_customers': pending_customers,
        'interested_customers': interested_customers,
        'converted_customers': converted_customers,
        
        # 오늘의 통화 목표
        'today_targets': {
            'total': today_total_targets,
            'remaining': today_total_targets - today_completed_targets,
            'completed': today_completed_targets,
            'completion_rate': today_target_completion_rate,
            'actual_calls': today_total_calls_count
        },
        
        # 해피콜 통계
        'happy_call_3month': {
            'total': happy_call_3month_total,
            'remaining': happy_call_3month_remaining,
            'completed': happy_call_3month_completed,
            'progress': round((happy_call_3month_completed / happy_call_3month_total * 100) if happy_call_3month_total > 0 else 0)
        },
        'happy_call_6month': {
            'total': happy_call_6month_total,
            'remaining': happy_call_6month_remaining,
            'completed': happy_call_6month_completed,
            'progress': round((happy_call_6month_completed / happy_call_6month_total * 100) if happy_call_6month_total > 0 else 0)
        },
        'happy_call_12month': {
            'total': happy_call_12month_total,
            'remaining': happy_call_12month_remaining,
            'completed': happy_call_12month_completed,
            'progress': round((happy_call_12month_completed / happy_call_12month_total * 100) if happy_call_12month_total > 0 else 0)
        },
        'happy_call_18month': {
            'total': happy_call_18month_total,
            'remaining': happy_call_18month_remaining,
            'completed': happy_call_18month_completed,
            'progress': round((happy_call_18month_completed / happy_call_18month_total * 100) if happy_call_18month_total > 0 else 0)
        },
        'overdue_customers': {
            'total': overdue_customers_total,
            'remaining': overdue_customers_remaining,
            'completed': overdue_customers_completed,
            'progress': round((overdue_customers_completed / overdue_customers_total * 100) if overdue_customers_total > 0 else 0)
        },
        'returning_customers': {
            'total': returning_customers_total,
            'remaining': returning_customers_remaining,
            'completed': returning_customers_completed,
            'progress': round((returning_customers_completed / returning_customers_total * 100) if returning_customers_total > 0 else 0)
        },
        
        # 기타 통계
        'first_time_lost': first_time_lost,
        'long_term_lost': long_term_lost,
        'frequent_visitors': frequent_visitors,
        'due_soon_customers': due_soon_customers,
        'today_total_calls': today_calls.count(),
        'today_connected_calls': today_connected.count(),
        
        'recent_calls': recent_calls,
        'agent_stats': agent_stats,
        'today_overdue_calls': today_overdue_calls,
        'today_due_soon_calls': today_due_soon_calls,
        'today_vip_calls': today_vip_calls,
        
        # 후속조치 관련
        'followup_pending': followup_pending,
        'followup_completed_today': followup_calls_today,
        'followup_due_today': followup_due_today,
        'followup_overdue': followup_overdue,
        'followup_completion_rate': followup_completion_rate,
        'pending_followup_list': pending_followup_list,
        
        # 호환성을 위한 추가 변수
        'pending_follow_ups': followup_pending,
        'completed_follow_ups': followup_calls_today,
        'today_follow_ups': followup_due_today,
        'overdue_follow_ups': followup_overdue,
        'follow_up_completion_rate': followup_completion_rate,
        'pending_follow_up_records': pending_followup_list,
    }
    
    # 사이드바 통계 추가
    context.update(sidebar_stats)
    
    return render(request, 'dashboard.html', context)


@login_required
def customer_list(request):
    """고객 목록 - 검색, 필터링, 페이징"""
    # 권한별 고객 필터링 추가
    if hasattr(request.user, 'userprofile') and request.user.userprofile.role == 'agent':
        # 상담원은 본인에게 배정된 고객 + 본인이 통화한 고객
        assigned_customer_ids = CallAssignment.objects.filter(
            assigned_to=request.user,
            status__in=['pending', 'in_progress']
        ).values_list('customer_id', flat=True)
        
        customers = Customer.objects.filter(
            Q(id__in=assigned_customer_ids) |
            Q(call_records__caller=request.user)
        ).distinct()
    else:
        # 팀장, 관리자는 전체 고객
        customers = Customer.objects.all()
    
    # 검색
    search_query = request.GET.get('search', '')
    if search_query:
        customers = customers.filter(
            Q(name__icontains=search_query) |
            Q(phone__icontains=search_query) |
            Q(vehicle_number__icontains=search_query)
        )
    
    # 상태 필터
    status_filter = request.GET.get('status', '')
    if status_filter:
        customers = customers.filter(status=status_filter)
    
    # 우선순위 필터
    priority_filter = request.GET.get('priority', '')
    if priority_filter == 'overdue':
        customers = customers.filter(inspection_expiry_date__lt=timezone.now().date())
    elif priority_filter == 'due_soon':
        today = timezone.now().date()
        three_months_later = today + timedelta(days=90)
        customers = customers.filter(
            inspection_expiry_date__gte=today,
            inspection_expiry_date__lte=three_months_later
        )
    elif priority_filter == 'high':
        customers = customers.filter(priority='high')
    
    # 해피콜 필터 (실제 검사일 기준으로 수정 - ±1일)
    happy_call_filter = request.GET.get('happy_call', '')
    if happy_call_filter:
        today = timezone.now().date()
        
        if happy_call_filter == '3month':
            three_months_ago = today - timedelta(days=90)
            customers = customers.filter(
                actual_inspection_date__gte=three_months_ago - timedelta(days=1),
                actual_inspection_date__lte=three_months_ago + timedelta(days=1)
            )
        elif happy_call_filter == '6month':
            six_months_ago = today - timedelta(days=180)
            customers = customers.filter(
                actual_inspection_date__gte=six_months_ago - timedelta(days=1),
                actual_inspection_date__lte=six_months_ago + timedelta(days=1)
            )
        elif happy_call_filter == '12month':
            twelve_months_ago = today - timedelta(days=365)
            customers = customers.filter(
                actual_inspection_date__gte=twelve_months_ago - timedelta(days=1),
                actual_inspection_date__lte=twelve_months_ago + timedelta(days=1)
            )
        elif happy_call_filter == '18month':
            eighteen_months_ago = today - timedelta(days=548)
            customers = customers.filter(
                actual_inspection_date__gte=eighteen_months_ago - timedelta(days=1),
                actual_inspection_date__lte=eighteen_months_ago + timedelta(days=1)
            )
    
    # 고객등급 필터
    grade_filter = request.GET.get('grade', '')
    if grade_filter:
        customers = customers.filter(customer_grade=grade_filter)
    
    # 재방문 고객 필터
    visit_count_filter = request.GET.get('visit_count', '')
    if visit_count_filter:
        try:
            min_visits = int(visit_count_filter)
            customers = customers.filter(visit_count__gte=min_visits)
        except ValueError:
            pass
    
    # 단골고객 필터
    frequent_filter = request.GET.get('frequent', '')
    if frequent_filter == 'true':
        customers = customers.filter(visit_count__gte=3)
    
    # 검사 임박 필터
    inspection_due = request.GET.get('inspection_due', '')
    if inspection_due == 'true':
        today = timezone.now().date()
        three_months_later = today + timedelta(days=90)
        customers = customers.filter(
            inspection_expiry_date__lte=three_months_later
        )
    
    # 정렬 - 기본값은 실제 검사일 오래된 순
    # actual_inspection_date로 정렬 (NULL 값은 뒤로)
    from django.db.models import F
    customers = customers.order_by(
        F('actual_inspection_date').asc(nulls_last=True),
        F('inspection_expiry_date').asc(nulls_last=True)
    )
    
    # 페이징
    paginator = Paginator(customers, 50)
    page_number = request.GET.get('page')
    customers = paginator.get_page(page_number)
    
    # 사이드바 통계 추가
    sidebar_stats = get_sidebar_stats()
    
    context = {
        'customers': customers,
        'search_query': search_query,
        'status_filter': status_filter,
        'inspection_due': inspection_due,
        'happy_call_filter': happy_call_filter,
        'visit_count_filter': visit_count_filter,
        'status_choices': Customer.STATUS_CHOICES,
        'today': timezone.now().date(),
    }
    context.update(sidebar_stats)
    
    return render(request, 'customer_list.html', context)

@login_required
def customer_detail(request, pk):
    """고객 상세 정보"""
    customer = get_object_or_404(Customer, pk=pk)
    # 삭제되지 않은 통화 기록 중 부모 통화가 없는 것만 가져오기 (후속조치 제외)
    call_records = customer.call_records.filter(
        is_deleted=False,
        parent_call__isnull=True
    ).order_by('-call_date')[:20]
    
    # 사이드바 통계 추가
    sidebar_stats = get_sidebar_stats()
    
    context = {
        'customer': customer,
        'call_records': call_records,
        'today': timezone.now().date(),
    }
    context.update(sidebar_stats)
    
    return render(request, 'customer_detail.html', context)


@login_required
def delete_call_record(request, call_id):
    """통화 기록 소프트 삭제"""
    if request.method != 'POST':
        return JsonResponse({
            'success': False, 
            'error': 'POST 방식만 허용됩니다.'
        })
    
    try:
        call_record = get_object_or_404(CallRecord, id=call_id, is_deleted=False)
        
        # 권한 체크
        if not call_record.can_delete(request.user):
            return JsonResponse({
                'success': False, 
                'error': '삭제 권한이 없습니다.'
            })
        
        # 소프트 삭제 실행
        call_record.soft_delete(request.user)
        
        return JsonResponse({
            'success': True,
            'message': '통화 기록이 삭제되었습니다.'
        })
        
    except CallRecord.DoesNotExist:
        return JsonResponse({
            'success': False, 
            'error': '통화 기록을 찾을 수 없습니다.'
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': f'삭제 중 오류가 발생했습니다: {str(e)}'
        })


@login_required
def add_call_record(request, pk):
    """통화 기록 추가 (AJAX 지원) - 개선된 후속조치 처리"""
    customer = get_object_or_404(Customer, pk=pk)
    # 통화 금지 고객 체크 추가
    if customer.is_do_not_call and not request.user.is_staff:
        return JsonResponse({
            'success': False,
            'error': '통화 금지 고객입니다. 관리자에게 문의하세요.'
        })
    
    if request.method == 'POST':
        try:
            # 폼 데이터 처리
            call_result = request.POST.get('call_result')
            interest_type = request.POST.get('interest_type')
            notes = request.POST.get('notes', '').strip()
            
            # 유효성 검사
            if not call_result:
                return JsonResponse({
                    'success': False, 
                    'error': '통화 상태를 선택해주세요.'
                })
            
            if not notes:
                return JsonResponse({
                    'success': False, 
                    'error': '통화 내용을 입력해주세요.'
                })
            
            # 트랜잭션으로 묶어서 처리
            with transaction.atomic():
                # 1. 새 통화 기록 생성
                call_record = CallRecord.objects.create(
                    customer=customer,
                    caller=request.user,
                    call_result=call_result,
                    interest_type=interest_type if interest_type else None,
                    notes=notes,
                    customer_attitude=request.POST.get('customer_attitude') or None,
                    requires_follow_up=request.POST.get('requires_follow_up') == 'on',
                    follow_up_date=request.POST.get('follow_up_date') or None,
                    follow_up_notes=request.POST.get('follow_up_memo', ''),
                    parent_call_id=request.POST.get('parent_call_id') or None
                )
                
                # 2. 후속조치 완료 처리 로직
                # 2-1. 명시적 후속조치 (parent_call이 지정된 경우)
                if call_record.parent_call_id:
                    try:
                        parent = CallRecord.objects.select_for_update().get(
                            id=call_record.parent_call_id
                        )
                        parent.follow_up_completed = True
                        parent.save()
                        
                        # 디버그 로그
                        print(f"✅ 후속조치 완료 처리: 원통화 ID {parent.id} (고객: {parent.customer.name})")
                        
                    except CallRecord.DoesNotExist:
                        print(f"❌ 원통화를 찾을 수 없음: ID {call_record.parent_call_id}")
                
                # 2-2. 암시적 후속조치 (같은 고객의 예정된 후속조치 자동 완료)
                elif call_record.call_result == 'connected':
                    today = timezone.now().date()
                    
                    # 이 고객의 미완료 후속조치들 찾기
                    pending_followups = CallRecord.objects.select_for_update().filter(
                        customer=customer,
                        requires_follow_up=True,
                        follow_up_completed=False,
                        is_deleted=False
                    ).exclude(id=call_record.id)  # 방금 생성한 기록 제외
                    
                    # 찾은 후속조치들 완료 처리
                    if pending_followups.exists():
                        count = pending_followups.count()
                        updated = pending_followups.update(follow_up_completed=True)
                        print(f"✅ 총 {updated}건의 후속조치 자동 완료 처리 (쿼리 결과: {count}건)")
                
                # 3. 고객 상태 업데이트
                if call_record.call_result == 'connected':
                    # 관심 분야에 따른 상태 설정
                    if call_record.interest_type == 'none':
                        customer.status = 'not_interested'
                    elif call_record.interest_type in ['insurance', 'maintenance', 'financing', 'multiple']:
                        customer.status = 'interested'
                    else:
                        customer.status = 'contacted'
                                        
                    customer.save()
                    print(f"고객 상태 업데이트: {customer.name} → {customer.status}")
                
                if request.POST.get('request_do_not_call') == 'on' and not customer.is_do_not_call:
                    if request.user.userprofile.role == 'agent':
                        # 상담원은 요청만
                        customer.do_not_call_requested = True
                        customer.do_not_call_requested_by = request.user
                        customer.do_not_call_request_date = timezone.now()
                        customer.save()
                        
                        # 통화 기록에 메모 추가
                        call_record.notes += "\n[시스템] 고객이 통화금지를 요청하였습니다. 팀장 승인 대기중."
                        call_record.save()
                    else:
                        # 팀장/관리자는 즉시 적용
                        customer.is_do_not_call = True
                        customer.do_not_call_reason = "고객 요청"
                        customer.do_not_call_date = timezone.now()
                        customer.do_not_call_approved_by = request.user
                        customer.do_not_call_approved_date = timezone.now()
                        customer.status = 'do_not_call'
                        customer.save()
                    
                elif call_record.call_result in ['no_answer', 'busy']:
                    # 부재중이나 통화중인 경우 상태 유지
                    pass
                
                elif call_record.call_result == 'callback_requested':
                    # 재통화 요청
                    if customer.status == 'pending':
                        customer.status = 'contacted'
                        customer.save()
                
                # 4. 성공 응답
                response_data = {
                    'success': True,
                    'message': '통화 기록이 저장되었습니다.',
                    'call_id': call_record.id,
                    'followup_completed': False
                }
                
                # 후속조치 완료 정보 추가
                if call_record.parent_call_id and call_record.parent_call:
                    response_data['followup_completed'] = True
                    response_data['parent_call_id'] = call_record.parent_call_id
                
                return JsonResponse(response_data)
            
        except Exception as e:
            # 상세한 에러 로깅
            import traceback
            print(f"❌ 통화 기록 저장 오류: {str(e)}")
            print(traceback.format_exc())
            
            return JsonResponse({
                'success': False,
                'error': f'저장 중 오류가 발생했습니다: {str(e)}'
            }, status=400)
    
    # GET 요청인 경우 (일반적으로 발생하지 않음)
    return redirect('customer_detail', pk=customer.pk)


@login_required
def call_records(request):
    """통화 기록 목록"""
    records = CallRecord.objects.filter(
        is_deleted=False,
        parent_call__isnull=True  # 후속조치가 아닌 원본 통화만
    ).select_related('customer', 'caller').prefetch_related(
        Prefetch(
            'child_calls',
            queryset=CallRecord.objects.filter(is_deleted=False).select_related('caller')
        )
    ).order_by('-call_date')

    # 검색 필터 추가
    search_query = request.GET.get('search', '')
    if search_query:
        records = records.filter(
            Q(customer__name__icontains=search_query) |
            Q(customer__phone__icontains=search_query) |
            Q(customer__vehicle_number__icontains=search_query)
        )
    
    # 날짜 필터
    start_date = request.GET.get('start_date', '')
    end_date = request.GET.get('end_date', '')
    
    if start_date:
        try:
            start = datetime.strptime(start_date, '%Y-%m-%d').date()
            records = records.filter(call_date__date__gte=start)
        except ValueError:
            pass
    
    if end_date:
        try:
            end = datetime.strptime(end_date, '%Y-%m-%d').date()
            records = records.filter(call_date__date__lte=end)
        except ValueError:
            pass
    
    # 상담원 필터
    agent_filter = request.GET.get('agent', '')
    if agent_filter:
        records = records.filter(caller__username=agent_filter)
    
    # 통화상태 필터
    result_filter = request.GET.get('result', '')
    if result_filter:
        records = records.filter(call_result=result_filter)
    
    # 후속조치 필터 추가
    today = timezone.now().date()
    filter_type = request.GET.get('filter', '')
    
    if filter_type == 'today':
        records = records.filter(call_date__date=today)
    elif filter_type == 'week':
        week_ago = today - timedelta(days=7)
        records = records.filter(call_date__date__gte=week_ago)
    elif filter_type == 'month':
        month_ago = today - timedelta(days=30)
        records = records.filter(call_date__date__gte=month_ago)
    elif filter_type == 'pending_follow_up':
        records = records.filter(requires_follow_up=True, follow_up_completed=False)
    elif filter_type == 'today_follow_up':
        records = records.filter(requires_follow_up=True, follow_up_date=today)
    elif filter_type == 'overdue_follow_up':
        records = records.filter(
            requires_follow_up=True, 
            follow_up_completed=False,
            follow_up_date__lt=today
        )
    elif filter_type == 'follow_up':
        records = records.filter(follow_up_date__isnull=False)
    
    # 통계 계산
    total_calls = records.count()
    connected_calls = records.filter(call_result='connected').count()
    follow_up_calls = records.filter(requires_follow_up=True).count()
    
    # 페이징
    paginator = Paginator(records, 50)
    page_number = request.GET.get('page')
    records = paginator.get_page(page_number)
    
    # 상담원 목록 (필터용)
    agents = User.objects.filter(
        callrecord__isnull=False
    ).distinct().order_by('username')

    sidebar_stats = get_sidebar_stats()

    context = {
        'records': records,
        'agents': agents,
        'total_calls': total_calls,
        'connected_calls': connected_calls,
        'follow_up_calls': follow_up_calls,
        'today': today,
    }
    
    context.update(sidebar_stats)

    return render(request, 'call_records.html', context)


@login_required
@manager_required
def upload_data(request):
    """CSV/Excel 데이터 업로드"""
    
    def process_batch(batch_data, data_extract_date):
        """배치 데이터 처리"""
        batch_new = 0
        batch_updated = 0
        
        for phone, vehicle_number, customer_data in batch_data:
            try:
                customer, created = Customer.objects.update_or_create(
                    phone=phone,
                    vehicle_number=vehicle_number,
                    defaults=customer_data
                )

                # 실제 검사일 계산 및 저장
                customer.calculate_inspection_date(data_extract_date)
                
                # 우선순위와 태그 업데이트
                customer.update_priority_tags()
                customer.save()
                
                if created:
                    batch_new += 1
                else:
                    batch_updated += 1
                    
            except Exception:
                continue
                
        return batch_new, batch_updated
    
    if request.method == 'POST':
        form = CustomerUploadForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded_file = request.FILES['file']
            data_extract_date = form.cleaned_data['data_extract_date']  # 추출일 가져오기

            try:
                # 파일 확장자에 따라 처리
                if uploaded_file.name.endswith(('.xlsx', '.xls')):
                    df = pd.read_excel(uploaded_file, sheet_name='고객')
                else:
                    df = pd.read_csv(uploaded_file, encoding='utf-8')
                
                new_count = 0
                updated_count = 0
                error_count = 0
                total_rows = len(df)
                
                def clean_phone_number(phone):
                    """전화번호 정제"""
                    if not phone or pd.isna(phone):
                        return ''
                    phone = re.sub(r'[^\d]', '', str(phone))
                    if len(phone) == 11 and phone.startswith('010'):
                        return f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
                    elif len(phone) == 10 and phone.startswith('01'):
                        return f"0{phone[:2]}-{phone[2:6]}-{phone[6:]}"
                    return phone
                
                def parse_date(date_value):
                    """날짜 파싱"""
                    if pd.isna(date_value) or not date_value:
                        return None
                    
                    # 문자열인 경우
                    if isinstance(date_value, str):
                        try:
                            # 'YYYY-MM-DD' 형식 파싱
                            return datetime.strptime(date_value.strip(), '%Y-%m-%d').date()
                        except ValueError:
                            # 다른 형식들 시도
                            date_formats = [
                                '%Y/%m/%d',
                                '%Y.%m.%d',
                                '%d-%m-%Y',
                                '%d/%m/%Y',
                                '%Y%m%d',
                            ]
                            for fmt in date_formats:
                                try:
                                    return datetime.strptime(date_value.strip(), fmt).date()
                                except ValueError:
                                    continue
                    
                    # datetime 객체인 경우
                    if hasattr(date_value, 'date'):
                        return date_value.date()
                    
                    return None
                
                def map_customer_grade(grade_str):
                    """고객등급 매핑"""
                    if not grade_str or pd.isna(grade_str):
                        return ''
                    grade_mapping = {
                        'VIP': 'vip', 'vip': 'vip',
                        '정회원': 'regular', '준회원': 'associate',
                        '신규': 'new',
                    }
                    return grade_mapping.get(str(grade_str).strip(), '')
                
                with transaction.atomic():
                    batch_size = 500
                    batch_data = []
                    
                    for index, row in df.iterrows():
                        try:
                            # 필수 필드 검증
                            name = str(row.get('고객명', '')).strip()
                            phone = clean_phone_number(row.get('휴대전화', ''))
                            vehicle_number = str(row.get('차량번호', '')).strip()
                            
                            if not name or not phone or not vehicle_number:
                                error_count += 1
                                continue
                            
                            # 고객 데이터 준비
                            customer_data = {
                                'name': name,
                                'phone': phone,
                                'vehicle_number': vehicle_number,
                                'vehicle_name': str(row.get('차량명', '') or '').strip(),
                                'vehicle_model': str(row.get('모델명', '') or '').strip(),
                                'address': str(row.get('주소', '') or '').strip(),
                                'inspection_expiry_date': parse_date(row.get('검사만료일')),
                                'insurance_expiry_date': parse_date(row.get('보험만기일')),
                                'vehicle_registration_date': parse_date(row.get('차량등록일')),
                                'customer_grade': map_customer_grade(row.get('고객등급', '')),
                                'visit_count': int(row.get('방문수', 0)) if pd.notna(row.get('방문수')) else 0,
                            }
                            
                            batch_data.append((phone, vehicle_number, customer_data))
                            
                            # 배치 처리 - data_extract_date 전달
                            if len(batch_data) >= batch_size:
                                batch_new, batch_updated = process_batch(batch_data, data_extract_date)
                                new_count += batch_new
                                updated_count += batch_updated
                                batch_data = []
                                
                        except Exception as e:
                            error_count += 1
                            continue
                    
                    # 남은 데이터 처리
                    if batch_data:
                        batch_new, batch_updated = process_batch(batch_data, data_extract_date)
                        new_count += batch_new
                        updated_count += batch_updated
                
                # 업로드 이력 저장
                UploadHistory.objects.create(
                    uploaded_by=request.user,
                    file_name=uploaded_file.name,
                    total_records=new_count + updated_count,
                    new_records=new_count,
                    updated_records=updated_count,
                    error_count=error_count,
                    notes=f"웹 업로드 완료. 총 {total_rows:,}행 처리. 데이터 추출일: {data_extract_date}"
                )
                
                messages.success(
                    request, 
                    f'🎉 업로드 완료! '
                    f'신규 {new_count:,}건, 업데이트 {updated_count:,}건, 오류 {error_count:,}건'
                )
                
            except Exception as e:
                messages.error(request, f'❌ 파일 처리 중 오류가 발생했습니다: {str(e)}')
            
            return redirect('upload_data')
    else:
        form = CustomerUploadForm()
    
    # 최근 업로드 이력
    upload_history = UploadHistory.objects.order_by('-upload_date')[:10]
    
    sidebar_stats = get_sidebar_stats()

    context = {
        'form': form,
        'upload_history': upload_history,
    }
    
    context.update(sidebar_stats)

    return render(request, 'upload_data.html', context)


@login_required
def add_follow_up(request):
    """후속조치 추가 (AJAX)"""
    print("add_follow_up 뷰 호출됨")
    print(f"Method: {request.method}")
    print(f"POST data: {request.POST}")
    if request.method == 'POST':
        try:
            call_record_id = request.POST.get('call_record_id')
            print(f"call_record_id: {call_record_id}")

            call_record = get_object_or_404(CallRecord, id=call_record_id)
            
            # CallFollowUp 생성
            follow_up = CallFollowUp.objects.create(
                call_record=call_record,
                created_by=request.user,
                action_type=request.POST.get('follow_up_action', ''),
                notes=request.POST.get('follow_up_notes', ''),
                scheduled_date=request.POST.get('follow_up_date') or None
            )
            
            # 후속조치가 완료 타입이면 원 통화 기록도 완료 처리
            if request.POST.get('follow_up_action') in ['converted', 'closed', 'data_sent']:
                call_record.follow_up_completed = True
                call_record.follow_up_completed_at = timezone.now()  # 완료 시간 기록 추가
                call_record.save()
            
            # 새 통화 기록으로도 저장 (parent_call 관계 설정)
            if request.POST.get('follow_up_action') != '':
                new_call = CallRecord.objects.create(
                    customer=call_record.customer,
                    caller=request.user,
                    call_result='connected',
                    notes=f"[후속조치] {request.POST.get('follow_up_notes', '')}",
                    parent_call=call_record,  # 원 통화를 parent로 설정
                    is_deleted=False
                )
                
                # 원 통화 기록의 후속조치 완료 처리
                call_record.follow_up_completed = True
                call_record.save()
            
            return JsonResponse({'success': True})
        except Exception as e:
            import traceback
            print(f"후속조치 추가 오류: {str(e)}")
            print(traceback.format_exc())
            return JsonResponse({'success': False, 'error': str(e)})
    
    return JsonResponse({'success': False, 'error': 'POST 요청만 허용됩니다.'})


@login_required
def sidebar_stats_api(request):
    """사이드바 통계 API"""
    today = timezone.now().date()
    
    # 오늘 통화 수
    today_calls = CallRecord.objects.filter(
        call_date__date=today,
        is_deleted=False
    ).count()
    
    # 미완료 후속조치
    pending_followups = CallRecord.objects.filter(
        requires_follow_up=True,
        follow_up_completed=False,
        is_deleted=False
    ).count()
    
    # 검사만료 고객
    overdue_customers = Customer.objects.filter(
        inspection_expiry_date__isnull=False,
        inspection_expiry_date__lt=today
    ).count()
    
    return JsonResponse({
        'success': True,
        'today_calls': today_calls,
        'pending_followups': pending_followups,
        'overdue_customers': overdue_customers
    })

@login_required
@ajax_manager_required
def approve_do_not_call(request, pk):
    """통화금지 요청 승인/거절"""
    if request.method == 'POST':
        customer = get_object_or_404(Customer, pk=pk)
        action = request.POST.get('action')
        
        if action == 'approve':
            customer.is_do_not_call = True
            customer.do_not_call_approved_by = request.user
            customer.do_not_call_approved_date = timezone.now()
            customer.status = 'do_not_call'
            customer.do_not_call_requested = False
            customer.save()
            
            return JsonResponse({
                'success': True,
                'message': '통화금지가 승인되었습니다.'
            })
        
        elif action == 'reject':
            customer.do_not_call_requested = False
            customer.do_not_call_requested_by = None
            customer.do_not_call_request_date = None
            customer.save()
            
            return JsonResponse({
                'success': True,
                'message': '통화금지 요청이 거절되었습니다.'
            })
    
    return JsonResponse({'success': False, 'error': '잘못된 요청입니다.'})

@login_required
@manager_required
def do_not_call_requests(request):
    """통화금지 요청 목록"""
    pending_requests = Customer.objects.filter(
        do_not_call_requested=True,
        is_do_not_call=False
    ).select_related('do_not_call_requested_by').order_by('-do_not_call_request_date')
    
    sidebar_stats = get_sidebar_stats()
    
    context = {
        'pending_requests': pending_requests,
    }
    context.update(sidebar_stats)
    
    return render(request, 'do_not_call_requests.html', context)


@login_required
@manager_required
def call_assignment(request):
    """콜 배정 관리 페이지"""
    # 먼저 만료된 배정 자동 처리
    expired_count = 0
    old_assignments = CallAssignment.objects.filter(
        status__in=['pending', 'in_progress'],
        assigned_at__lte=timezone.now() - timedelta(days=7)
    )
    for assignment in old_assignments:
        if assignment.auto_expire():
            expired_count += 1
    
    if expired_count > 0:
        messages.info(request, f'{expired_count}건의 배정이 7일 경과로 자동 만료되었습니다.')
    
    if request.method == 'POST':
        action = request.POST.get('action')
        
        if action == 'assign':
            # 배정 처리
            customer_ids = request.POST.getlist('customer_ids')
            agent_id = request.POST.get('agent_id')
            priority = request.POST.get('priority', 'normal')
            due_date = request.POST.get('due_date')
            notes = request.POST.get('notes', '')
            
            if not customer_ids or not agent_id:
                messages.error(request, '고객과 상담원을 선택해주세요.')
                return redirect('call_assignment')
            
            try:
                with transaction.atomic():
                    agent = User.objects.get(id=agent_id)
                    assigned_count = 0
                    already_assigned_count = 0
                    skipped_count = 0
                    
                    for customer_id in customer_ids:
                        customer = Customer.objects.select_for_update().get(id=customer_id)
                        
                        # 기존 배정 확인
                        existing = CallAssignment.objects.filter(
                            customer=customer,
                            status__in=['pending', 'in_progress']
                        ).select_for_update().first()
                        
                        if existing:
                            if existing.assigned_to == agent:
                                skipped_count += 1
                                continue
                            
                            # 기존 배정 종료
                            existing.status = 'cancelled'
                            existing.completed_date = timezone.now()
                            existing.notes += f"\n[재배정] {timezone.now().strftime('%Y-%m-%d %H:%M')} - 새 담당자: {agent.username}"
                            existing.save()
                            already_assigned_count += 1
                        
                        # 새 배정 생성
                        CallAssignment.objects.create(
                            customer=customer,
                            assigned_to=agent,
                            assigned_by=request.user,
                            priority=priority,
                            due_date=due_date if due_date else None,
                            notes=notes + (f"\n[재배정] 이전 담당: {existing.assigned_to.username}" if existing else "")
                        )
                        assigned_count += 1
                    
                    # 결과 메시지
                    message_parts = []
                    if assigned_count > 0:
                        message_parts.append(f'{assigned_count}명 배정 완료')
                    if already_assigned_count > 0:
                        message_parts.append(f'재배정 {already_assigned_count}명')
                    if skipped_count > 0:
                        message_parts.append(f'중복 제외 {skipped_count}명')
                    
                    if message_parts:
                        messages.success(request, ' ('.join(message_parts) + ')')
                    else:
                        messages.warning(request, '배정할 고객이 없습니다.')
                        
            except User.DoesNotExist:
                messages.error(request, '상담원을 찾을 수 없습니다.')
            except Exception as e:
                messages.error(request, f'배정 중 오류가 발생했습니다: {str(e)}')
            
            return redirect('call_assignment')
        
        elif action == 'reassign':
            # 개별 재배정 처리
            customer_id = request.POST.get('customer_id')
            new_agent_id = request.POST.get('new_agent_id')
            reason = request.POST.get('reason', '')
            
            try:
                customer = Customer.objects.get(id=customer_id)
                new_agent = User.objects.get(id=new_agent_id)
                
                # 기존 배정 종료
                current_assignment = CallAssignment.objects.filter(
                    customer=customer,
                    status__in=['pending', 'in_progress']
                ).first()
                
                if current_assignment:
                    old_agent = current_assignment.assigned_to.username
                    current_assignment.status = 'cancelled'
                    current_assignment.completed_date = timezone.now()
                    current_assignment.notes += f"\n[재배정] {timezone.now().strftime('%Y-%m-%d %H:%M')} - {reason}"
                    current_assignment.save()
                else:
                    old_agent = '없음'
                
                # 새 배정 생성
                CallAssignment.objects.create(
                    customer=customer,
                    assigned_to=new_agent,
                    assigned_by=request.user,
                    priority='normal',
                    notes=f"[재배정] 이전 담당: {old_agent}\n사유: {reason}"
                )
                
                messages.success(request, f'{customer.name} 고객을 {new_agent.username}에게 재배정했습니다.')
                
            except Exception as e:
                messages.error(request, f'재배정 중 오류가 발생했습니다: {str(e)}')
            
            return redirect('call_assignment')
    
    # GET 요청 처리
    today = timezone.now().date()
    
    # 탭 선택 (assigned: 배정된 고객, unassigned: 미배정 고객)
    tab = request.GET.get('tab', 'unassigned')
    
    # 필터 파라미터
    customer_type = request.GET.get('type', '')
    grade_filter = request.GET.get('grade', '')
    search_query = request.GET.get('search', '')
    page_number = request.GET.get('page', 1)
    
    # 상담원 목록
    agents = User.objects.filter(
        userprofile__role='agent',
        is_active=True
    ).select_related('userprofile')
    
    # 배정 현황
    active_assignments = CallAssignment.objects.filter(
        status__in=['pending', 'in_progress']
    ).select_related('customer', 'assigned_to', 'assigned_by').order_by('-assigned_at')
    
    # 현재 배정된 고객 ID 목록
    assigned_customer_ids = active_assignments.values_list('customer_id', flat=True)
    
    # 전체 미배정 고객 수 계산 (필터 적용 전)
    total_unassigned_customers = Customer.objects.filter(
        is_active_customer=True,
        is_do_not_call=False
    ).exclude(id__in=assigned_customer_ids).count()
    
    # 기본 고객 쿼리
    customers_query = Customer.objects.filter(
        is_active_customer=True,
        is_do_not_call=False
    )
    
    # 탭에 따른 필터링
    if tab == 'assigned':
        # 배정된 고객만
        customers_query = customers_query.filter(id__in=assigned_customer_ids)
    else:
        # 미배정 고객만 (기본)
        customers_query = customers_query.exclude(id__in=assigned_customer_ids)
    
    # 검색 필터
    if search_query:
        customers_query = customers_query.filter(
            Q(name__icontains=search_query) |
            Q(phone__icontains=search_query) |
            Q(vehicle_number__icontains=search_query)
        )

    # 필터 적용 및 날짜 정보 생성
    filter_date_info = None
    if customer_type:
        if customer_type == 'overdue':
            customers_query = customers_query.filter(
                inspection_expiry_date__isnull=False,
                inspection_expiry_date__lt=today
            )
            filter_date_info = f"검사만료일이 {today.strftime('%Y-%m-%d')} 이전"
        elif customer_type == 'due_soon':
            three_months_later = today + timedelta(days=90)
            customers_query = customers_query.filter(
                inspection_expiry_date__gte=today,
                inspection_expiry_date__lte=three_months_later
            )
            filter_date_info = f"검사만료일이 {today.strftime('%Y-%m-%d')} ~ {three_months_later.strftime('%Y-%m-%d')} 사이"
        elif customer_type == 'happy_3month':
            # 실제 검사일 기준 - ±1일로 변경
            three_months_ago = today - timedelta(days=90)
            customers_query = customers_query.filter(
                actual_inspection_date__gte=three_months_ago - timedelta(days=1),
                actual_inspection_date__lte=three_months_ago + timedelta(days=1)
            )
            filter_date_info = f"3개월 전 검사 고객 (검사일: {three_months_ago.strftime('%Y-%m-%d')} ±1일)"
        elif customer_type == 'happy_6month':
            # 실제 검사일 기준 - ±1일로 변경
            six_months_ago = today - timedelta(days=180)
            customers_query = customers_query.filter(
                actual_inspection_date__gte=six_months_ago - timedelta(days=1),
                actual_inspection_date__lte=six_months_ago + timedelta(days=1)
            )
            filter_date_info = f"6개월 전 검사 고객 (검사일: {six_months_ago.strftime('%Y-%m-%d')} ±1일)"
        elif customer_type == 'happy_12month':
            # 실제 검사일 기준 - ±1일로 변경
            twelve_months_ago = today - timedelta(days=365)
            customers_query = customers_query.filter(
                actual_inspection_date__gte=twelve_months_ago - timedelta(days=1),
                actual_inspection_date__lte=twelve_months_ago + timedelta(days=1)
            )
            filter_date_info = f"12개월 전 검사 고객 (검사일: {twelve_months_ago.strftime('%Y-%m-%d')} ±1일)"
        elif customer_type == 'vip':
            customers_query = customers_query.filter(customer_grade='vip')
            filter_date_info = "VIP 등급 고객"
        elif customer_type == 'frequent':
            customers_query = customers_query.filter(visit_count__gte=3)
            filter_date_info = "단골 고객 (방문 3회 이상)"
        elif customer_type == 'pending':
            customers_query = customers_query.filter(status='pending')
            filter_date_info = "미접촉 고객"
    
    if grade_filter:
        customers_query = customers_query.filter(customer_grade=grade_filter)
    
    # 정렬 - 기본값을 오래된 날짜순으로 변경
    sort_by = request.GET.get('sort', 'oldest')
    if sort_by == 'expiry' or sort_by == 'oldest':
        customers_query = customers_query.order_by('actual_inspection_date', 'inspection_expiry_date')
    elif sort_by == 'visit':
        customers_query = customers_query.order_by('-visit_count')
    elif sort_by == 'name':
        customers_query = customers_query.order_by('name')
    elif sort_by == 'priority':
        customers_query = customers_query.order_by('-is_inspection_overdue', '-priority', 'updated_at')
    else:
        customers_query = customers_query.order_by('actual_inspection_date', 'inspection_expiry_date')
    
    # 필터링된 고객 수
    filtered_customers_count = customers_query.count()
    
    # 페이지네이션
    paginator = Paginator(customers_query, 50)
    page_obj = paginator.get_page(page_number)
    
    # 현재 페이지의 고객 ID 추출
    customer_ids = [customer.id for customer in page_obj]
    
    # 배정 정보 조회 및 처리
    current_assignments = CallAssignment.objects.filter(
        customer_id__in=customer_ids,
        status__in=['pending', 'in_progress']
    ).select_related('assigned_to').values(
        'customer_id', 'assigned_to__username', 'status', 'assigned_at', 'due_date'
    )
    
    assignment_dict = {}
    for assignment in current_assignments:
        days_passed = (timezone.now().date() - assignment['assigned_at'].date()).days
        assignment_dict[assignment['customer_id']] = {
            'assigned_to_username': assignment['assigned_to__username'],
            'status': assignment['status'],
            'assigned_at': assignment['assigned_at'],
            'due_date': assignment['due_date'],
            'days_passed': days_passed,
            'is_overdue': days_passed >= 7
        }
    
    # 각 고객에 배정 정보 추가
    for customer in page_obj:
        customer.current_assignment_info = assignment_dict.get(customer.id)
    
    # 상담원별 통계
    agent_stats = []
    for agent in agents:
        stats = {
            'agent': agent,
            'pending_count': active_assignments.filter(assigned_to=agent, status='pending').count(),
            'in_progress_count': active_assignments.filter(assigned_to=agent, status='in_progress').count(),
            'completed_today': CallAssignment.objects.filter(
                assigned_to=agent,
                status='completed',
                completed_date__date=today
            ).count(),
            'calls_today': CallRecord.objects.filter(
                caller=agent,
                call_date__date=today,
                is_deleted=False
            ).count()
        }
        agent_stats.append(stats)
    
    # 배정 통계
    assignment_stats = {
        'total_assigned': active_assignments.count(),
        'pending': active_assignments.filter(status='pending').count(),
        'in_progress': active_assignments.filter(status='in_progress').count(),
        'completed_today': CallAssignment.objects.filter(
            status='completed',
            completed_date__date=today
        ).count(),
        'overdue': active_assignments.filter(
            assigned_at__lte=timezone.now() - timedelta(days=7)
        ).count()
    }
    
    sidebar_stats = get_sidebar_stats()
    
    # context에 추가
    context = {
        'agents': agents,
        'assignments': active_assignments[:50],
        'assignable_customers': page_obj,
        'total_customers': total_unassigned_customers,
        'filtered_customers_count': filtered_customers_count,
        'agent_stats': agent_stats,
        'assignment_stats': assignment_stats,
        'today': today,
        'tab': tab,
        'customer_type': customer_type,
        'grade_filter': grade_filter,
        'search_query': search_query,
        'filter_date_info': filter_date_info,
    }
    context.update(sidebar_stats)
    
    return render(request, 'call_assignment.html', context)



@login_required
def my_assignments(request):
    """내 배정 목록 (상담원용)"""
    assignments = CallAssignment.objects.filter(
        assigned_to=request.user,
        status__in=['pending', 'in_progress']
    ).select_related('customer', 'assigned_by').order_by('priority', 'due_date')
    
    # 오늘 처리한 배정
    today = timezone.now().date()
    completed_today = CallAssignment.objects.filter(
        assigned_to=request.user,
        status='completed',
        completed_date__date=today
    ).count()
    
    sidebar_stats = get_sidebar_stats()
    
    context = {
        'assignments': assignments,
        'completed_today': completed_today,
        'today': today,
    }
    context.update(sidebar_stats)
    
    return render(request, 'my_assignments.html', context)


@login_required
def update_assignment_status(request, assignment_id):
    """배정 상태 업데이트 (AJAX)"""
    if request.method == 'POST':
        try:
            assignment = get_object_or_404(CallAssignment, id=assignment_id)
            
            # 권한 확인
            if assignment.assigned_to != request.user and not request.user.userprofile.is_manager_or_above():
                return JsonResponse({'success': False, 'error': '권한이 없습니다.'})
            
            new_status = request.POST.get('status')
            if new_status in ['pending', 'in_progress', 'completed', 'cancelled']:
                assignment.status = new_status
                
                if new_status == 'completed':
                    assignment.completed_date = timezone.now()
                
                assignment.save()
                
                return JsonResponse({
                    'success': True,
                    'message': '상태가 업데이트되었습니다.'
                })
            else:
                return JsonResponse({'success': False, 'error': '잘못된 상태값입니다.'})
                
        except Exception as e:
            return JsonResponse({'success': False, 'error': str(e)})
    
    return JsonResponse({'success': False, 'error': 'POST 요청만 허용됩니다.'})


@login_required
@manager_required
def team_dashboard(request):
    """팀장 대시보드 - 팀원 성과 모니터링"""
    today = timezone.now().date()
    
    # 사용자 프로필 및 권한 확인
    user_profile = request.user.userprofile if hasattr(request.user, 'userprofile') else None
    is_admin = user_profile and user_profile.role == 'admin'
    
    # 사용 가능한 팀 목록 생성
    available_teams = []
    selected_team = request.GET.get('team', '')
    
    if is_admin:
        # 관리자는 모든 팀 목록 가져오기
        available_teams = UserProfile.objects.exclude(
            team__isnull=True
        ).exclude(
            team=''
        ).values_list('team', flat=True).distinct().order_by('team')
        
        # 선택된 팀이 없으면 전체 보기
        if selected_team and selected_team in available_teams:
            # 특정 팀 선택됨
            team_agents = User.objects.filter(
                userprofile__team=selected_team,
                is_active=True
            ).select_related('userprofile')
        else:
            # 전체 팀 보기
            selected_team = ''
            team_agents = User.objects.filter(
                is_active=True,
                userprofile__role__in=['agent', 'manager']
            ).select_related('userprofile')
    else:
        # 팀장은 자기 팀만
        team_name = user_profile.team if user_profile else None
        if team_name:
            selected_team = team_name
            team_agents = User.objects.filter(
                userprofile__team=team_name,
                is_active=True
            ).select_related('userprofile')
        else:
            # 팀이 없으면 본인만
            team_agents = User.objects.filter(id=request.user.id)
    
    # 기간 설정 - 기본값 오늘
    date_from_str = request.GET.get('date_from')
    date_to_str = request.GET.get('date_to')
    
    if date_from_str:
        try:
            date_from = datetime.strptime(date_from_str, '%Y-%m-%d').date()
        except:
            date_from = today
    else:
        date_from = today
    
    if date_to_str:
        try:
            date_to = datetime.strptime(date_to_str, '%Y-%m-%d').date()
        except:
            date_to = today
    else:
        date_to = today
    
    # 상담원별 성과 데이터 수집
    agent_performances = []
    
    for agent in team_agents:
        # 기간 내 통화 기록
        calls = CallRecord.objects.filter(
            caller=agent,
            call_date__date__gte=date_from,
            call_date__date__lte=date_to,
            is_deleted=False
        )
        
        # 오늘 통화만 별도 조회
        calls_today = calls.filter(call_date__date=today)
        
        total_calls = calls.count()
        connected_calls = calls.filter(call_result='connected').count()
        
        # 오늘 통화
        today_total = calls_today.count()
        today_connected = calls_today.filter(call_result='connected').count()
        
        # 관심 고객 수
        interested_calls = calls.filter(
            interest_type__in=['insurance', 'maintenance', 'financing', 'multiple']
        ).count()
        
        # 후속조치 관련
        followup_required = calls.filter(requires_follow_up=True).count()
        followup_completed = calls.filter(
            requires_follow_up=True,
            follow_up_completed=True
        ).count()
        
        # 오늘 배정된 고객 수
        assigned_today = CallAssignment.objects.filter(
            assigned_to=agent,
            assigned_at__date=today,
            status__in=['pending', 'in_progress']
        ).count()
        
        # 오늘 완료한 배정
        completed_today = CallAssignment.objects.filter(
            assigned_to=agent,
            status='completed',
            completed_at__date=today
        ).count()
        
        # 해피콜 성과 (오늘 기준)
        happy_calls = {
            '3month': calls_today.filter(
                customer__actual_inspection_date__gte=today - timedelta(days=97),
                customer__actual_inspection_date__lte=today - timedelta(days=83)
            ).count(),
            '6month': calls_today.filter(
                customer__actual_inspection_date__gte=today - timedelta(days=187),
                customer__actual_inspection_date__lte=today - timedelta(days=173)
            ).count(),
            '12month': calls_today.filter(
                customer__actual_inspection_date__gte=today - timedelta(days=372),
                customer__actual_inspection_date__lte=today - timedelta(days=358)
            ).count(),
            '18month': calls_today.filter(
                customer__actual_inspection_date__gte=today - timedelta(days=555),
                customer__actual_inspection_date__lte=today - timedelta(days=541)
            ).count(),
        }
        
        # 통화 성공률
        success_rate = 0
        if total_calls > 0:
            success_rate = round((connected_calls / total_calls) * 100, 1)
        
        # 오늘 목표 달성률
        daily_target = agent.userprofile.daily_call_target if hasattr(agent, 'userprofile') else 100
        achievement_rate = 0
        if daily_target > 0:
            achievement_rate = round((today_total / daily_target) * 100, 1)
        
        # 마지막 활동 시간
        last_call = calls_today.order_by('-call_date').first()
        last_activity = last_call.call_date if last_call else None
        
        # 상태 판단 (30분 이내 활동이면 online)
        status = 'offline'
        if last_activity:
            time_diff = timezone.now() - last_activity
            if time_diff.seconds < 1800:  # 30분
                status = 'online'
            elif time_diff.seconds < 3600:  # 1시간
                status = 'idle'
        
        # 팀장 여부 확인
        is_manager = hasattr(agent, 'userprofile') and agent.userprofile.role == 'manager'
        
        # 정렬을 위한 sort_key 추가 (팀장은 999999로 설정하여 항상 앞에 오도록)
        sort_key = 999999 if is_manager else achievement_rate
        
        agent_performances.append({
            'agent': agent,
            'total_calls': total_calls,
            'connected_calls': connected_calls,
            'today_total': today_total,
            'today_connected': today_connected,
            'success_rate': success_rate,
            'interested_calls': interested_calls,
            'followup_required': followup_required,
            'followup_completed': followup_completed,
            'assigned_today': assigned_today,
            'completed_today': completed_today,
            'happy_calls': happy_calls,
            'daily_target': daily_target,
            'achievement_rate': achievement_rate,
            'status': status,
            'last_activity': last_activity,
            'is_manager': is_manager,
            'sort_key': sort_key
        })
    
    # 팀 전체 통계 (오늘 기준)
    team_stats = {
        'total_agents': len(team_agents),
        'active_agents': sum(1 for p in agent_performances if p['status'] in ['online', 'idle']),
        'total_calls': sum(p['today_total'] for p in agent_performances),
        'total_connected': sum(p['today_connected'] for p in agent_performances),
        'total_interested': sum(p['interested_calls'] for p in agent_performances),
        'avg_success_rate': round(
            sum(p['success_rate'] for p in agent_performances) / len(agent_performances), 1
        ) if agent_performances else 0,
        'avg_achievement_rate': round(
            sum(p['achievement_rate'] for p in agent_performances) / len(agent_performances), 1
        ) if agent_performances else 0,
    }
    
    # 시간대별 통화 분포 (오늘)
    hourly_data = []
    max_hourly_calls = 0
    
    for hour in range(9, 19):  # 9시부터 18시까지
        hour_calls = CallRecord.objects.filter(
            caller__in=team_agents,
            call_date__date=today,
            call_date__hour=hour,
            is_deleted=False
        ).count()
        
        if hour_calls > max_hourly_calls:
            max_hourly_calls = hour_calls
        
        hourly_data.append({
            'hour': f"{hour:02d}:00",
            'hour_display': f"{hour:02d}시",
            'count': hour_calls
        })
    
    # 비율 계산
    if max_hourly_calls > 0:
        for item in hourly_data:
            item['percentage'] = round((item['count'] / max_hourly_calls) * 100, 1)
    else:
        for item in hourly_data:
            item['percentage'] = 0
    
    # 최근 통화 기록 (팀 전체, 오늘)
    recent_team_calls = CallRecord.objects.filter(
        caller__in=team_agents,
        call_date__date=today,
        is_deleted=False
    ).select_related('customer', 'caller').order_by('-call_date')[:20]
    
    # 미완료 후속조치 목록
    pending_followups = CallRecord.objects.filter(
        caller__in=team_agents,
        requires_follow_up=True,
        follow_up_completed=False,
        is_deleted=False
    ).select_related('customer', 'caller').order_by('follow_up_date')[:10]
    
    # 팀별 요약 (관리자가 전체 팀 볼 때만)
    team_summaries = []
    if is_admin and not selected_team and available_teams:
        for team in available_teams:
            team_members = User.objects.filter(
                userprofile__team=team,
                is_active=True
            ).select_related('userprofile')
            
            team_manager = team_members.filter(userprofile__role='manager').first()
            team_agent_count = team_members.filter(userprofile__role='agent').count()
            
            # 오늘 통화 통계
            team_calls_today = CallRecord.objects.filter(
                caller__in=team_members,
                call_date__date=today,
                is_deleted=False
            )
            
            total_today = team_calls_today.count()
            connected_today = team_calls_today.filter(call_result='connected').count()
            
            # 목표 달성률 계산
            team_daily_target = sum(
                member.userprofile.daily_call_target 
                for member in team_members 
                if hasattr(member, 'userprofile')
            )
            
            achievement_rate = 0
            if team_daily_target > 0:
                achievement_rate = round((total_today / team_daily_target) * 100, 1)
            
            success_rate = 0
            if total_today > 0:
                success_rate = round((connected_today / total_today) * 100, 1)
            
            team_summaries.append({
                'team_name': team,
                'manager_name': team_manager.username if team_manager else None,
                'agent_count': team_agent_count,
                'today_calls': total_today,
                'success_rate': success_rate,
                'achievement_rate': achievement_rate,
            })
    
    sidebar_stats = get_sidebar_stats()
    
    context = {
        'selected_team': selected_team,
        'available_teams': list(available_teams) if available_teams else [],
        'is_admin': is_admin,
        'team_summaries': team_summaries,
        'team_agents': team_agents,
        'agent_performances': agent_performances,
        'team_stats': team_stats,
        'hourly_data': hourly_data,
        'recent_team_calls': recent_team_calls,
        'pending_followups': pending_followups,
        'date_from': date_from,
        'date_to': date_to,
        'today': today,
    }
    context.update(sidebar_stats)
    
    return render(request, 'team_dashboard.html', context)


@login_required
@admin_required
def admin_dashboard(request):
    """관리자 대시보드 - 전체 팀/팀장 성과 모니터링"""
    today = timezone.now().date()
    
    # 기간 설정
    date_from_str = request.GET.get('date_from')
    date_to_str = request.GET.get('date_to')
    
    if date_from_str:
        try:
            date_from = datetime.strptime(date_from_str, '%Y-%m-%d').date()
        except:
            date_from = today - timedelta(days=7)
    else:
        date_from = today - timedelta(days=7)  # 기본 7일
    
    if date_to_str:
        try:
            date_to = datetime.strptime(date_to_str, '%Y-%m-%d').date()
        except:
            date_to = today
    else:
        date_to = today
    
    # 팀별 성과 수집
    teams = UserProfile.objects.values_list('team', flat=True).distinct().exclude(team='').exclude(team__isnull=True)
    team_performances = []
    
    for team_name in teams:
        # 팀 구성원
        team_members = User.objects.filter(
            userprofile__team=team_name,
            is_active=True
        ).select_related('userprofile')
        
        team_manager = team_members.filter(userprofile__role='manager').first()
        team_agents = team_members.filter(userprofile__role='agent')
        
        # 팀 통화 기록
        team_calls = CallRecord.objects.filter(
            caller__in=team_members,
            call_date__date__gte=date_from,
            call_date__date__lte=date_to,
            is_deleted=False
        )
        
        total_calls = team_calls.count()
        connected_calls = team_calls.filter(call_result='connected').count()
        
        # 팀 목표 및 달성률
        team_daily_target = sum(
            member.userprofile.daily_call_target 
            for member in team_agents
            if hasattr(member, 'userprofile')
        )
        
        days_count = (date_to - date_from).days + 1
        team_period_target = team_daily_target * days_count
        
        achievement_rate = 0
        if team_period_target > 0:
            achievement_rate = round((total_calls / team_period_target) * 100, 1)
        
        # 성공률
        success_rate = 0
        if total_calls > 0:
            success_rate = round((connected_calls / total_calls) * 100, 1)
        
        # 관심 고객 및 후속조치
        interested_customers = team_calls.filter(
            interest_type__in=['insurance', 'maintenance', 'financing', 'multiple']
        ).values('customer').distinct().count()
        
        followup_completion_rate = 0
        followup_required = team_calls.filter(requires_follow_up=True).count()
        if followup_required > 0:
            followup_completed = team_calls.filter(
                requires_follow_up=True,
                follow_up_completed=True
            ).count()
            followup_completion_rate = round((followup_completed / followup_required) * 100, 1)
        
        # 관심 고객 비율 계산
        interested_ratio = 0
        if total_calls > 0:
            interested_ratio = round((interested_customers / total_calls) * 100, 1)
        
        team_performances.append({
            'team_name': team_name,
            'manager': team_manager,
            'agent_count': team_agents.count(),
            'total_calls': total_calls,
            'connected_calls': connected_calls,
            'success_rate': success_rate,
            'interested_customers': interested_customers,
            'interested_ratio': interested_ratio,
            'achievement_rate': achievement_rate,
            'followup_completion_rate': followup_completion_rate,
            'daily_target': team_daily_target,
            'period_target': team_period_target,
        })
    
    # 팀 성과 정렬 (달성률 높은 순)
    team_performances = sorted(team_performances, key=lambda x: x['achievement_rate'], reverse=True)
    
    # 전체 통계
    all_calls = CallRecord.objects.filter(
        call_date__date__gte=date_from,
        call_date__date__lte=date_to,
        is_deleted=False
    )
    
    overall_stats = {
        'total_teams': len(teams),
        'total_managers': User.objects.filter(userprofile__role='manager', is_active=True).count(),
        'total_agents': User.objects.filter(userprofile__role='agent', is_active=True).count(),
        'total_calls': all_calls.count(),
        'total_connected': all_calls.filter(call_result='connected').count(),
        'total_customers': Customer.objects.filter(
            call_records__call_date__date__gte=date_from,
            call_records__call_date__date__lte=date_to,
            call_records__is_deleted=False
        ).distinct().count(),
        'new_interested': all_calls.filter(
            interest_type__in=['insurance', 'maintenance', 'financing', 'multiple']
        ).values('customer').distinct().count(),
    }
    
    # 일별 성과 추이
    daily_performance = []
    current_date = date_from
    while current_date <= date_to:
        day_calls = all_calls.filter(call_date__date=current_date)
        daily_performance.append({
            'date': current_date,
            'total': day_calls.count(),
            'connected': day_calls.filter(call_result='connected').count(),
        })
        current_date += timedelta(days=1)
    
    # 상담원별 TOP 10 (기간 내)
    top_agents = []
    agents = User.objects.filter(userprofile__role='agent', is_active=True)
    
    for agent in agents:
        agent_calls = CallRecord.objects.filter(
            caller=agent,
            call_date__date__gte=date_from,
            call_date__date__lte=date_to,
            is_deleted=False
        )
        total = agent_calls.count()
        if total > 0:
            connected = agent_calls.filter(call_result='connected').count()
            success_rate = round((connected / total) * 100, 1) if total > 0 else 0
            
            top_agents.append({
                'agent': agent,
                'total_calls': total,
                'connected_calls': connected,
                'success_rate': success_rate,
                'team': agent.userprofile.team if hasattr(agent, 'userprofile') else '-'
            })
    
    # 통화 수 기준 정렬 후 상위 10명
    top_agents = sorted(top_agents, key=lambda x: x['total_calls'], reverse=True)[:10]
    
    # 실시간 알림 생성
    alerts = []
    
    # 목표 미달성 팀 체크
    for team in team_performances[:3]:  # 상위 3개 팀만
        if team['achievement_rate'] < 60:
            alerts.append({
                'type': 'warning',
                'title': '목표 미달성 주의',
                'message': f"{team['team_name']}팀의 목표 달성률이 60% 미만입니다.",
                'time': '방금 전'
            })
    
    # 후속조치 지연 체크
    overdue_followups = CallRecord.objects.filter(
        requires_follow_up=True,
        follow_up_completed=False,
        follow_up_date__lt=today,
        is_deleted=False
    ).count()
    
    if overdue_followups > 0:
        alerts.append({
            'type': 'danger',
            'title': '후속조치 지연',
            'message': f"{overdue_followups}건의 후속조치가 기한을 초과했습니다.",
            'time': '10분 전'
        })
    
    sidebar_stats = get_sidebar_stats()
    
    context = {
        'team_performances': team_performances,
        'overall_stats': overall_stats,
        'daily_performance': daily_performance,
        'top_agents': top_agents,
        'alerts': alerts,
        'date_from': date_from,
        'date_to': date_to,
        'today': today,
    }
    context.update(sidebar_stats)
    
    return render(request, 'admin_dashboard.html', context)


@login_required
@ajax_manager_required
def team_performance_api(request):
    """팀 성과 실시간 API"""
    team_name = request.GET.get('team')
    date_str = request.GET.get('date', timezone.now().date().isoformat())
    
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    except:
        target_date = timezone.now().date()
    
    # 팀 구성원
    if team_name:
        team_members = User.objects.filter(
            userprofile__team=team_name,
            is_active=True
        )
    else:
        team_members = User.objects.filter(
            userprofile__role='agent',
            is_active=True
        )
    
    # 실시간 데이터
    performance_data = []
    for member in team_members:
        calls_today = CallRecord.objects.filter(
            caller=member,
            call_date__date=target_date,
            is_deleted=False
        )
        
        performance_data.append({
            'id': member.id,
            'name': member.username,
            'total_calls': calls_today.count(),
            'connected': calls_today.filter(call_result='connected').count(),
            'last_call': calls_today.order_by('-call_date').first().call_date.isoformat() if calls_today.exists() else None,
            'status': 'active' if calls_today.filter(
                call_date__gte=timezone.now() - timedelta(minutes=30)
            ).exists() else 'idle'
        })
    
    return JsonResponse({
        'success': True,
        'data': performance_data,
        'timestamp': timezone.now().isoformat()
    })


@login_required
def agent_performance_api(request, agent_id):
    """특정 상담원 상세 성과 API"""
    try:
        agent = User.objects.get(id=agent_id)
        
        # 권한 체크
        if not request.user.userprofile.is_manager_or_above():
            if request.user != agent:
                return JsonResponse({'success': False, 'error': '권한이 없습니다.'})
        
        # 기간 설정
        days = int(request.GET.get('days', 7))
        date_from = timezone.now().date() - timedelta(days=days)
        
        # 일별 성과
        daily_data = []
        for i in range(days):
            target_date = date_from + timedelta(days=i)
            day_calls = CallRecord.objects.filter(
                caller=agent,
                call_date__date=target_date,
                is_deleted=False
            )
            
            daily_data.append({
                'date': target_date.isoformat(),
                'total': day_calls.count(),
                'connected': day_calls.filter(call_result='connected').count(),
                'interested': day_calls.filter(
                    interest_type__in=['insurance', 'maintenance', 'financing', 'multiple']
                ).count()
            })
        
        return JsonResponse({
            'success': True,
            'agent': {
                'id': agent.id,
                'name': agent.username,
                'team': agent.userprofile.team if hasattr(agent, 'userprofile') else None
            },
            'daily_data': daily_data
        })
        
    except User.DoesNotExist:
        return JsonResponse({'success': False, 'error': '상담원을 찾을 수 없습니다.'})