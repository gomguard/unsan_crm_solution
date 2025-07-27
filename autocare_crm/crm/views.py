# crm/views.py
from django.shortcuts import render, get_object_or_404, redirect
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.core.paginator import Paginator
from django.db.models import Q, Count
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

from .models import Customer, CallRecord, UploadHistory, UserProfile, CallFollowUp
from .forms import CallRecordForm, CustomerUploadForm

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
    
    # 오늘 이미 통화한 고객 ID 목록
    today_called_customer_ids = today_calls.values_list('customer_id', flat=True).distinct()
    
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
    
    vip_customers = Customer.objects.filter(customer_grade='vip').count()
    
    # ===== 해피콜 관련 통계 (개선) =====
    # 3개월콜 대상
    happy_call_3month_total = Customer.objects.filter(needs_3month_call=True).count()
    happy_call_3month_remaining = Customer.objects.filter(
        needs_3month_call=True
    ).exclude(id__in=today_called_customer_ids).count()
    happy_call_3month_completed = happy_call_3month_total - happy_call_3month_remaining
    
    # 6개월콜 대상
    happy_call_6month_total = Customer.objects.filter(needs_6month_call=True).count()
    happy_call_6month_remaining = Customer.objects.filter(
        needs_6month_call=True
    ).exclude(id__in=today_called_customer_ids).count()
    happy_call_6month_completed = happy_call_6month_total - happy_call_6month_remaining
    
    # 12개월콜 대상
    happy_call_12month_total = Customer.objects.filter(needs_12month_call=True).count()
    happy_call_12month_remaining = Customer.objects.filter(
        needs_12month_call=True
    ).exclude(id__in=today_called_customer_ids).count()
    happy_call_12month_completed = happy_call_12month_total - happy_call_12month_remaining
    
    # 18개월콜 대상
    happy_call_18month_total = Customer.objects.filter(needs_18month_call=True).count()
    happy_call_18month_remaining = Customer.objects.filter(
        needs_18month_call=True
    ).exclude(id__in=today_called_customer_ids).count()
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
    
    # VIP 고객 (추가)
    vip_customers_total = Customer.objects.filter(customer_grade='vip').count()
    vip_customers_remaining = Customer.objects.filter(
        customer_grade='vip'
    ).exclude(id__in=today_called_customer_ids).count()
    vip_customers_completed = vip_customers_total - vip_customers_remaining
    
    # ===== 오늘의 통화 대상자 통계 =====
    # 해피콜 대상자들 (오늘 통화하지 않은)
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
    
    # ===== 후속조치 관련 통계 =====
    # 1. 후속조치가 필요한 전체 통화
    followup_required_total = CallRecord.objects.filter(
        requires_follow_up=True,
        is_deleted=False
    ).count()
    
    # 2. 완료된 후속조치
    followup_completed_total = CallRecord.objects.filter(
        requires_follow_up=True,
        follow_up_completed=True,
        is_deleted=False
    ).count()
    
    # 3. 미완료 후속조치
    followup_pending = followup_required_total - followup_completed_total
    
    # 4. 오늘 처리해야 할 후속조치
    followup_due_today = CallRecord.objects.filter(
        follow_up_date=today,
        follow_up_completed=False,
        is_deleted=False
    ).count()
    
    # 5. 기한이 지난 후속조치
    followup_overdue = CallRecord.objects.filter(
        follow_up_date__lt=today,
        follow_up_completed=False,
        requires_follow_up=True,
        is_deleted=False
    ).count()
    
    # 6. 오늘 실행한 후속조치
    followup_calls_today = CallRecord.objects.filter(
        parent_call__isnull=False,
        call_date__date=today,
        is_deleted=False
    ).count()
    
    # 7. 후속조치 완료율
    followup_completion_rate = 0
    if followup_required_total > 0:
        followup_completion_rate = round(
            (followup_completed_total / followup_required_total) * 100, 1
        )
    
    # 8. 미완료 후속조치 목록
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
                connected_calls=Count('id', filter=Q(call_result='connected')),
                converted_calls=Count('id', filter=Q(is_converted=True))
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
    
    # 계약성사률 계산
    today_conversions = today_connected.filter(is_converted=True).count()
    conversion_rate = 0
    if today_connected.count() > 0:
        conversion_rate = round((today_conversions / today_connected.count()) * 100, 1)

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
        'today_conversions': today_conversions,
        'today_conversion_rate': conversion_rate,
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
    customers = Customer.objects.all().order_by('-updated_at')
    
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
    
    # 해피콜 필터
    happy_call_filter = request.GET.get('happy_call', '')
    if happy_call_filter == '3month':
        customers = customers.filter(needs_3month_call=True)
    elif happy_call_filter == '6month':
        customers = customers.filter(needs_6month_call=True)
    elif happy_call_filter == '12month':
        customers = customers.filter(needs_12month_call=True)
    elif happy_call_filter == '18month':
        customers = customers.filter(needs_18month_call=True)
    
    # 고객등급 필터
    grade_filter = request.GET.get('grade', '')
    if grade_filter:
        customers = customers.filter(customer_grade=grade_filter)
    
    # 재방문 고객 필터 (추가)
    visit_count_filter = request.GET.get('visit_count', '')
    if visit_count_filter:
        try:
            min_visits = int(visit_count_filter)
            customers = customers.filter(visit_count__gte=min_visits)
        except ValueError:
            pass
    
    # 단골고객 필터 (기존)
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
        'visit_count_filter': visit_count_filter,  # 추가
        'status_choices': Customer.STATUS_CHOICES,
        'today': timezone.now().date(),
    }
    context.update(sidebar_stats)  # 사이드바 통계 추가
    
    return render(request, 'customer_list.html', context)


@login_required
def customer_detail(request, pk):
    """고객 상세 정보"""
    customer = get_object_or_404(Customer, pk=pk)
    # 삭제되지 않은 통화 기록 중 부모 통화가 없는 것만 가져오기 (후속조치 제외)
    call_records = customer.call_records.filter(
        is_deleted=False,
        parent_call__isnull=True  # 이 조건 추가
    ).order_by('-call_date')[:20]
    
    # 사이드바 통계 추가
    sidebar_stats = get_sidebar_stats()
    
    context = {
        'customer': customer,
        'call_records': call_records,
        'today': timezone.now().date(),
    }
    context.update(sidebar_stats)  # 사이드바 통계 추가
    
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
                    'error': '통화 결과를 선택해주세요.'
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
                    requires_follow_up=request.POST.get('requires_follow_up') == 'on',
                    follow_up_date=request.POST.get('follow_up_date') or None,
                    follow_up_notes=request.POST.get('follow_up_memo', ''),
                    parent_call_id=request.POST.get('parent_call_id') or None,
                    is_converted=request.POST.get('is_converted') == 'on',
                    conversion_amount=request.POST.get('conversion_amount') or None
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
                    
                    # follow_up_date 조건 제거 - 모든 미완료 후속조치를 완료 처리
                    # follow_up_date__lte=today를 제거하면 날짜와 관계없이 처리
                    
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
                    
                    # 계약성사인 경우
                    if call_record.is_converted:
                        customer.status = 'converted'
                    
                    customer.save()
                    print(f"고객 상태 업데이트: {customer.name} → {customer.status}")
                
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
        is_deleted=False
    ).select_related('customer', 'caller').order_by('-call_date')
    
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
    
    # 통화결과 필터
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
    elif filter_type == 'converted':
        records = records.filter(is_converted=True)
    
    # 통계 계산
    total_calls = records.count()
    connected_calls = records.filter(call_result='connected').count()
    follow_up_calls = records.filter(requires_follow_up=True).count()
    conversions = records.filter(is_converted=True).count()
    
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
        'conversions': conversions,
        'today': today,
    }
    
    context.update(sidebar_stats)

    return render(request, 'call_records.html', context)


@login_required
def upload_data(request):
    """CSV/Excel 데이터 업로드"""
    
    def process_batch(batch_data):
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
                            
                            # 배치 처리
                            if len(batch_data) >= batch_size:
                                batch_new, batch_updated = process_batch(batch_data)
                                new_count += batch_new
                                updated_count += batch_updated
                                batch_data = []
                                
                        except Exception as e:
                            error_count += 1
                            continue
                    
                    # 남은 데이터 처리
                    if batch_data:
                        batch_new, batch_updated = process_batch(batch_data)
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
                    notes=f"웹 업로드 완료. 총 {total_rows:,}행 처리"
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
            if request.POST.get('follow_up_action') in ['converted', 'closed']:
                call_record.follow_up_completed = True
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
    """사이드바 통계 API"""
    today = timezone.now().date()
    
    # 오늘 통화 수
    today_total_calls = CallRecord.objects.filter(
        call_date__date=today,
        is_deleted=False
    ).count()
    
    # 미완료 후속조치
    pending_follow_ups = CallRecord.objects.filter(
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
        'today_calls': today_total_calls,  # JavaScript와 일치하도록 수정
        'pending_followups': pending_follow_ups,  # JavaScript와 일치하도록 수정
        'overdue_customers': overdue_customers  # 이미 일치함
    })