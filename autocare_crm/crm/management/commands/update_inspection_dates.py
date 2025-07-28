# crm/management/commands/update_inspection_dates.py
from django.core.management.base import BaseCommand
from django.db import transaction
from crm.models import Customer
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = '모든 고객의 실제 검사일을 재계산하고 우선순위/태그를 업데이트합니다.'

    def add_arguments(self, parser):
        parser.add_argument(
            '--extract-date',
            type=str,
            help='데이터 추출일 (YYYY-MM-DD 형식). 기본값: 오늘',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='실제로 저장하지 않고 변경사항만 확인',
        )
        parser.add_argument(
            '--customer-id',
            type=int,
            help='특정 고객 ID만 처리',
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='처리할 최대 고객 수',
        )

    def handle(self, *args, **options):
        extract_date_str = options.get('extract_date')
        dry_run = options.get('dry_run')
        customer_id = options.get('customer_id')
        limit = options.get('limit')
        
        # 추출일 파싱
        if extract_date_str:
            try:
                extract_date = datetime.strptime(extract_date_str, '%Y-%m-%d').date()
            except ValueError:
                self.stdout.write(self.style.ERROR('날짜 형식이 올바르지 않습니다. YYYY-MM-DD 형식을 사용하세요.'))
                return
        else:
            extract_date = datetime.now().date()
        
        self.stdout.write(f'데이터 추출일: {extract_date}')
        
        # 고객 쿼리
        if customer_id:
            customers = Customer.objects.filter(id=customer_id)
            if not customers.exists():
                self.stdout.write(self.style.ERROR(f'고객 ID {customer_id}를 찾을 수 없습니다.'))
                return
        else:
            customers = Customer.objects.all()
        
        if limit:
            customers = customers[:limit]
        
        total_count = customers.count()
        self.stdout.write(f'처리할 고객 수: {total_count}명')
        
        if dry_run:
            self.stdout.write(self.style.WARNING('DRY RUN 모드 - 실제로 저장되지 않습니다.'))
        
        # 통계
        updated_count = 0
        error_count = 0
        changes = {
            'inspection_dates_calculated': 0,
            'priorities_changed': 0,
            'tags_updated': 0,
            'happy_calls_updated': 0,
        }
        
        with transaction.atomic():
            for i, customer in enumerate(customers, 1):
                try:
                    old_data = {
                        'actual_inspection_date': customer.actual_inspection_date,
                        'priority': customer.priority,
                        'is_inspection_overdue': customer.is_inspection_overdue,
                        'needs_3month_call': customer.needs_3month_call,
                        'needs_6month_call': customer.needs_6month_call,
                        'needs_12month_call': customer.needs_12month_call,
                        'needs_18month_call': customer.needs_18month_call,
                    }
                    
                    # 실제 검사일 계산
                    if customer.inspection_expiry_date:
                        customer.calculate_inspection_date(extract_date)
                        if old_data['actual_inspection_date'] != customer.actual_inspection_date:
                            changes['inspection_dates_calculated'] += 1
                    
                    # 우선순위와 태그 업데이트
                    customer.update_priority_tags()
                    
                    # 변경사항 확인
                    if old_data['priority'] != customer.priority:
                        changes['priorities_changed'] += 1
                    
                    if old_data['is_inspection_overdue'] != customer.is_inspection_overdue:
                        changes['tags_updated'] += 1
                    
                    if (old_data['needs_3month_call'] != customer.needs_3month_call or
                        old_data['needs_6month_call'] != customer.needs_6month_call or
                        old_data['needs_12month_call'] != customer.needs_12month_call or
                        old_data['needs_18month_call'] != customer.needs_18month_call):
                        changes['happy_calls_updated'] += 1
                    
                    if not dry_run:
                        customer.save()
                    
                    updated_count += 1
                    
                    # 진행 상황 출력
                    if i % 100 == 0:
                        self.stdout.write(f'진행 중... {i}/{total_count} ({i/total_count*100:.1f}%)')
                    
                    # 상세 정보 출력 (단일 고객 또는 처음 10명)
                    if customer_id or i <= 10:
                        self.stdout.write(f'\n고객: {customer.name} (ID: {customer.id})')
                        if customer.inspection_expiry_date:
                            self.stdout.write(f'  - 검사만료일: {customer.inspection_expiry_date}')
                            self.stdout.write(f'  - 실제검사일: {customer.actual_inspection_date}')
                        self.stdout.write(f'  - 우선순위: {old_data["priority"]} → {customer.priority}')
                        self.stdout.write(f'  - 검사만료: {old_data["is_inspection_overdue"]} → {customer.is_inspection_overdue}')
                        if customer.needs_3month_call:
                            self.stdout.write('  - 3개월 해피콜 필요')
                        if customer.needs_6month_call:
                            self.stdout.write('  - 6개월 해피콜 필요')
                        if customer.needs_12month_call:
                            self.stdout.write('  - 12개월 해피콜 필요')
                        if customer.needs_18month_call:
                            self.stdout.write('  - 18개월 해피콜 필요')
                
                except Exception as e:
                    error_count += 1
                    self.stdout.write(self.style.ERROR(f'고객 {customer.id} 처리 중 오류: {str(e)}'))
                    logger.error(f'고객 {customer.id} 처리 오류', exc_info=True)
            
            if dry_run:
                transaction.set_rollback(True)
        
        # 결과 요약
        self.stdout.write('\n' + '='*50)
        self.stdout.write(self.style.SUCCESS('처리 완료!'))
        self.stdout.write(f'- 총 고객 수: {total_count}명')
        self.stdout.write(f'- 성공: {updated_count}명')
        self.stdout.write(f'- 오류: {error_count}명')
        self.stdout.write('\n변경 내역:')
        self.stdout.write(f'- 실제 검사일 계산: {changes["inspection_dates_calculated"]}건')
        self.stdout.write(f'- 우선순위 변경: {changes["priorities_changed"]}건')
        self.stdout.write(f'- 태그 업데이트: {changes["tags_updated"]}건')
        self.stdout.write(f'- 해피콜 대상 업데이트: {changes["happy_calls_updated"]}건')
        
        if dry_run:
            self.stdout.write(self.style.WARNING('\nDRY RUN 모드였으므로 실제로 저장되지 않았습니다.'))
            self.stdout.write('실제로 적용하려면 --dry-run 옵션 없이 다시 실행하세요.')