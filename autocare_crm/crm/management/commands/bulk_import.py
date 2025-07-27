# crm/management/commands/bulk_import.py
from django.core.management.base import BaseCommand
from django.db import transaction
from crm.models import Customer, UploadHistory
from django.contrib.auth.models import User
import pandas as pd
import re
from datetime import datetime

class Command(BaseCommand):
    help = '대용량 엑셀 파일을 안전하게 업로드'

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help='엑셀 파일 경로')
        parser.add_argument('--batch-size', type=int, default=500, help='배치 크기 (기본값: 500)')
        parser.add_argument('--dry-run', action='store_true', help='실제 저장하지 않고 테스트만')

    def clean_phone_number(self, phone):
        """전화번호 정제"""
        if not phone or pd.isna(phone):
            return ''
        phone = re.sub(r'[^\d]', '', str(phone))
        if len(phone) == 11 and phone.startswith('010'):
            return f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
        elif len(phone) == 10 and phone.startswith('01'):
            return f"0{phone[:2]}-{phone[2:6]}-{phone[6:]}"
        return phone

    def parse_date(self, date_value):
        """날짜 파싱"""
        if pd.isna(date_value) or not date_value:
            return None
        if hasattr(date_value, 'date'):
            return date_value.date()
        return None

    def map_customer_grade(self, grade_str):
        """고객등급 매핑"""
        if not grade_str or pd.isna(grade_str):
            return ''
        grade_mapping = {
            'VIP': 'vip', 'vip': 'vip',
            '정회원': 'regular', '준회원': 'associate',
            '신규': 'new',
        }
        return grade_mapping.get(str(grade_str).strip(), '')

    def process_batch(self, batch_data):
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
                    
            except Exception as e:
                self.stdout.write(f'❌ 오류: {phone}, {vehicle_number} - {str(e)}')
                continue
                
        return batch_new, batch_updated

    def handle(self, *args, **options):
        file_path = options['file_path']
        batch_size = options['batch_size']
        dry_run = options['dry_run']
        
        self.stdout.write(f'📂 파일 읽는 중: {file_path}')
        
        try:
            # 엑셀 파일 읽기
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path, encoding='utf-8')
            else:
                df = pd.read_excel(file_path, sheet_name='고객')
                
            total_rows = len(df)
            self.stdout.write(f'📊 총 {total_rows:,}개 행 발견')
            
            new_count = 0
            updated_count = 0
            error_count = 0
            batch_data = []
            
            if dry_run:
                self.stdout.write(self.style.WARNING('🧪 DRY RUN 모드 - 실제로 저장하지 않습니다'))
            
            # 데이터 처리
            for index, row in df.iterrows():
                try:
                    # 진행률 표시
                    if (index + 1) % 1000 == 0:
                        progress = ((index + 1) / total_rows) * 100
                        self.stdout.write(f'⏳ 진행률: {progress:.1f}% ({index + 1:,}/{total_rows:,})')
                    
                    # 필수 필드 검증
                    name = str(row.get('고객명', '')).strip()
                    phone = self.clean_phone_number(row.get('휴대전화', ''))
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
                        'inspection_expiry_date': self.parse_date(row.get('검사만료일')),
                        'insurance_expiry_date': self.parse_date(row.get('보험만기일')),
                        'vehicle_registration_date': self.parse_date(row.get('차량등록일')),
                        'customer_grade': self.map_customer_grade(row.get('고객등급', '')),
                        'visit_count': int(row.get('방문수', 0)) if pd.notna(row.get('방문수')) else 0,
                        'last_inspection_completed': self.parse_date(row.get('검사만료일')),  # 임시로 설정
                    }
                    
                    batch_data.append((phone, vehicle_number, customer_data))
                    
                    # 배치 처리
                    if len(batch_data) >= batch_size:
                        if not dry_run:
                            with transaction.atomic():
                                batch_new, batch_updated = self.process_batch(batch_data)
                                new_count += batch_new
                                updated_count += batch_updated
                        else:
                            new_count += len(batch_data)  # dry run에서는 모두 새로운 것으로 가정
                        
                        batch_data = []
                        
                except Exception as e:
                    error_count += 1
                    continue
            
            # 남은 데이터 처리
            if batch_data:
                if not dry_run:
                    with transaction.atomic():
                        batch_new, batch_updated = self.process_batch(batch_data)
                        new_count += batch_new
                        updated_count += batch_updated
                else:
                    new_count += len(batch_data)
            
            # 결과 출력
            self.stdout.write(self.style.SUCCESS(f"""
🎉 {'테스트' if dry_run else '업로드'} 완료!

📊 처리 결과:
  - 신규: {new_count:,}건
  - 업데이트: {updated_count:,}건  
  - 오류: {error_count:,}건
  - 총 처리: {new_count + updated_count:,}건

⏱️  배치 크기: {batch_size}개씩 처리
"""))
            
            # 업로드 이력 저장 (실제 업로드인 경우)
            if not dry_run:
                try:
                    admin_user = User.objects.filter(is_superuser=True).first()
                    if admin_user:
                        UploadHistory.objects.create(
                            uploaded_by=admin_user,
                            file_name=file_path.split('/')[-1],
                            total_records=new_count + updated_count,
                            new_records=new_count,
                            updated_records=updated_count,
                            error_count=error_count,
                            notes=f"명령어 대량 업로드 완료 (배치크기: {batch_size})"
                        )
                except Exception:
                    pass  # 이력 저장 실패해도 무시
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ 파일 처리 중 오류: {str(e)}'))