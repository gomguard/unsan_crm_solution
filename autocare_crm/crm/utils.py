# crm/utils.py
import pandas as pd
from django.utils import timezone
from datetime import datetime, date
import re

def clean_phone_number(phone):
    """전화번호 정제"""
    if not phone:
        return ''
    # 숫자만 추출
    phone = re.sub(r'[^\d]', '', str(phone))
    # 010으로 시작하는 11자리 휴대폰 번호 형식으로 변환
    if len(phone) == 11 and phone.startswith('010'):
        return f"{phone[:3]}-{phone[3:7]}-{phone[7:]}"
    elif len(phone) == 10 and phone.startswith('01'):
        return f"0{phone[:2]}-{phone[2:6]}-{phone[6:]}"
    return phone

def parse_excel_date(date_value):
    """엑셀 날짜 파싱"""
    if pd.isna(date_value) or not date_value:
        return None
    
    if isinstance(date_value, datetime):
        return date_value.date()
    elif isinstance(date_value, str):
        try:
            return datetime.strptime(date_value, '%Y-%m-%d').date()
        except:
            return None
    return None

def map_customer_grade(grade_str):
    """고객등급 매핑"""
    if not grade_str:
        return ''
    
    grade_mapping = {
        'VIP': 'vip',
        'vip': 'vip',
        '정회원': 'regular',
        '준회원': 'associate',
        '신규': 'new',
    }
    return grade_mapping.get(grade_str, '')

# crm/management/commands/import_excel_data.py
from django.core.management.base import BaseCommand
from django.db import transaction
from crm.models import Customer, UploadHistory
from crm.utils import clean_phone_number, parse_excel_date, map_customer_grade
import pandas as pd
import numpy as np

class Command(BaseCommand):
    help = '엑셀 파일에서 고객 데이터 임포트'

    def add_arguments(self, parser):
        parser.add_argument('file_path', type=str, help='엑셀 파일 경로')
        parser.add_argument('--dry-run', action='store_true', help='실제 저장하지 않고 테스트만')

    def handle(self, *args, **options):
        file_path = options['file_path']
        dry_run = options['dry_run']
        
        self.stdout.write(f'엑셀 파일 읽는 중: {file_path}')
        
        try:
            # 엑셀 파일 읽기
            df = pd.read_excel(file_path, sheet_name='고객')
            self.stdout.write(f'총 {len(df)}개 행 발견')
            
            # 컬럼명 매핑
            column_mapping = {
                '고객명': 'name',
                '휴대전화': 'phone',
                '전화번호': 'landline',
                '생년월일': 'birth_date',
                '주소': 'address',
                '우편번호': 'postal_code',
                '고객등급': 'customer_grade',
                '이메일': 'email',
                '방문수': 'visit_count',
                '차량번호': 'vehicle_number',
                '차량명': 'vehicle_name',
                '모델명': 'vehicle_model',
                '차량등록일': 'vehicle_registration_date',
                '검사만료일': 'inspection_expiry_date',
                '보험만기일': 'insurance_expiry_date',
                '오일교환일': 'oil_change_date',
                '차대번호': 'chassis_number',
                '소속회사': 'company'
            }
            
            # 필요한 컬럼만 선택하고 이름 변경
            df = df.rename(columns=column_mapping)
            
            new_count = 0
            updated_count = 0
            error_count = 0
            errors = []
            
            with transaction.atomic():
                for index, row in df.iterrows():
                    try:
                        # 필수 필드 검증
                        phone = clean_phone_number(row.get('phone', ''))
                        vehicle_number = str(row.get('vehicle_number', '')).strip()
                        name = str(row.get('name', '')).strip()
                        
                        if not phone or not vehicle_number or not name:
                            error_count += 1
                            errors.append(f"행 {index+2}: 필수 정보 누락 (이름: {name}, 폰: {phone}, 차량번호: {vehicle_number})")
                            continue
                        
                        # 고객 데이터 준비
                        customer_data = {
                            'name': name,
                            'phone': phone,
                            'landline': clean_phone_number(row.get('landline', '')),
                            'birth_date': parse_excel_date(row.get('birth_date')),
                            'address': str(row.get('address', '')).strip(),
                            'postal_code': str(row.get('postal_code', '')).strip(),
                            'email': str(row.get('email', '')).strip(),
                            'vehicle_number': vehicle_number,
                            'vehicle_name': str(row.get('vehicle_name', '')).strip(),
                            'vehicle_model': str(row.get('vehicle_model', '')).strip(),
                            'vehicle_registration_date': parse_excel_date(row.get('vehicle_registration_date')),
                            'inspection_expiry_date': parse_excel_date(row.get('inspection_expiry_date')),
                            'insurance_expiry_date': parse_excel_date(row.get('insurance_expiry_date')),
                            'oil_change_date': parse_excel_date(row.get('oil_change_date')),
                            'chassis_number': str(row.get('chassis_number', '')).strip(),
                            'company': str(row.get('company', '')).strip(),
                            'customer_grade': map_customer_grade(row.get('customer_grade', '')),
                            'visit_count': int(row.get('visit_count', 0)) if pd.notna(row.get('visit_count')) else 0,
                        }
                        
                        # 고객 생성 또는 업데이트
                        customer, created = Customer.objects.update_or_create(
                            phone=phone,
                            vehicle_number=vehicle_number,
                            defaults=customer_data
                        )
                        
                        # 우선순위와 태그 업데이트
                        customer.update_priority_tags()
                        customer.save()
                        
                        if created:
                            new_count += 1
                        else:
                            updated_count += 1
                            
                        if (new_count + updated_count) % 1000 == 0:
                            self.stdout.write(f'처리 중... {new_count + updated_count}건 완료')
                            
                    except Exception as e:
                        error_count += 1
                        errors.append(f"행 {index+2}: {str(e)}")
                        continue
                
                if dry_run:
                    self.stdout.write(self.style.WARNING('DRY RUN 모드 - 실제로 저장되지 않음'))
                    transaction.set_rollback(True)
                else:
                    # 업로드 이력 저장
                    UploadHistory.objects.create(
                        uploaded_by_id=1,  # 관리자 계정으로 가정
                        file_name=file_path.split('/')[-1],
                        total_records=new_count + updated_count,
                        new_records=new_count,
                        updated_records=updated_count,
                        error_count=error_count,
                        notes=f"엑셀 임포트 완료. 오류: {len(errors)}건"
                    )
            
            # 결과 출력
            self.stdout.write(self.style.SUCCESS(f"""
임포트 완료!
- 신규: {new_count}건
- 업데이트: {updated_count}건
- 오류: {error_count}건
"""))
            
            if errors:
                self.stdout.write(self.style.ERROR("오류 상세:"))
                for error in errors[:10]:  # 처음 10개만 출력
                    self.stdout.write(f"  {error}")
                if len(errors) > 10:
                    self.stdout.write(f"  ... 및 {len(errors)-10}개 추가 오류")
                    
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'파일 처리 중 오류: {str(e)}'))
