# crm/management/commands/create_sample_data.py
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from django.utils import timezone
from datetime import datetime, timedelta
from crm.models import UserProfile, Customer, CallRecord
import random

class Command(BaseCommand):
    help = '샘플 사용자 및 데이터 생성'

    def handle(self, *args, **options):
        self.stdout.write('샘플 데이터 생성 시작...')
        
        # 1. 관리자 생성
        admin, created = User.objects.get_or_create(
            username='admin',
            defaults={
                'email': 'admin@example.com',
                'first_name': '관리',
                'last_name': '자',
                'is_staff': True,
                'is_superuser': True,
            }
        )
        if created:
            admin.set_password('admin123')
            admin.save()
            UserProfile.objects.create(
                user=admin,
                role='admin',
                team='운영팀',
                daily_call_target=0
            )
            self.stdout.write(self.style.SUCCESS(f'✅ 관리자 생성: admin'))
        
        # 2. 팀장들 생성
        teams = ['영업1팀', '영업2팀', '영업3팀']
        managers = []
        
        for i, team_name in enumerate(teams, 1):
            manager, created = User.objects.get_or_create(
                username=f'manager{i}',
                defaults={
                    'email': f'manager{i}@example.com',
                    'first_name': f'팀장{i}',
                    'last_name': '김',
                    'is_staff': True,
                }
            )
            if created:
                manager.set_password('manager123')
                manager.save()
                UserProfile.objects.create(
                    user=manager,
                    role='manager',
                    team=team_name,
                    daily_call_target=50
                )
                self.stdout.write(self.style.SUCCESS(f'✅ 팀장 생성: manager{i} ({team_name})'))
            managers.append(manager)
        
        # 3. 상담원들 생성 (각 팀별 3명씩)
        agents = []
        agent_count = 1
        
        for team_name in teams:
            for j in range(3):
                agent, created = User.objects.get_or_create(
                    username=f'agent{agent_count}',
                    defaults={
                        'email': f'agent{agent_count}@example.com',
                        'first_name': f'상담원{agent_count}',
                        'last_name': '이',
                    }
                )
                if created:
                    agent.set_password('agent123')
                    agent.save()
                    UserProfile.objects.create(
                        user=agent,
                        role='agent',
                        team=team_name,
                        daily_call_target=100
                    )
                    self.stdout.write(self.style.SUCCESS(f'✅ 상담원 생성: agent{agent_count} ({team_name})'))
                agents.append(agent)
                agent_count += 1
        
        # 4. 샘플 고객 생성
        self.stdout.write('샘플 고객 생성 중...')
        customers = []
        names = ['김철수', '이영희', '박민수', '최지원', '정대한', '강미나', '조현우', '윤서연', '임도현', '한소희']
        vehicle_names = ['소나타', 'K5', '그랜저', '아반떼', '투싼', '스포티지', 'K3', '모닝', '카니발', '싼타페']
        grades = ['vip', 'regular', 'associate', 'new', '']
        
        today = timezone.now().date()
        
        for i in range(50):
            customer, created = Customer.objects.get_or_create(
                phone=f'010-{random.randint(1000,9999)}-{random.randint(1000,9999)}',
                vehicle_number=f'{random.randint(10,99)}{"가나다라마바사아자차"[random.randint(0,9)]}{random.randint(1000,9999)}',
                defaults={
                    'name': random.choice(names) + str(i),
                    'vehicle_name': random.choice(vehicle_names),
                    'vehicle_model': f'2024년형',
                    'customer_grade': random.choice(grades),
                    'visit_count': random.randint(0, 10),
                    'inspection_expiry_date': today + timedelta(days=random.randint(-180, 365)),
                    'status': random.choice(['pending', 'contacted', 'interested', 'not_interested', 'callback']),
                    'data_extracted_date': today,
                }
            )
            if created:
                # 실제 검사일 계산
                customer.calculate_inspection_date(today)
                customer.update_priority_tags()
                customer.save()
                customers.append(customer)
        
        self.stdout.write(self.style.SUCCESS(f'✅ {len(customers)}명의 고객 생성'))
        
        # 5. 오늘 통화 기록 생성
        self.stdout.write('오늘 통화 기록 생성 중...')
        all_users = list(managers) + list(agents)
        call_results = ['connected', 'no_answer', 'busy', 'callback_requested']
        interest_types = ['insurance', 'maintenance', 'financing', 'multiple', 'none', None]
        
        call_count = 0
        for user in all_users:
            # 각 사용자별로 10-30개의 통화 기록
            num_calls = random.randint(10, 30)
            for _ in range(num_calls):
                customer = random.choice(customers)
                CallRecord.objects.create(
                    customer=customer,
                    caller=user,
                    call_date=timezone.now() - timedelta(hours=random.randint(0, 8)),
                    call_result=random.choice(call_results),
                    interest_type=random.choice(interest_types),
                    notes=f'{user.username}의 상담 내용입니다. 고객과 통화했습니다.',
                    requires_follow_up=random.choice([True, False]),
                    follow_up_date=today + timedelta(days=random.randint(1, 7)) if random.choice([True, False]) else None,
                )
                call_count += 1
        
        self.stdout.write(self.style.SUCCESS(f'✅ {call_count}개의 통화 기록 생성'))
        
        # 6. 결과 출력
        self.stdout.write(self.style.SUCCESS(f"""
========================================
샘플 데이터 생성 완료!
========================================
생성된 계정:
- 관리자: admin / admin123
- 팀장: manager1~3 / manager123
- 상담원: agent1~9 / agent123

팀 구성:
- 영업1팀: manager1, agent1~3
- 영업2팀: manager2, agent4~6
- 영업3팀: manager3, agent7~9

생성된 데이터:
- 고객: {len(customers)}명
- 통화 기록: {call_count}건
========================================
        """))