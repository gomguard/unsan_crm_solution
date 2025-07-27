# crm/management/commands/update_customer_tags.py
from django.core.management.base import BaseCommand
from crm.models import Customer

class Command(BaseCommand):
    help = '기존 고객 데이터의 태그와 우선순위 업데이트'

    def handle(self, *args, **options):
        customers = Customer.objects.all()
        total = customers.count()
        
        self.stdout.write(f'총 {total:,}명의 고객 태그를 업데이트합니다...')
        
        updated = 0
        for customer in customers:
            customer.update_priority_tags()
            customer.save()
            updated += 1
            
            if updated % 1000 == 0:
                progress = (updated / total) * 100
                self.stdout.write(f'진행률: {progress:.1f}% ({updated:,}/{total:,})')
        
        self.stdout.write(self.style.SUCCESS(f'✅ {updated:,}명 고객 태그 업데이트 완료!'))
        
        # 통계 출력
        stats = {
            '3개월콜 필요': Customer.objects.filter(needs_3month_call=True).count(),
            '6개월콜 필요': Customer.objects.filter(needs_6month_call=True).count(),
            '12개월콜 필요': Customer.objects.filter(needs_12month_call=True).count(),
            '18개월콜 필요': Customer.objects.filter(needs_18month_call=True).count(),
            '1회차 이탈': Customer.objects.filter(is_first_time_no_return=True).count(),
            '장기 이탈': Customer.objects.filter(is_long_term_absent=True).count(),
            '활성 고객': Customer.objects.filter(is_active_customer=True).count(),
        }
        
        self.stdout.write('\n📊 업데이트 결과:')
        for label, count in stats.items():
            self.stdout.write(f'  {label}: {count:,}명')