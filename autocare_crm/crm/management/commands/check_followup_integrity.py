# management/commands/check_followup_integrity.py
from django.core.management.base import BaseCommand
from crm.models import CallRecord
from django.db.models import Q

class Command(BaseCommand):
    help = '후속조치 데이터 정합성 체크 및 수정'

    def handle(self, *args, **options):
        # 1. 후속조치가 실행되었지만 완료 처리 안 된 건 찾기
        inconsistent = 0
        fixed = 0
        
        # parent_call이 있는 모든 통화
        followup_calls = CallRecord.objects.filter(
            parent_call__isnull=False,
            is_deleted=False
        ).select_related('parent_call')
        
        for call in followup_calls:
            parent = call.parent_call
            if parent and parent.requires_follow_up and not parent.follow_up_completed:
                parent.follow_up_completed = True
                parent.save()
                fixed += 1
                self.stdout.write(
                    f"수정: {parent.customer.name}의 통화 ID {parent.id}"
                )
        
        # 2. 통계 출력
        self.stdout.write(self.style.SUCCESS(f"\n수정 완료: {fixed}건"))
        
        # 3. 최종 통계
        total = CallRecord.objects.filter(
            requires_follow_up=True, is_deleted=False
        ).count()
        completed = CallRecord.objects.filter(
            requires_follow_up=True, 
            follow_up_completed=True, 
            is_deleted=False
        ).count()
        
        self.stdout.write(f"\n최종 통계:")
        self.stdout.write(f"- 전체 후속조치 필요: {total}")
        self.stdout.write(f"- 완료: {completed}")
        self.stdout.write(f"- 미완료: {total - completed}")
        self.stdout.write(f"- 완료율: {round(completed/total*100, 1)}%")