# crm/models.py
from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone

class Customer(models.Model):
    # 기본 정보
    name = models.CharField(max_length=50, verbose_name='고객명')
    phone = models.CharField(max_length=20, db_index=True, verbose_name='휴대전화')
    landline = models.CharField(max_length=20, blank=True, verbose_name='전화번호')
    birth_date = models.DateField(null=True, blank=True, verbose_name='생년월일')
    address = models.TextField(blank=True, verbose_name='주소')
    postal_code = models.CharField(max_length=10, blank=True, verbose_name='우편번호')
    email = models.EmailField(blank=True, verbose_name='이메일')
    
    # 차량 정보
    vehicle_number = models.CharField(max_length=20, db_index=True, verbose_name='차량번호')
    vehicle_name = models.CharField(max_length=100, blank=True, verbose_name='차량명')
    vehicle_model = models.CharField(max_length=100, blank=True, verbose_name='모델명')
    vehicle_registration_date = models.DateField(null=True, blank=True, verbose_name='차량등록일')
    chassis_number = models.CharField(max_length=50, blank=True, verbose_name='차대번호')
    
    # 검사 관련
    inspection_expiry_date = models.DateField(null=True, blank=True, verbose_name='검사만료일')
    last_inspection_completed = models.DateField(null=True, blank=True, verbose_name='최근검사완료일')  # 새로 추가
    insurance_expiry_date = models.DateField(null=True, blank=True, verbose_name='보험만기일')
    oil_change_date = models.DateField(null=True, blank=True, verbose_name='오일교환일')
    
    # 고객 등급 및 이력
    GRADE_CHOICES = [
        ('vip', 'VIP'),
        ('regular', '정회원'),
        ('associate', '준회원'),
        ('new', '신규'),
        ('', '등급없음'),
    ]
    customer_grade = models.CharField(max_length=20, choices=GRADE_CHOICES, blank=True, verbose_name='고객등급')
    visit_count = models.IntegerField(default=0, verbose_name='방문수')
    company = models.CharField(max_length=100, blank=True, verbose_name='소속회사')
    
    # 상태 관리
    STATUS_CHOICES = [
        ('pending', '미접촉'),
        ('contacted', '접촉완료'),
        ('interested', '관심있음'),
        ('not_interested', '관심없음'),
        ('callback', '재통화예정'),
        ('converted', '계약성사'),
        ('do_not_call', '통화거부'),
    ]
    # 기존 STATUS_CHOICES 바로 아래에 추가
    # 통화 금지 관련 필드 추가
    is_do_not_call = models.BooleanField(default=False, verbose_name='통화금지')
    do_not_call_reason = models.TextField(blank=True, verbose_name='통화금지사유')
    do_not_call_date = models.DateTimeField(null=True, blank=True, verbose_name='통화금지등록일')
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default='pending', verbose_name='상태')
    
    # 태그 및 우선순위
    PRIORITY_CHOICES = [
        ('high', '높음'),
        ('medium', '보통'),
        ('low', '낮음'),
    ]
    priority = models.CharField(max_length=10, choices=PRIORITY_CHOICES, default='medium', verbose_name='우선순위')
    
    # 태그 필드들
    is_inspection_overdue = models.BooleanField(default=False, verbose_name='검사만료')
    is_frequent_visitor = models.BooleanField(default=False, verbose_name='단골고객')  # 방문수 3회 이상
    has_premium_vehicle = models.BooleanField(default=False, verbose_name='프리미엄차량')  # VIP나 고급차량
    
    # 이탈 고객 분류 (새로 추가)
    is_first_time_no_return = models.BooleanField(default=False, verbose_name='1회차이탈')  # 1회 방문 후 2년+ 미방문
    is_long_term_absent = models.BooleanField(default=False, verbose_name='장기이탈')      # 4년+ 미방문 (폐차 추정)
    is_active_customer = models.BooleanField(default=True, verbose_name='활성고객')        # 관리 대상 여부
    
    # 해피콜 관련
    needs_3month_call = models.BooleanField(default=False, verbose_name='3개월콜필요')
    needs_6month_call = models.BooleanField(default=False, verbose_name='6개월콜필요')
    needs_12month_call = models.BooleanField(default=False, verbose_name='12개월콜필요')
    needs_18month_call = models.BooleanField(default=False, verbose_name='18개월콜필요')
    last_happy_call_date = models.DateField(null=True, blank=True, verbose_name='최근해피콜일')
    
    # 고객 상태 추가
    CUSTOMER_STATUS_CHOICES = [
        ('active', '활성고객'),
        ('first_time_lost', '1회차이탈'),
        ('long_term_lost', '장기이탈'),
        ('possibly_scrapped', '폐차추정'),
    ]
    customer_status = models.CharField(max_length=20, choices=CUSTOMER_STATUS_CHOICES, default='active', verbose_name='고객상태')
    
    # 메타 정보
    data_source = models.CharField(max_length=20, default='import', verbose_name='데이터출처')
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = '고객'
        verbose_name_plural = '고객들'
        indexes = [
            models.Index(fields=['phone', 'vehicle_number']),  # 복합 고유 식별자
            models.Index(fields=['status']),
            models.Index(fields=['inspection_expiry_date']),
            models.Index(fields=['priority']),
            models.Index(fields=['customer_grade']),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=['phone', 'vehicle_number'], 
                name='unique_customer_vehicle'
            )
        ]
        
    def __str__(self):
        return f"{self.name} ({self.phone}) - {self.vehicle_number}"
    
    @property
    def is_inspection_due_soon(self):
        """검사가 3개월 이내 만료인지 확인"""
        if not self.inspection_expiry_date:
            return False
        from datetime import timedelta
        return self.inspection_expiry_date <= timezone.now().date() + timedelta(days=90)
    
    @property
    def inspection_status(self):
        """검사 상태 반환"""
        if not self.inspection_expiry_date:
            return 'unknown'
        
        today = timezone.now().date()
        if self.inspection_expiry_date < today:
            return 'overdue'  # 만료됨
        elif self.inspection_expiry_date <= today + timedelta(days=90):
            return 'due_soon'  # 3개월 이내 만료
        else:
            return 'valid'  # 유효
    
    def update_priority_tags(self):
        """우선순위와 태그 자동 업데이트"""
        from datetime import timedelta
        today = timezone.now().date()
        
        # 검사 만료 태그
        if self.inspection_expiry_date and self.inspection_expiry_date < today:
            self.is_inspection_overdue = True
            self.priority = 'high'
        elif self.is_inspection_due_soon:
            self.priority = 'high' if self.priority == 'low' else self.priority
        
        # 단골고객 태그 (방문수 3회 이상)
        self.is_frequent_visitor = self.visit_count >= 3
        
        # 프리미엄 차량 태그 (VIP 고객)
        self.has_premium_vehicle = self.customer_grade == 'vip'
        
        # 이탈 고객 분류
        self.classify_customer_status()
        
        # 해피콜 필요 체크 (활성 고객만)
        if self.is_active_customer:
            self.update_happy_call_needs()
        
        # 우선순위 조정
        if self.is_inspection_overdue and self.is_active_customer:
            self.priority = 'high'
        elif self.is_frequent_visitor and self.priority == 'low':
            self.priority = 'medium'
        elif not self.is_active_customer:
            self.priority = 'low'  # 이탈 고객은 낮은 우선순위
    
    def classify_customer_status(self):
        """고객 상태 분류"""
        if not self.inspection_expiry_date:
            return
            
        from datetime import timedelta
        today = timezone.now().date()
        
        # 검사만료일로부터 경과 기간 계산
        days_overdue = (today - self.inspection_expiry_date).days
        
        if days_overdue > 1460:  # 4년 이상 (1460일)
            self.is_long_term_absent = True
            self.is_active_customer = False
            self.customer_status = 'possibly_scrapped'  # 폐차 추정
            
        elif days_overdue > 730 and self.visit_count == 1:  # 2년 이상 + 1회 방문
            self.is_first_time_no_return = True
            self.is_active_customer = False
            self.customer_status = 'first_time_lost'  # 1회차 이탈
            
        elif days_overdue > 730:  # 2년 이상 (기존 고객)
            self.is_long_term_absent = True
            self.is_active_customer = False
            self.customer_status = 'long_term_lost'  # 장기 이탈
            
        else:
            self.is_active_customer = True
            self.customer_status = 'active'  # 활성 고객
    
    def update_happy_call_needs(self):
        """해피콜 필요 여부 업데이트 (활성 고객만)"""
        if not self.last_inspection_completed or not self.is_active_customer:
            return
            
        from datetime import timedelta
        today = timezone.now().date()
        
        # 검사 완료일로부터 경과 기간 계산
        days_since_inspection = (today - self.last_inspection_completed).days
        
        # 각 시점별 해피콜 필요 여부
        if 90 <= days_since_inspection < 180:  # 3개월
            self.needs_3month_call = True
        elif 180 <= days_since_inspection < 365:  # 6개월
            self.needs_6month_call = True
        elif 365 <= days_since_inspection < 540:  # 12개월
            self.needs_12month_call = True
        elif 540 <= days_since_inspection < 730:  # 18개월
            self.needs_18month_call = True
    
    @property
    def customer_lifecycle_stage(self):
        """고객 생애주기 단계"""
        if self.visit_count == 0:
            return '신규고객'
        elif self.visit_count == 1:
            if self.is_first_time_no_return:
                return '1회차이탈'
            else:
                return '신규고객'
        elif self.visit_count <= 3:
            return '성장고객'
        elif self.visit_count <= 10:
            return '성숙고객'
        else:
            return '충성고객'
    
    @property
    def retention_risk_level(self):
        """이탈 위험도"""
        if not self.is_active_customer:
            return '이탈완료'
        elif self.is_inspection_overdue:
            return '위험'
        elif self.is_inspection_due_soon:
            return '주의'
        else:
            return '안전'


class CallRecord(models.Model):
    # 기본 정보
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE, related_name='call_records')
    caller = models.ForeignKey(User, on_delete=models.CASCADE, verbose_name='상담원')
    call_date = models.DateTimeField(default=timezone.now, verbose_name='통화일시')
    
    # 통화 상태
    RESULT_CHOICES = [
        ('connected', '통화성공'),
        ('no_answer', '부재중'),
        ('busy', '통화중'),
        ('wrong_number', '잘못된번호'),
        ('callback_requested', '재통화요청'),
    ]
    call_result = models.CharField(max_length=20, choices=RESULT_CHOICES, verbose_name='통화상태')
    
    # 상담 내용
    INTEREST_CHOICES = [
        ('insurance', '보험'),
        ('maintenance', '소모품교체'),
        ('financing', '자동차금융'),
        ('multiple', '복수관심'),
        ('none', '관심없음'),
    ]
    interest_type = models.CharField(max_length=20, choices=INTEREST_CHOICES, null=True, blank=True, verbose_name='관심분야')
    
    # 고객 반응
    ATTITUDE_CHOICES = [
        ('positive', '긍정적'),
        ('neutral', '보통'),
        ('negative', '부정적'),
    ]
    customer_attitude = models.CharField(max_length=20, choices=ATTITUDE_CHOICES, null=True, blank=True, verbose_name='고객반응')
    
    # 세부 내용
    notes = models.TextField(blank=True, verbose_name='상담내용')
    follow_up_date = models.DateField(null=True, blank=True, verbose_name='재통화예정일')
    
    # 성과
    # is_converted = models.BooleanField(default=False, verbose_name='계약성사여부')
    # conversion_amount = models.DecimalField(max_digits=10, decimal_places=0, null=True, blank=True, verbose_name='계약금액')
    
    # 소프트 삭제 필드들 (추가)
    is_deleted = models.BooleanField(default=False, verbose_name='삭제여부')
    deleted_at = models.DateTimeField(null=True, blank=True, verbose_name='삭제일시')
    deleted_by = models.ForeignKey(
        User, 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True, 
        related_name='deleted_call_records',
        verbose_name='삭제자'
    )

    # 후속조치 관련 필드
    requires_follow_up = models.BooleanField(default=False, verbose_name='후속조치필요')
    follow_up_completed = models.BooleanField(default=False, verbose_name='후속조치완료')
    follow_up_notes = models.TextField(blank=True, verbose_name='후속조치내용')
    updated_at = models.DateTimeField(auto_now=True, verbose_name='수정일시')
    
    # parent_call 필드의 related_name을 변경
    parent_call = models.ForeignKey(
        'self', 
        null=True, 
        blank=True, 
        on_delete=models.SET_NULL, 
        related_name='child_calls',  # follow_ups 대신 child_calls로 변경
        verbose_name='원통화기록'
    )

    class Meta:
        verbose_name = '통화기록'
        verbose_name_plural = '통화기록들'
        ordering = ['-call_date']
        
    def __str__(self):
        return f"{self.customer.name} - {self.call_date.strftime('%Y-%m-%d %H:%M')}"
    
    def soft_delete(self, user):
        """소프트 삭제 메소드"""
        self.is_deleted = True
        self.deleted_at = timezone.now()
        self.deleted_by = user
        self.save()
    
    def can_delete(self, user):
        """삭제 권한 체크"""
        # 관리자이거나 본인이 작성한 기록인 경우
        return user.is_staff or self.caller == user


class UploadHistory(models.Model):
    """CSV 업로드 이력 관리"""
    uploaded_by = models.ForeignKey(User, on_delete=models.CASCADE)
    upload_date = models.DateTimeField(auto_now_add=True)
    file_name = models.CharField(max_length=200)
    total_records = models.IntegerField()
    new_records = models.IntegerField()
    updated_records = models.IntegerField()
    error_count = models.IntegerField(default=0)
    notes = models.TextField(blank=True)
    
    class Meta:
        verbose_name = '업로드이력'
        verbose_name_plural = '업로드이력들'
        ordering = ['-upload_date']
        
    def __str__(self):
        return f"{self.file_name} - {self.upload_date.strftime('%Y-%m-%d')}"


# 사용자 프로필 확장 (팀장/팀원 구분)
class UserProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    ROLE_CHOICES = [
        ('manager', '팀장'),
        ('agent', '상담원'),
    ]
    role = models.CharField(max_length=20, choices=ROLE_CHOICES, default='agent')
    daily_call_target = models.IntegerField(default=100, verbose_name='일일통화목표')
    
    class Meta:
        verbose_name = '사용자프로필'
        verbose_name_plural = '사용자프로필들'
        
    def __str__(self):
        return f"{self.user.username} - {self.get_role_display()}"
    
class CallFollowUp(models.Model):
    """통화 후속조치 기록"""
    call_record = models.ForeignKey(CallRecord, on_delete=models.CASCADE, related_name='follow_ups')
    created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    
    ACTION_CHOICES = [
        ('callback_scheduled', '재통화 예약'),
        ('visit_scheduled', '방문 예약'),
        ('quote_sent', '견적 발송'),
        ('converted', '계약 성사'),
        ('closed', '종료'),
    ]
    action_type = models.CharField(max_length=20, choices=ACTION_CHOICES, blank=True)
    notes = models.TextField(verbose_name='후속조치 내용')
    scheduled_date = models.DateField(null=True, blank=True, verbose_name='예정일')
    
    class Meta:
        verbose_name = '후속조치'
        verbose_name_plural = '후속조치들'
        ordering = ['created_at']