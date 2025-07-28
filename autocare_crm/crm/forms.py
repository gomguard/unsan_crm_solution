# crm/forms.py
from django import forms
from .models import CallRecord, Customer

class CallRecordForm(forms.ModelForm):
    class Meta:
        model = CallRecord
        fields = [
            'call_result', 
            'interest_type', 
            'notes', 
            'follow_up_date'
        ]
        widgets = {
            'call_result': forms.Select(attrs={
                'class': 'form-select',
                'required': True
            }),
            'interest_type': forms.Select(attrs={
                'class': 'form-select'
            }),
            'notes': forms.Textarea(attrs={
                'class': 'form-control',
                'rows': 4,
                'placeholder': '상담 내용을 입력하세요...'
            }),
            'follow_up_date': forms.DateInput(attrs={
                'class': 'form-control',
                'type': 'date'
            }),
            'customer_attitude': forms.Select(attrs={
                'class': 'form-select'
            })
        }
        labels = {
            'call_result': '통화 상태',
            'interest_type': '관심 분야',
            'notes': '상담 내용',
            'follow_up_date': '재통화 예정일',
            'customer_attitude': '고객 반응',
        }

class CustomerUploadForm(forms.Form):
    file = forms.FileField(
        label='CSV 파일',
        widget=forms.FileInput(attrs={
            'class': 'form-control',
            'accept': '.csv',
            'required': True
        }),
        help_text='CSV 파일만 업로드 가능합니다. 파일 크기는 10MB 이하로 제한됩니다.'
    )
    
    def clean_file(self):
        file = self.cleaned_data.get('file')
        
        if file:
            # 파일 크기 체크 (10MB)
            if file.size > 10 * 1024 * 1024:
                raise forms.ValidationError('파일 크기가 10MB를 초과할 수 없습니다.')
            
            # 파일 확장자 체크
            if not file.name.endswith('.csv'):
                raise forms.ValidationError('CSV 파일만 업로드할 수 있습니다.')
        
        return file

class CustomerSearchForm(forms.Form):
    search = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': '이름, 전화번호, 고객ID로 검색...'
        })
    )
    
    status = forms.ChoiceField(
        choices=[('', '전체 상태')] + Customer.STATUS_CHOICES,
        required=False,
        widget=forms.Select(attrs={
            'class': 'form-select'
        })
    )
    
    inspection_due = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={
            'class': 'form-check-input'
        }),
        label='검사 임박 고객만'
    )

class CustomerEditForm(forms.ModelForm):
    class Meta:
        model = Customer
        fields = [
            'name', 
            'phone', 
            'vehicle_name',
            'vehicle_model', 
            'vehicle_number',
            'inspection_expiry_date',
            'customer_grade',
            'status'
        ]
        widgets = {
            'name': forms.TextInput(attrs={
                'class': 'form-control',
                'required': True
            }),
            'phone': forms.TextInput(attrs={
                'class': 'form-control',
                'required': True
            }),
            'vehicle_name': forms.TextInput(attrs={
                'class': 'form-control'
            }),
            'vehicle_model': forms.TextInput(attrs={
                'class': 'form-control'
            }),
            'vehicle_number': forms.TextInput(attrs={
                'class': 'form-control',
                'required': True
            }),
            'inspection_expiry_date': forms.DateInput(attrs={
                'class': 'form-control',
                'type': 'date'
            }),
            'customer_grade': forms.Select(attrs={
                'class': 'form-select'
            }),
            'status': forms.Select(attrs={
                'class': 'form-select'
            })
        }
        labels = {
            'name': '고객명',
            'phone': '휴대전화',
            'vehicle_name': '차량명',
            'vehicle_model': '모델명',
            'vehicle_number': '차량번호',
            'inspection_expiry_date': '검사만료일',
            'customer_grade': '고객등급',
            'status': '상태'
        }

class DateRangeForm(forms.Form):
    start_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date'
        }),
        label='시작일'
    )
    
    end_date = forms.DateField(
        required=False,
        widget=forms.DateInput(attrs={
            'class': 'form-control',
            'type': 'date'
        }),
        label='종료일'
    )
    
    agent = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': '상담원 이름'
        }),
        label='상담원'
    )