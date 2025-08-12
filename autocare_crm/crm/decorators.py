from functools import wraps
from django.http import JsonResponse
from django.shortcuts import redirect
from django.contrib import messages

def manager_required(view_func):
    """팀장 이상 권한 필요"""
    @wraps(view_func)
    def wrapped_view(request, *args, **kwargs):
        print(f"[DECORATOR] manager_required 체크: user={request.user.username}")

        if not hasattr(request.user, 'userprofile'):
            messages.error(request, '사용자 프로필이 없습니다.')
            return redirect('dashboard')
        
        if not request.user.userprofile.is_manager_or_above():
            messages.error(request, '팀장 이상 권한이 필요합니다.')
            return redirect('dashboard')
        
        return view_func(request, *args, **kwargs)
    return wrapped_view

def admin_required(view_func):
    """관리자 권한 필요"""
    @wraps(view_func)
    def wrapped_view(request, *args, **kwargs):
        if not hasattr(request.user, 'userprofile'):
            messages.error(request, '사용자 프로필이 없습니다.')
            return redirect('dashboard')
        
        if not request.user.userprofile.is_admin():
            messages.error(request, '관리자 권한이 필요합니다.')
            return redirect('dashboard')
        
        return view_func(request, *args, **kwargs)
    return wrapped_view

def ajax_manager_required(view_func):
    """AJAX 요청용 팀장 권한 체크"""
    @wraps(view_func)
    def wrapped_view(request, *args, **kwargs):
        if not hasattr(request.user, 'userprofile'):
            return JsonResponse({'success': False, 'error': '사용자 프로필이 없습니다.'})
        
        if not request.user.userprofile.is_manager_or_above():
            return JsonResponse({'success': False, 'error': '팀장 이상 권한이 필요합니다.'})
        
        return view_func(request, *args, **kwargs)
    return wrapped_view