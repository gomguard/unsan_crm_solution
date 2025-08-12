# crm/templatetags/dashboard_filters.py
from django import template

register = template.Library()

@register.filter
def mul(value, arg):
    """곱셈 필터"""
    try:
        return float(value) * float(arg)
    except (ValueError, TypeError):
        return 0

@register.filter
def div(value, arg):
    """나눗셈 필터"""
    try:
        return float(value) / float(arg) if float(arg) != 0 else 0
    except (ValueError, TypeError):
        return 0

@register.filter
def percentage(value, total):
    """백분율 계산"""
    try:
        if float(total) == 0:
            return 0
        return round((float(value) / float(total)) * 100, 1)
    except (ValueError, TypeError):
        return 0

@register.filter
def subtract(value, arg):
    """뺄셈 필터"""
    try:
        return float(value) - float(arg)
    except (ValueError, TypeError):
        return 0