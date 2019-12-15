from django import template
import json

register = template.Library()

@register.filter
def to_json(a):
    try:
        return json.dumps(a)
    except (ValueError, TypeError):
        return ''
