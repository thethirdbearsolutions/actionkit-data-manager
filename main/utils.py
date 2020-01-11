import datetime
from django.conf import settings
from django.shortcuts import redirect
from django.utils import timezone
from django_otp import user_has_device

from .models import JobTask

def enforce_2fa_setup(view):
    def inner(request, *args, **kw):
        if not settings.ENFORCE_TWO_FACTOR:
            return view(request, *args, **kw)
        if not request.user.is_authenticated:
            return redirect(settings.LOGIN_URL)
        if request.user.is_verified():
            return view(request, *args, **kw)
        if not user_has_device(request.user):
            return redirect('two_factor:profile')
        return redirect('two_factor_login_step2')
    inner.__name__ = view.__name__
    return inner

def flush_old_tasks(days_ago=30):
    one_month_ago = timezone.now() - datetime.timedelta(days=days_ago)
    jobs = JobTask.objects.filter(
        created_on__lte=one_month_ago
    )

    print('Deleting %s jobs' % (jobs.count()))
    jobs.delete()
