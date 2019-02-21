import datetime
from django.utils import timezone
from .models import JobTask

def flush_old_tasks():
    one_month_ago = timezone.now() - datetime.timedelta(days=30)
    jobs = JobTask.objects.filter(
        created_on__lte=one_month_ago
    )
    jobs.delete()
