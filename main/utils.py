import datetime
from django.utils import timezone
from .models import JobTask

def flush_old_tasks(days_ago=30):
    one_month_ago = timezone.now() - datetime.timedelta(days=days_ago)
    jobs = JobTask.objects.filter(
        created_on__lte=one_month_ago
    )
    print 'Deleting %s jobs' % jobs.count()
    jobs.delete()
