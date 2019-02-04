import datetime
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone
from main.models import RecurringTask

class Command(BaseCommand):
    help = 'Finds suspiciously long-running jobs and stops them'

    def add_arguments(self, parser):
        parser.add_argument('stale_seconds_threshold', type=int)

    def handle(self, *args, **options):
        threshold = timezone.now() - datetime.timedelta(
            seconds=options['stale_seconds_threshold']
        )
        return RecurringTask.objects.filter(
            is_running=True,
            last_started_on__lt=threshold
        ).update(is_running=False)
