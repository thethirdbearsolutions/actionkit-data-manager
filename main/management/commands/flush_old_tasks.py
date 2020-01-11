from django.core.management.base import BaseCommand, CommandError
from main.utils import flush_old_tasks

class Command(BaseCommand):
    help = 'Flushes ancient jobs (and their log entries) from the system'

    def add_arguments(self, parser):
        parser.add_argument('days', type=int)

    def handle(self, *args, **options):
        return flush_old_tasks(options['days'])
