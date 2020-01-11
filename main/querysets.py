from django.apps import apps
from django.db import models
from django.db.models.functions import Concat

class RecurringTaskQueryset(models.QuerySet):

    def add_latest_stats(self):
        JobTask = apps.get_model('main', 'JobTask')

        def q(val):
            return (
                JobTask.objects
                .filter(parent_recurring_task=models.OuterRef('id'))
                .exclude(completed_on=None)
                .order_by("-created_on")
                .values(val)
                [:1]
            )

        def r(val):
            return (
                JobTask.objects
                .filter(parent_recurring_task=models.OuterRef('id'))
                .exclude(completed_on=None)
                .exclude(num_rows=0)
                .order_by("-created_on")
                .values(val)
                [:1]
            )
            
        return self.annotate(
            #latest_nonzero_id=models.Subquery(q('id')),            
            #latest_run_num_rows=models.Subquery(q('num_rows')),
            #latest_run_success_count=models.Subquery(q('success_count')),
            #latest_run_error_count=models.Subquery(q('error_count')),

            latest_nonzero_id=models.Subquery(r('id')),
            latest_nonzero_completed_on=models.Subquery(r('completed_on')),            
            latest_nonzero_num_rows=models.Subquery(r('num_rows')),
            latest_nonzero_success_count=models.Subquery(r('success_count')),
            latest_nonzero_error_count=models.Subquery(r('error_count')),
        )
