from django.db import models
from django.db import connections
from django.utils import timezone
from djcelery.models import TaskState
import decimal

from main import task_registry

class LogEntry(models.Model):
    task = models.ForeignKey('main.JobTask')
    type = models.CharField(max_length=10)
    data = models.TextField(null=True, blank=True)

class BatchJob(models.Model):
    created_by = models.ForeignKey('auth.User')
    created_on = models.DateTimeField(auto_now_add=True)
    sql = models.TextField()
    form_data = models.TextField(default="{}")
    only_email_if_rows_above = models.IntegerField(default=0)

    title = models.CharField(max_length=255, null=True, blank=True)

    database = models.CharField(default='ak', max_length=255)

    def __unicode__(self):
        if self.title:
            return "%s: %s" % (self.id, self.title)
        else:
            return u"%s: A %s created by %s on %s" % (self.id, self.type, self.created_by, self.created_on)

    TYPE_CHOICES = [
        (task.slug, task.description) for task in task_registry.tasks.values() #@@TODO
        ]

    type = models.CharField(max_length=255, choices=TYPE_CHOICES)

    @property
    def form_factory(self):
        return task_registry.get_task(self.type).form_class

    def get_form(self, recurrence=None):
        return self.form_factory.from_job(self, recurrence=recurrence)

    def run_sql(self, ctx={}):
        cursor = connections[self.database].cursor()
        sql = self.sql
        if ctx and '{form[' in sql:
            sql = sql.format(form=ctx)
        cursor.execute(sql)

        row = cursor.fetchone()
        while row:
            row = [float(i) if isinstance(i, decimal.Decimal) else i for i in row]
            yield dict(zip([i[0] for i in cursor.description], row))
            row = cursor.fetchone()

class RecurringTask(models.Model):
    parent_job = models.ForeignKey(BatchJob)
    period = models.IntegerField()
    TIME_CHOICES = (
        ("minutes", "minutes"),
        ("hours", "hours"),
        ("days", "days"),
        )
    period_unit = models.CharField(max_length=255, choices=TIME_CHOICES)

    created_on = models.DateTimeField(auto_now_add=True)
    last_started_on = models.DateTimeField(null=True, blank=True)

    is_running = models.BooleanField(default=False)
    is_active = models.BooleanField(default=True)

    def current_time(self):
        return timezone.now()

    def __unicode__(self):
        return u"Every %s %s: %s" % (self.period, self.period_unit, unicode(self.parent_job))

    def reset(self):
        self.is_running = False
        self.save()

    def latest_run(self):
        return JobTask.objects.filter(parent_recurring_task=self).order_by("-created_on")[0]

    def completed_runs(self):
        return JobTask.objects.filter(parent_recurring_task=self).exclude(completed_on=None).order_by("-created_on")

    def latest_completed_run(self):
        return self.completed_runs()[0]

    def stale_runs(self):
        """ 
        Returns all but the most recent two runs of this job.
        """
        runs = JobTask.objects.filter(parent_recurring_task=self).order_by("created_on")
        runs = list(runs)
        return runs[:-2]

class JobTask(models.Model):
    parent_job = models.ForeignKey(BatchJob, null=True, blank=True)
    parent_recurring_task = models.ForeignKey(RecurringTask,
                                              null=True, blank=True)

    created_on = models.DateTimeField(auto_now_add=True)
    completed_on = models.DateTimeField(null=True, blank=True)

    num_rows = models.IntegerField(default=0)
    success_count = models.IntegerField(default=0)
    error_count = models.IntegerField(default=0)

    form_data = models.TextField(null=True, blank=True)

    def current_time(self):
        return timezone.now()
        
    def __unicode__(self):
        job = self.parent_job
        task = self.parent_recurring_task
        if task is not None:
            job = task.parent_job

        return "(job:%s) task:%s" % (unicode(job), self.id)

from django.contrib import admin
from djangohelpers.lib import register_admin

from djangohelpers.export_action import admin_list_export
register_admin(BatchJob)

def admin_make_active(modeladmin, request, queryset):
    queryset.update(is_active=True)
def admin_make_inactive(modeladmin, request, queryset):
    queryset.update(is_active=False)

class RecurringTaskAdmin(admin.ModelAdmin):
    list_display = [f.name for f in RecurringTask._meta.fields] + ['current_time']
    actions = [admin_list_export, admin_make_active, admin_make_inactive]
admin.site.register(RecurringTask, RecurringTaskAdmin)

from django.contrib import admin

class JobTaskAdmin(admin.ModelAdmin):
    list_display = ['id', 'parent_job', 'parent_recurring_task', 'created_on', 'completed_on', 'current_time', 'num_rows', 'success_count', 'error_count', 'form_data']
    list_filter = ['parent_recurring_task', 'parent_job']

admin.site.register(JobTask, JobTaskAdmin)

class TaskBatch(models.Model):
    tasks = models.TextField(null=True, blank=True)

    def add_task(self, task):
        if not self.tasks:
            self.tasks = str(task)
        else:
            self.tasks = self.tasks + "," + str(task)
        self.save()

    def get_tasks(self):
        if not self.tasks:
            return []
        tasks = self.tasks.split(",")
        return TaskState.objects.using("celerytasks").filter(task_id__in=tasks)

    @models.permalink
    def get_absolute_url(self):
        return ("batch", [self.id])
