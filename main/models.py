from django.db import connections
from django.db import models
from django.urls import reverse
from django.utils import timezone
from djcelery.models import TaskState
import decimal
import uuid

from main import task_registry
from main.querysets import RecurringTaskQueryset
from actionkit.rest import run_query

class LogEntry(models.Model):
    task = models.ForeignKey('main.JobTask', on_delete=models.CASCADE)
    type = models.CharField(max_length=10)
    data = models.TextField(null=True, blank=True)

class BatchJob(models.Model):
    created_by = models.ForeignKey('auth.User', on_delete=models.SET_NULL, null=True)
    created_on = models.DateTimeField(auto_now_add=True)
    sql = models.TextField()
    form_data = models.TextField(default="{}")
    only_email_if_rows_above = models.IntegerField(default=0)
    run_via = models.CharField(
        max_length=20,
        default='client-db',
        choices=(("client-db", "client-db"), ("api", "api"))
    )

    title = models.CharField(max_length=255, null=True, blank=True)

    database = models.CharField(default='ak', max_length=255)

    def __str__(self):
        if self.title:
            return "%s: %s" % (self.id, self.title)
        else:
            return "%s: A %s created by %s on %s" % (self.id, self.type, self.created_by, self.created_on)

    type = models.CharField(max_length=255)
    
    @property
    def form_factory(self):
        return task_registry.get_task(self.type).form_class

    def get_form(self, recurrence=None):
        return self.form_factory.from_job(self, recurrence=recurrence)

    def run_sql_api(self, ctx={}):
        cursor = connections[self.database].cursor()
        
        sql = self.sql
        if ctx and '{form[' in sql:
            sql = sql.format(form=ctx)

        cursor.execute(sql)
        
        results = run_query(sql)

        for row in results:
            row = [float(i) if isinstance(i, decimal.Decimal) else i for i in row]
            yield dict(zip([i[0] for i in cursor.description], row))

    def run_sql_count(self, ctx={}):
        cursor = connections[self.database].cursor()
        sql = self.sql
        if ctx and '{form[' in sql:
            sql = sql.format(form=ctx)
        sql = """
        select count(*) from (
        %s
        ) count_subquery_%s
        """ % (sql, uuid.uuid4().hex)
        cursor.execute(sql)
        row = cursor.fetchone()
        return row[0]
        
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

    objects = RecurringTaskQueryset.as_manager()
    
    parent_job = models.ForeignKey(BatchJob, on_delete=models.CASCADE)
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

    def __str__(self):
        return "Every %s %s: %s" % (self.period, self.period_unit, str(self.parent_job))

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

class RecurringTaskConflict(models.Model):
    recurring_tasks = models.ManyToManyField(RecurringTask)
    description = models.TextField()

class DraftJob(models.Model):
    type = models.CharField(max_length=255)
    query_string = models.TextField()
    name = models.CharField(max_length=255, default='', blank=True)
    created_on = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey('auth.User', null=True, on_delete=models.SET_NULL)

class JobTask(models.Model):
    parent_job = models.ForeignKey(BatchJob, null=True, blank=True, on_delete=models.CASCADE)
    parent_recurring_task = models.ForeignKey(RecurringTask,
                                              null=True, blank=True, on_delete=models.CASCADE)

    created_on = models.DateTimeField(auto_now_add=True)
    completed_on = models.DateTimeField(null=True, blank=True)

    expected_num_rows = models.IntegerField(default=0)
    num_rows = models.IntegerField(default=0)
    success_count = models.IntegerField(default=0)
    error_count = models.IntegerField(default=0)

    form_data = models.TextField(null=True, blank=True)

    def current_time(self):
        return timezone.now()
        
    def __str__(self):
        job = self.parent_job
        task = self.parent_recurring_task
        if task is not None:
            job = task.parent_job

        return "(job:%s) task:%s" % (str(job), self.id)

from django.contrib import admin
from djangohelpers.lib import register_admin

from djangohelpers.export_action import admin_list_export
from django.utils.timesince import timesince

register_admin(BatchJob)

def admin_make_active(modeladmin, request, queryset):
    queryset.update(is_active=True)
def admin_make_inactive(modeladmin, request, queryset):
    queryset.update(is_active=False)

class RecurringTaskAdmin(admin.ModelAdmin):

    def get_queryset(self, request):
        return super().get_queryset(request).add_latest_stats()
    
    def get_parent_job(self, obj):
        if obj.parent_job_id:
            return mark_safe("<a href='/admin/main/batchjob/%s/'>%s</a>" % (
                obj.parent_job_id, obj.parent_job
            ))

    def frequency(self, obj):
        return "%s %s" % (obj.period, obj.period_unit)

    def last_started_ago(self, obj):
        return '%s ago' % timesince(obj.last_started_on)

    def type(self, obj):
        return obj.parent_job.type

    def latest_nonzero(self, obj):
        return mark_safe(
            '<a href="/admin/main/jobtask/?id=%s">%s ago: %s rows (%s success; %s error)</a>' % (
                obj.latest_nonzero_id,
                timesince(obj.latest_nonzero_completed_on),
                obj.latest_nonzero_num_rows,
                obj.latest_nonzero_success_count,
                obj.latest_nonzero_error_count,
            )
        ) if obj.latest_nonzero_id else ''

    list_display = [
        'id', 'get_parent_job', 'type',
        'frequency', 'last_started_ago',
        'latest_nonzero',
        'is_active', 'is_running',
    ]

    actions = [admin_list_export, admin_make_active, admin_make_inactive]
    list_select_related = ['parent_job']

    def get_readonly_fields(self, request, obj=None):
        if obj:
            return ['parent_job']
        return []

admin.site.register(RecurringTask, RecurringTaskAdmin)

from django.contrib import admin
from django.utils.safestring import mark_safe

class HasResultsListFilter(admin.SimpleListFilter):
    title = "num rows"
    parameter_name = "num_rows"

    def lookups(self, request, model_admin):
        return [
            ("none", "None"),
            ("some", "Some"),
        ]

    def queryset(self, request, queryset):
        if self.value() == "none":
            return queryset.filter(num_rows=0)
        elif self.value() == "some":
            return queryset.filter(num_rows__gt=0)
        return queryset
    
class JobTaskAdmin(admin.ModelAdmin):

    def rows_logged(self, obj):
        return obj._rows_logged

    def rows_per_second(self, obj):
        return '%.2f' % (1 / self.seconds_per_row(obj))
    
    def seconds_per_row(self, obj):
        td = ((obj.completed_on or timezone.now()) - obj.created_on).total_seconds()
        return td / obj._rows_logged 

    def estimated_duration(self, obj):
        remaining = obj.expected_num_rows - obj._rows_logged
        seconds_remaining = remaining * self.seconds_per_row(obj)
        return '%.2f minutes' % (seconds_remaining / 60)
    
    def get_queryset(self, *args, **kw):
        return super().get_queryset(*args, **kw).annotate(
            _rows_logged=models.Count('logentry', filter=models.Q(logentry__type='success')),
        )
    
    def get_logs_url(self, obj):
        return mark_safe("<a href='/logs/%s'>%s</a>" % (obj.id, 'logs'))

    def get_parent_recurring_task(self, obj):
        if obj.parent_recurring_task_id:
            return mark_safe("<a href='/admin/main/recurringtask/%s/'>%s</a>" % (obj.parent_recurring_task_id, obj.parent_recurring_task))
    
    list_display = [
        'id', 'get_logs_url',
        'parent_job', 'get_parent_recurring_task',
        'estimated_duration',
        'created_on', 'completed_on', 'current_time',
        'num_rows', 'success_count', 'error_count', 'form_data',
        'expected_num_rows',
        'rows_logged', 'rows_per_second',
    ]
    list_filter = [HasResultsListFilter, 'parent_recurring_task', 'parent_job']
    list_select_related = ['parent_recurring_task', 'parent_job']
    
admin.site.register(JobTask, JobTaskAdmin)

register_admin(RecurringTaskConflict)

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

    def get_absolute_url(self):
        return reverse("batch", [self.id])

class LogAdmin(admin.ModelAdmin):
    search_fields = ['data']
    list_filter = ['type', 'task', 'task__parent_job', 'task__parent_recurring_task']
    list_display = ['type', 'task', 'data']
    list_select_related = ['task', 'task__parent_job', 'task__parent_recurring_task',
                           'task__parent_recurring_task__parent_job']
    readonly_fields = ['task', 'type', 'data']

admin.site.register(LogEntry, LogAdmin)
