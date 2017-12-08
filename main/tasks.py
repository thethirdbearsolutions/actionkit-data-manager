import traceback

from actionkit import Client
from actionkit.models import CoreUserField
import datetime
from actionkit.rest import client as RestClient
import json
from actionkit.models import CoreAction, CorePage, CorePageTag, CoreActionField
from main.models import RecurringTask, JobTask

from actionkit import *
from django.core.mail import send_mail
from dateutil.relativedelta import relativedelta
import json
from django.conf import settings

from akdata.celery import app

@app.task()
def run_batch_job(task_id):
    task = JobTask.objects.get(id=task_id)
    recurrence = None
    job = task.parent_job 
    if job is None:
        recurrence = task.parent_recurring_task
        job = recurrence.parent_job 

    form = job.get_form(recurrence=recurrence)
    rows = job.run_sql(form.get_data())

    name = job.title
    try:
        task.num_rows, task.success_count, task.error_count = form.run(task, rows)
    except Exception, e:
        message = traceback.format_exc()
        subject = "[ActionKit Data Manager] Task %s (%s) failed :-(" % (task.id, name)
    else:
        if hasattr(form, 'make_nonfailed_email_message'):
            message = form.make_nonfailed_email_message(task)
        else:
            message = "Num rows attempted: %s.  Success count: %s.  Error count: %s." % (
            task.num_rows, task.success_count, task.error_count)
        if task.error_count:
            subject = "[ActionKit Data Manager] Task %s (%s) completed with errors =/" % (task.id, name)
        else:
            subject = "[ActionKit Data Manager] Task %s (%s) succeeded =)" % (task.id, name)

    if task.form_data:
        try:
            data = json.dumps(json.loads(task.form_data), indent=2)
        except Exception:
            pass
        else:
            messages += "\n\n%s" % data

    message += "\n\nCheck it out here: http://%s/admin/main/jobtask/%s/" % (settings.SITE_DOMAIN, task.id)
    message += "\nThe job configuration is here: http://%s/admin/main/batchjob/%s/" % (settings.SITE_DOMAIN, job.id)
    message += "\nMake it recurring here: http://%s/schedule/%s/" % (settings.SITE_DOMAIN, job.id)

    task.completed_on = datetime.datetime.now()
    task.save()

    if recurrence is not None:
        recurrence.is_running = False
        recurrence.save()

    num = send_mail(subject, message, settings.DEFAULT_FROM_EMAIL,
                    [job.created_by.email] + [i[1] for i in settings.ADMINS], 
                    fail_silently=False)

    print "Sent %s mails with subject %s; job %s completed; %s rows" % (num, subject, job.id, task.num_rows)
    return message

@app.task()
def run_recurring_tasks():

    print "Looking for tasks..."

    now = datetime.datetime.now()

    tasks = RecurringTask.objects.filter(
        is_active=True, is_running=False).select_related("parent_job")
    active_tasks = []
    for task in tasks:
        if task.period_unit == "minutes":
            if task.last_started_on is None or task.last_started_on < now - relativedelta(minutes=task.period):
                active_tasks.append(task)
                task.is_running = True
                task.save()

        elif task.period_unit == "hours":
            if task.last_started_on is None or task.last_started_on < now - relativedelta(hours=task.period):
                active_tasks.append(task)
                task.is_running = True
                task.save()

        elif task.period_unit == "days":
            if task.last_started_on is None or task.last_started_on < now - relativedelta(days=task.period):
                active_tasks.append(task)
                task.is_running = True
                task.save()
                
    for r in active_tasks:
        job = r.parent_job

        r.last_started_on = datetime.datetime.now()
        r.save()

        task = JobTask(parent_recurring_task=r)
        task.save()

        run_batch_job.delay(task.id)

