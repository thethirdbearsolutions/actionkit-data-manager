from django.contrib import messages
from django.http import (HttpResponse,
                         HttpResponseForbidden, 
                         HttpResponseNotFound)
from django.shortcuts import redirect, get_object_or_404
from djangohelpers import (rendered_with,
                           allow_http)
from actionkit.models import CoreAction
from main.models import TaskBatch
from djcelery.models import TaskState
from main.forms import *
from actionkit import *

from django.db import connections

from main.forms import RecurringForm
from main.tasks import run_batch_job
from main.models import BatchJob, JobTask, LogEntry


import csv
import json
import io

@allow_http("GET")
@rendered_with("main/logs_index.html")
def get_logs_index(request, id):
    task = JobTask.objects.filter(id=id)
    if 'filter' in request.GET:
        logs = logs.filter(data__icontains=request.GET['filter'])
    task = task.first()
    _types = LogEntry.objects.filter(task=task).values_list("type", flat=True).distinct()
    types = {}
    for type in _types:
        types[type] = LogEntry.objects.filter(task=task, type=type).count()
    
    return {"task": task, "types": types}

def get_logs(request, id, type):
    task = JobTask.objects.filter(id=id)
    logs = LogEntry.objects.filter(task=task, type=type)
    if 'filter' in request.GET:
        logs = logs.filter(data__icontains=request.GET['filter'])
    return HttpResponse(json.dumps(
        [json.loads(i.data) for i in logs],
        indent=2), content_type="application/json")

@allow_http("GET", "POST")
@rendered_with("main/schedule.html")
def schedule(request, id):
    form = RecurringForm(data=request.POST if request.method == 'POST' else None)
    job = BatchJob.objects.filter(id=id)
    if not request.user.is_superuser:
        job = job.filter(created_by=request.user)
    job = get_object_or_404(job)
    already = list(RecurringTask.objects.filter(parent_job=job))

    if request.method == 'POST':
        if form.is_valid():
            scheduled = form.save(commit=False)
            scheduled.parent_job = job
            scheduled.save()
            return redirect(".")
    return {"form": form, "job": job, "already": already}


@rendered_with("main/job_results.html")
def job_results(request, result):
    return {"result": result}

@allow_http("GET", "POST")
@rendered_with("main/batch_job.html")
def batch_job(request, type):

    allowed = False
    if request.user.is_superuser:
        allowed = True
    elif request.user.groups.filter(name=type).exists():
        allowed = True
    if not allowed:
        return HttpResponseForbidden("You do not have permission to access this job. "
                                     "Please contact an administrator.")

    job = BatchJob(created_by=request.user, type=type)
    
    if request.method == "GET":
        form = job.form_factory(data=request.GET)
    else:
        form = job.form_factory(data=request.POST)

    if not form.is_valid():
        preview = False
        return locals()        
    job = form.fill_job(job)

    if request.method == "GET":
        try:
            _rows = job.run_sql_api(form.get_data())
            limit = request.GET.get("limit", 100)
            rows = []
            while len(rows) < limit:
                try:
                    rows.append(_rows.next())
                except StopIteration:
                    break
            preview = True
            return locals() 
        except Exception, err:
            rows = []
            return locals()


    job.save()

    task = JobTask(parent_job=job)
    task.save()

    if request.POST.get("submit") == "Run Now, Synchronously":
        result = run_batch_job(task.id, request.META['QUERY_STRING'])
        return job_results(request, result)
    else:
        run_batch_job.delay(task.id, request.META['QUERY_STRING'])

        resp = redirect(".")
        resp['Location'] += '?' + request.META['QUERY_STRING']
    return resp

from main import task_registry
from django.contrib.flatpages.models import FlatPage

@allow_http("GET", "POST")
@rendered_with("main/home.html")
def home(request):
    links = []
    for job_type in task_registry.tasks.items():
        links.append(("/batch-job/%s/" % job_type[0], job_type[1].description))

    try:
        page = FlatPage.objects.get(url="/akdata-home/")
    except FlatPage.DoesNotExist:
        page = None
    return locals()
