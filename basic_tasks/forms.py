import datetime
import decimal
from django import forms
from django.conf import settings
from django.db import connections
from django.template import Template, Context
import gzip
import json
import os
import shutil
import subprocess
import requests
import traceback
import lxml.html
import lxml.etree as ET

from actionkit import Client
from actionkit.rest import client as RestClient
from actionkit.models import (
    CoreAction, CoreActionField, QueryReport, CoreTag, CorePageField, CorePage
)

from main.forms import BatchForm, get_task_log
from urllib.parse import quote, unquote, parse_qs
from urllib.request import urlopen
from json import load

user_agent_list = [
   #Chrome
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]
import random
from html.parser import HTMLParser

dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else None

from django.contrib.humanize.templatetags.humanize import intcomma
from django.template import defaultfilters 

class StoreDataForm(BatchForm):

    database_connection = forms.CharField(required=True)
    local_table = forms.CharField(required=True)

    def create_table(self, cur, header, tablename):
        columns_statement = ', '.join([
            '"%s" char(255)' % col for col in header
        ])
        sql = 'create unlogged table "%s" (%s)' % (tablename, columns_statement)
        print(sql)
        cur.execute(sql)
    
    def run(self, task, rows):
        task_log = get_task_log()
        cur = connections[self.cleaned_data['database_connection']].cursor()
        n_rows = n_success = n_error = 0
        
        header = None
        tablename = self.cleaned_data['local_table']
        for row in rows:
            n_rows += 1
            task_log.sql_log(task, row)            
            if not header:
                header = row.keys()
                
                cur.execute("select * from information_schema.tables where table_name=%s",
                            (tablename,))
                if cur.rowcount == 0:
                    self.create_table(cur, header, tablename)
            columns_clause = ', '.join([
                '"%s"' % col for col in header
            ])
            values_clause = ', '.join([
                "'%s'" % str(val).strip() for val in row.values()
            ])
            sql = 'insert into "%s" (%s) values (%s)' % (tablename, columns_clause, values_clause)
            task_log.activity_log(task, {'sql': sql})
            
            cur.execute(sql)
            n_success += 1
        return n_rows, n_success, n_error

class TagSyncForm(BatchForm):

    ak_tag_prefix = forms.CharField(required=False)
    ak_tag_suffix = forms.CharField(required=False)
    tag_mapping_database_page_id = forms.IntegerField(required=True)

    def run(self, task, rows):
        task_log = get_task_log()
        n_rows = n_success = n_error = 0

        for row in rows:
            n_rows += 1
            task_log.sql_log(task, row)
            try:
                tag_id, did_something = self.find_or_create_tag(
                    task, 
                    row['tag_unique_identifier'],
                    row['new_data_tag_name'],
                    self.cleaned_data.get("ak_tag_prefix"),
                    self.cleaned_data.get("ak_tag_suffix"),
                )
            except Exception as e:
                n_error += 1
                task_log.error_log(task, {
                    "row": row,
                    "error": str(e),
                })
            else:
                n_success += 1 if did_something else 0

        return n_rows, n_success, n_error
    
    def find_or_create_tag(self, task,
                           tag_uuid, tag_name,
                           ak_tag_prefix, ak_tag_suffix):

        task_log = get_task_log()

        rest = RestClient()
        rest.safety_net = False

        desired_tag_name = []
        if ak_tag_prefix:
            desired_tag_name.append(ak_tag_prefix)
        desired_tag_name.append(tag_name)
        if ak_tag_suffix:
            desired_tag_name.append(ak_tag_suffix)
        desired_tag_name = ' '.join(desired_tag_name)

        fields = CorePageField.objects.using("ak").filter(
            parent_id=self.cleaned_data['tag_mapping_database_page_id'],
            name='zyx_stashing_data_code',
        )
        fields = dict(
            f.value.split('=', 1) for f in fields
        )

        created_tag = False
        patched_tag = False
        found_tag_unaccounted_for = False
        ak_tag_id = None
        ak_tag_name = None

        try:
            ak_tag_id = fields[str(tag_uuid)]
        except KeyError:
            try:
                ak_tag_id = rest.tag.create(
                    name=desired_tag_name,
                )
            except AssertionError as e:
                ak_tag_id = CoreTag.objects.using("ak").get(name=desired_tag_name).id
                fields[tag_uuid] = ak_tag_id
                created_tag = True
                found_tag_unaccounted_for = True
            else:
                fields[tag_uuid] = ak_tag_id
                created_tag = True
        else:
            ak_tag_name = rest.tag.get(id=ak_tag_id)['name']
            if ak_tag_name != desired_tag_name:
                task_log.activity_log(task, {
                    "message": "patching tag",
                    "id": ak_tag_id,
                    "current_name": ak_tag_name,
                    "new_name": desired_tag_name,
                })
                rest.tag.patch(id=ak_tag_id, name=desired_tag_name)
                patched_tag = True

        if created_tag:
            serialized_fields = [
                '%s=%s' % (key, value)
                for key, value
                in fields.items()
            ]

            task_log.activity_log(task, {
                "message": "patching tag mapping db",
                "ak_tag_name": ak_tag_name,
                "ak_tag_id": ak_tag_id,
                "new_name": desired_tag_name,
                "created_tag": created_tag,
                "found_tag_unaccounted_for": found_tag_unaccounted_for,
                "page_id": self.cleaned_data['tag_mapping_database_page_id'],
                "fields": {'zyx_stashing_data_code': serialized_fields},
            })
            rest.signuppage.patch(
                id=self.cleaned_data['tag_mapping_database_page_id'],
                fields={'zyx_stashing_data_code': serialized_fields},
            )

        return ak_tag_id, (created_tag or patched_tag)
    
class CloudinaryImageForm(BatchForm):
    
    template = forms.CharField(label="Cloudinary URL template", required=True)
    s3_location = forms.CharField(label="S3 location (bucketname/path/to/folder)", required=True)
    
    def run(self, task, rows):
        task_log = get_task_log()
        n_rows = n_success = n_errors = 0
        
        for row in rows:

            task_log.sql_log(task, row)
            n_rows += 1
            
            row['donations'] = intcomma(int(row['donations']))
            row['donors'] = intcomma(int(row['donors']))
            row['goal'] = intcomma(int(row['goal']))
        
            row['timestamp'] = ("As of %s at %s Eastern" % (
                defaultfilters.date(row['timestamp'], "M jS"),
                defaultfilters.date(row['timestamp'], "P").replace(".", "").replace(" ", ""),
            )).upper()

            for key in row:
                row[key] = quote(str(row[key]).replace(',', "%2C").replace('/', "%2F"))

            url = self.cleaned_data['template'].format(**row)
        
            try:
                response = requests.get(url, stream=True)
                with open('/tmp/%s.png' % row['filename'], 'wb') as handle:
                    for block in response.iter_content(1024):
                        handle.write(block)

                if os.path.getsize("/tmp/%s.png" % row['filename']) < 10000:
                    raise Exception(
                        "File %s is suspiciously short: %s bytes" % (
                            row['filename'], os.path.getsize("/tmp/%s.png" % row['filename'])
                        )
                    )
                
                subprocess.check_call([
                    "s3cmd", "put", "--acl-public",
                    "--cf-invalidate",
                    "--add-header=Cache-Control:no-cache",
                    "-c", "/home/taskman/.s3cfg",
                    "/tmp/%s.png" % row['filename'],
                    "s3://%s/%s.png" % (self.cleaned_data['s3_location'], row['filename'])
                ])
                
            except Exception as e:
                n_errors += 1
                task_log.error_log(task, {"row": row, "error": str(e)})
            else:
                n_success += 1

        return n_rows, n_success, n_errors


class EventGeolocationForm(BatchForm):
    google_api_key = forms.CharField(label="Google API Key", required=True)
    
    def get_address(self, row):
        components = []
        fields = "address1 address2 city state region postal zip country".split()
        for field in fields:
            value = row.get(field)
            if value and value.strip() and value.strip() not in components:
                components.append(value)
        return (u', '.join(components)).encode("utf-8")

    def get_coords(self, addr):
        GEOCODE_URL = 'https://maps.googleapis.com/maps/api/geocode/json?sensor=false&address='
        api_key = self.cleaned_data['google_api_key']
        url = "%s%s&key=%s" % (GEOCODE_URL, quote(addr), api_key)
        res = load(urlopen(url))
        results = res['results'][0]
        return (results['geometry']['location']['lat'],
                results['geometry']['location']['lng'],
                results)

    def run(self, task, rows):
        ak = Client()
        n_rows = n_success = n_error = 0

        task_log = get_task_log()
        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1
            assert row.get('event_id') and int(row['event_id'])
            addr = self.get_address(row)
            if row.get('geocoded_latitude') and \
               row.get('geocoded_longitude') and \
               row.get('geocoded_address') == addr:
                continue
            try:
                coords = self.get_coords(addr)
            except Exception as e:
                n_error += 1
                task_log.error_log(task, {
                    "event": row['event_id'],
                    "status": "geocoding-failed",
                    "address": addr,
                    "error": str(e),
                })
                continue
            else:
                task_log.activity_log(task, {
                    "id": row['event_id'],
                    "geo-result": coords[2],
                })
            try:
                resp = ak.Event.set_custom_fields({
                    'id': row['event_id'],
                    'geocoded_address': addr,
                    'geocoded_latitude': coords[0],
                    'geocoded_longitude': coords[1],
                })
            except Exception as e:
                n_error += 1
                task_log.error_log(task, {
                    "event": row['event_id'],
                    "status": "set-custom-fields-failed",
                    "address": addr,
                    "coords": coords,
                    "error": str(e),
                })
                continue
            n_success += 1
            task_log.success_log(task, resp)
        return n_rows, n_success, n_error


class EventFieldCreateForm(BatchForm):
    field_name = forms.CharField(label="Event Field Name", required=True)
    field_value = forms.CharField(label="Event Field Value", required=False)

    def run(self, task, rows):
        field_value = self.cleaned_data.get("field_value").strip() or None
        field_name = self.cleaned_data['field_name']

        is_array = False
        separator = None
        if '[' in field_name and ']' in field_name:
            is_array = True
            separator = field_name.split("[")[1].split("]")[0]
            field_name = field_name.split("[")[0]

        ak = Client()
        n_rows = n_success = n_error = 0

        task_log = get_task_log()
        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1
            assert row.get('event_id') and int(row['event_id'])

            try:
                fields = ak.Event.get_custom_fields({"id": row['event_id']})
                fields['id'] = row['event_id']
                
                if not is_array:
                    fields[field_name] = field_value or row['field_value']
                else:
                    fields[field_name] = (field_value or row['field_value']).split(separator)
                
                resp = ak.Event.set_custom_fields(fields)
            except Exception as e:
                n_error += 1
                resp = {"log_id": row['event_id'], "error": traceback.format_exc()}
                task_log.error_log(task, resp)
            else:
                n_success += 1
                task_log.success_log(task, resp)

        return n_rows, n_success, n_error

class PublishReportResultsForm(BatchForm):
    report_id = forms.IntegerField(required=False)
    filename = forms.CharField(max_length=255)
    wrapper = forms.CharField(max_length=255)
    bucket = forms.CharField(max_length=255)
    bucket_url = forms.CharField(max_length=1000)
    variable_mapping = forms.CharField(max_length=1000, required=False)
    
    def make_nonfailed_email_message(self, task):
        message = """Report results have been generated and published to the following URL:

%s%s
""" % (self.cleaned_data['bucket_url'], self.cleaned_data['filename'])
        return message

    def run_sql(self, sql):

        if '{{' in sql and '}}' in sql and self.cleaned_data.get("variable_mapping"):
            variable_mapping = json.loads(self.cleaned_data['variable_mapping'])
            sql = "{% autoescape off %}" + sql + "{% endautoescape %}"
            for key in variable_mapping:
                if isinstance(variable_mapping[key], basestring):
                    variable_mapping[key] = '"%s"' % variable_mapping[key]
            context = Context(variable_mapping)
            sql = Template(sql).render(context)
        
        cursor = connections['ak'].cursor()
        cursor.execute(sql)

        row = cursor.fetchone()
        while row:
            row = [float(i) if isinstance(i, decimal.Decimal) else i for i in row]
            yield dict(zip([i[0] for i in cursor.description], row))
            row = cursor.fetchone()

    def run(self, task, rows):
        ak = Client()

        task_log = get_task_log()

        if self.cleaned_data.get("report_id"):
            report = QueryReport.objects.using("ak").get(report_ptr__id=self.cleaned_data['report_id'])
            rows = list(self.run_sql(report.sql))
        else:
            _rows = {}
            for row in rows:
                report = QueryReport.objects.using("ak").get(report_ptr__id=row['report_id'])
                result = list(self.run_sql(report.sql))
                if (row.get("format") or "").startswith('json0['):
                    result = result[0]
                    result = result[row['format'].split("[")[1].split("]")[0]]
                    result = json.loads(result)
                _rows[row['key']] = result
            rows = _rows

        if os.path.exists("/tmp/%s" % self.cleaned_data['filename']):
            shutil.move("/tmp/%s" % self.cleaned_data['filename'], "/tmp/%s.old" % self.cleaned_data['filename'])
        
        fp = open("/tmp/%s" % self.cleaned_data['filename'], 'w')
        data = json.dumps(rows, default=dthandler, indent=2)
        if self.cleaned_data.get("wrapper") and '%' in self.cleaned_data['wrapper']:
            data = self.cleaned_data['wrapper'] % data
        fp.write(data)
        fp.close()

        if not os.path.exists("/tmp/%s.old" % self.cleaned_data['filename']):
            subprocess.check_call([
                "s3cmd", "put", "--acl-public",
                "/tmp/%s" % self.cleaned_data['filename'],
                "s3://%s/" % self.cleaned_data['bucket']
            ])
            return 1, 1, 0 

        try:
            subprocess.check_output([
                "diff",
                "/tmp/%s.old" % self.cleaned_data['filename'],
                "/tmp/%s" % self.cleaned_data['filename'],
            ])
        except subprocess.CalledProcessError as e:
            diff = e.output.splitlines()
            task.form_data = json.dumps({"diff": e.output})
            task.save()
        else:
            return 0, 0, 0

        try:
            cmd = [
                "s3cmd", "put", "--acl-public",
                "/tmp/%s" % self.cleaned_data['filename'],
                "s3://%s/" % self.cleaned_data['bucket']
            ]
            subprocess.check_call(cmd)
        except subprocess.CalledProcessError as e:
            task_log.error_log(task, {
                "stage": "s3cmd_put",
                "errorcode": e.returncode,
                "error": str(e),
            })
            return 1, 0, 1
        return 1, len(diff), 0

class UserMergeForm(BatchForm):
    help_text = """
Required columns: merge_from_user_id, merge_to_user_id, whose_address ("from" or "to")
"""

    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        n_rows = n_success = n_error = 0
        task_log = get_task_log()

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            try:
                for field in 'merge_from_user_id merge_to_user_id'.split():
                    assert row.get(field) and int(row[field])
                assert row.get("whose_address") and row['whose_address'] in ("from", "to")
            
                resp = rest.usermerge.create(
                    primary_user='/rest/v1/user/%s/' % row['merge_to_user_id'],
                    address_user='/rest/v1/user/%s/' % (row['merge_to_user_id'] if row['whose_address'] == 'to' else row['merge_from_user_id']),
                    users=[
                        '/rest/v1/user/%s/' % row['merge_from_user_id'],
                    ],
                )
            except Exception as e:
                n_error += 1
                resp = {"log_id": row['merge_from_user_id'],
                        'error': traceback.format_exc()}
                task_log.error_log(task, resp)
            else:
                n_success += 1
                task_log.success_log(task, resp)
        return n_rows, n_success, n_error

class UserEraseForm(BatchForm):
    help_text = """
    Required columns: user_id
    """

    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        n_rows = n_success = n_error = 0
        task_log = get_task_log()

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            try:
                for field in 'user_id'.split():
                    assert row.get(field) and int(row[field])
            
                resp = rest.eraser.create(
                    user_id=row['user_id'],
                    order_user_details=True,
                    user_fields=True,
                    action_fields=True,
                    transactional_mailings=True,
                )
            except Exception as e:
                n_error += 1
                resp = {"log_id": row['merge_from_user_id'],
                        'error': traceback.format_exc()}
                task_log.error_log(task, resp)
            else:
                n_success += 1
                task_log.success_log(task, resp)
        return n_rows, n_success, n_error

    
class UserfieldJobForm(BatchForm):
    help_text = """
The SQL must return a column named `user_id`. Userfield Value is optional -- include it if you want a hardcoded userfield value filled in for all results. Alternatively, you can cause the SQL to return a column named `userfield_value`, and this will be used instead.
"""
    userfield_name = forms.CharField(label="Userfield Name", required=True)
    userfield_value = forms.CharField(label="Userfield Value", required=False)
    action_page = forms.CharField(label="Page Names to Act On", required=False)
    split_on_character = forms.CharField(label="Split on", required=False)
    
    def run(self, task, rows):
        userfield_value = self.cleaned_data.get("userfield_value").strip() or None
        userfield_name = self.cleaned_data['userfield_name']

        page = self.cleaned_data.get('action_page', '').strip() or None

        ak = Client()
        rest = RestClient()
        rest.safety_net = False

        userfield_name = 'user_%s' % userfield_name

        n_rows = n_success = n_error = 0

        task_log = get_task_log()
        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1
            assert row.get('user_id') and int(row['user_id'])

            if page:
                try:
                    action = CoreAction.objects.using("ak").select_related(
                        "page").get(
                        user__id=row['user_id'], page__name=page)
                except CoreAction.DoesNotExist:
                    action = None
                except CoreAction.MultipleObjectsReturned:
                    action = None
            else:
                action = None

            try:
                if page is None:
                    obj = {
                        "id": row['user_id'],
                        userfield_name: (userfield_value or
                                         row['userfield_value'])
                    }
                    split_char = self.cleaned_data.get("split_on_character")
                    if split_char is not None \
                       and split_char in obj[userfield_name]:
                        obj[userfield_name] = [
                            i.strip() for i in obj[userfield_name].split(split_char)
                        ]
                    task_log.activity_log(task, obj)
                    resp = ak.User.save(obj)
                elif action is None:
                    resp = ak.act({
                            "id": row['user_id'],
                            "page": page,
                            "source": "aktasks-%s" % task.id,
                            userfield_name: (userfield_value or
                                             row['userfield_value'])})
                else:
                    page_url = "/rest/v1/%spage/%s/" % (
                        action.page.type.lower(), action.page_id)
                    user_url = "/rest/v1/user/%s/" % action.user_id
                    handler = getattr(
                        rest, "%saction" % action.page.type.lower())
                    args = dict(page=page_url, user=user_url)
                    args[userfield_name] = (userfield_value or
                                            row['userfield_value'])
                    args['source'] = "aktasks-%s" % task.id
                    handler.put(action.id, **args)
                                

                resp['log_id'] = row['user_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['user_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error

class ActionfieldRenameJobForm(BatchForm):
    help_text = """
The SQL must return a column named `actionfield_id`. New Actionfield Name is optional -- include it if you want all results renamed to the same thing.  Alternatively, you can cause the SQL to return a column named `new_actionfield_name`, and this will be used instead.  If your SQL returns a column named `new_actionfield_value`, or if you fill out the New Actionfield Value field, then actionfields will have their values updated as well.  If you leave this form field blank and do not return this column, the actionfield values will not be changed.
"""
    new_actionfield_name = forms.CharField(label="New Actionfield Name", 
                                           required=False)
    new_actionfield_value = forms.CharField(label="New Actionfield Value", 
                                           required=False)

    def run(self, task, rows):
        new_actionfield_name = self.cleaned_data.get(
            "new_actionfield_name").strip() or None
        new_actionfield_value = self.cleaned_data.get(
            "new_actionfield_value").strip() or None

        rest = RestClient()
        rest.safety_net = False

        n_rows = n_success = n_error = 0

        task_log = get_task_log()

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1
            assert row.get("actionfield_id") and int(row['actionfield_id'])

            actionfield = CoreActionField.objects.using("ak").select_related(
                "parent", "parent__page").get(
                id=row['actionfield_id'])
            data = {
                'parent': "/rest/v1/%saction/%s/" % (
                    actionfield.parent.page.type.lower(), 
                    actionfield.parent.id),
                'name': (new_actionfield_name
                         or row['new_actionfield_name']),
                'value': (new_actionfield_value 
                          or row.get('new_actionfield_value')
                          or actionfield.value),
                }
            try:
                resp = rest.actionfield.put(actionfield.id, **data)
                resp['log_id'] = row['actionfield_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['actionfield_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error

class ActionDeleteJobForm(BatchForm):
    help_text = """The SQL must return a column named `action_id`.  All of these actions will be really, permanently, deleted. Be careful."""

    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        n_rows = n_success = n_error = 0

        task_log = get_task_log()
        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1
            assert row.get("action_id") and int(row['action_id'])

            try:
                resp = rest.action.delete(id=row['action_id'])
            except AssertionError as e:
                resp = str(e)
                
            if resp is not None:
                n_error += 1
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error

class CustomFieldJSONForm(BatchForm):
    field_type = forms.CharField(label="Object type that this custom field is attached to (templateset, page, action, user, mailing)")
    help_text = """
The SQL must return a column named `ak_custom_field_id`
and a column named `json_primary_key`.
"""

    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        endpoint = getattr(rest, self.cleaned_data['field_type'] + "field")
        field = endpoint.get(id=49)
        current = json.loads(field['value'])
        
        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            try:
                assert row.get("ak_custom_field_id") and int(row['ak_custom_field_id'])
                assert row.get("json_primary_key")

                endpoint = getattr(rest, self.cleaned_data['field_type'] + "field")
                field = endpoint.get(id=row['ak_custom_field_id'])
                current = json.loads(field['value'])
                task_log.activity_log(task, current)
                current[str(row['json_primary_key'])] = row
                new = json.dumps(current)
                field['value'] = new
                resp = endpoint.put(**field)
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                task_log.error_log(
                    task, {
                        'row': row,
                        'error': traceback.format_exc(),
                    })
            else:
                n_success += 1
        return n_rows, n_success, n_error

class ActionCloneForm(BatchForm):
    help_text = """
The SQL must return columns named `action_id` and `new_page_name`

Each action will be fetched and its data (along with actionfields) will be
cloned to a new action against its corresponding `new_page_name`.

The new action's created_at will also be backdated to match the original action.

All columns apart from action_id, new_page_name, and delete_original_action
will be ignored by the job code (but can be used to review records for accuracy, log 
old values, etc)
"""

    delete_original_action = forms.ChoiceField(choices=[("no", "no"), ("yes", "yes")])
    skip_confirmation = forms.ChoiceField(choices=[("yes", "yes"), ("no", "no")])

    def run_one(self, task, row, task_log):
        rest = RestClient()
        rest.safety_net = False

        action_id = row['action_id']
        action = rest.action.get(id=action_id)
        for field_name in action.get("fields", {}):
            field_value = action['fields'][field_name]
            action['action_%s' % field_name] = field_value
        if self.cleaned_data['skip_confirmation'] != "no":
            action['skip_confirmation'] = 1
        action['page'] = row['new_page_name']
        action['return_full_response'] = True

        task_log.activity_log(task, action)
        resp = rest.action.create(**action)
        new_action_id = resp['id']
        action.pop("id")
        action.pop("page")
        type = resp['resource_uri'].split("/rest/v1/")[1].split("action/")[0]
        
        resp2 = getattr(rest, "%saction" % type).patch(id=new_action_id, **action)

        if self.cleaned_data['delete_original_action'] == "yes":
            resp3 = rest.action.delete(id=action_id)
            
        return resp
    
    def run(self, task, rows):
        task_log = get_task_log()
        
        n_rows = n_success = n_error = 0
        
        for row in rows:
            try:
                task_log.sql_log(task, row)
                resp = self.run_one(task, row, task_log)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['action_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1
                task_log.success_log(task, resp)
        return n_rows, n_success, n_error
    
class ActionModificationForm(BatchForm):
    help_text = """
The SQL must return columns named `action_id` and `action_type`

All columns with prefix `new_data_` will be treated as new values
for the core_action attributes.  For example, `select id as action_id, 
"signup" as action_type, concat("my-source-", id)
as new_data_source from core_action
where id in (100,101);` would cause four action records to have their
source attributes set to "my-source-100" and "my-source-101" respectively.

Columns prefixed new_data_action_* can also be used to set or update actionfield
values.

All columns apart from action_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""
    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("action_id") and int(row['action_id'])
            assert row.get("action_type")
            
            new_values = {"id": row['action_id']}
            new_values['fields'] = {}
            for key in row:
                if not key.startswith("new_data_"):
                    continue
                if key.startswith("new_data_action_"):
                    new_values['fields'][key.replace("new_data_action_", "", 1)] = row[key]
                else:
                    new_values[key.replace("new_data_", "", 1)] = row[key]
            if not new_values['fields']: new_values.pop("fields")

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                resp = getattr(rest, "%saction" % row['action_type'].lower()).put(id=row['action_id'], **new_values)
                resp = {
                    'put_response': resp
                }
                resp['log_id'] = row['action_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['action_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error

class OrderModificationForm(BatchForm):
    help_text = """
The SQL must return a column named `order_id`

All columns with prefix `new_data_` will be treated as new values
for the core_order attributes.  For example, `select id as order_id, 
"failed" as new_data_status from core_order where id in (100,101);` 
would cause two order records to be marked as cancelled.

All columns apart from order_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""
    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("order_id") and int(row['order_id'])
            
            new_values = {"id": row['order_id']}
            for key in row:
                if not key.startswith("new_data_"):
                    continue
                else:
                    new_values[key.replace("new_data_", "", 1)] = row[key]

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                resp = getattr(rest, "order").patch(id=row['order_id'], **new_values)
                resp = {
                    'patch_response': resp
                }
                resp['log_id'] = row['order_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['order_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error

class TransactionModificationForm(BatchForm):
    help_text = """
The SQL must return a column named `transaction_id`

All columns with prefix `new_data_` will be treated as new values
for the core_transaction attributes.  For example, `select id as order_id, 
"failed" as new_data_status from core_transaction where id in (100,101);` 
would cause two transaction records to be marked as cancelled.

All columns apart from transaction_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""
    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("transaction_id") and int(row['transaction_id'])
            
            new_values = {"id": row['transaction_id']}
            for key in row:
                if not key.startswith("new_data_"):
                    continue
                else:
                    new_values[key.replace("new_data_", "", 1)] = row[key]

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                resp = getattr(rest, "transaction").patch(id=row['transaction_id'], **new_values)
                resp = {
                    'patch_response': resp
                }
                resp['log_id'] = row['transaction_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['transaction_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error

class PageCustomFieldJSONForm(BatchForm):
    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("page_id") and int(row['page_id'])
            assert row.get("page_type")

            field_name = row['field_name']
            endpoint = getattr(rest, "%spage" % row['page_type'].lower())
            try:
                current = endpoint.get(id=row['page_id']).get("fields", {})
                previous = current = current.get(field_name)
                try:
                    current = json.loads(current)
                except (TypeError, ValueError) as e:
                    previous = '{}'
                    current = {}
                    
                current[str(row['json_primary_key'])] = row
                new = json.dumps(current)

                if json.loads(new) == json.loads(previous):
                    continue
                    
                resp = getattr(rest, "%spage" % row['page_type'].lower()).patch(
                    id=row['page_id'], fields={
                        field_name: new
                    })
                resp = {
                    'patch_response': resp
                }
                resp['log_id'] = row['page_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['page_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1
        
        return n_rows, n_success, n_error

class PageCreationForm(BatchForm):
    help_text = ""

    def find_page(self, task, page_uuid):
        return CorePage.objects.using("ak").filter(
                notes__endswith=page_uuid,
        ).first()

    def create_page(self, task, row):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()
        
        page_values = {}
        form_values = {}
        followup_values = {}
        for key in row:
            if key.startswith("new_data_page_"):
                page_values[
                    key.replace("new_data_page_", "", 1)] = row[key]
            elif key.startswith("new_data_form_"):
                form_values[
                    key.replace("new_data_form_", "", 1)] = row[key]
            elif key.startswith("new_data_followup_"):
                followup_values[
                    key.replace("new_data_followup_", "", 1)] = row[key]
                
        page_endpoint = getattr(rest, '%spage' % row['page_type'].lower())
        form_endpoint = getattr(rest, '%sform' % row['page_type'].lower())

        task_log.activity_log(task, {"page": page_values})
        try:
            page_id = id = page_endpoint.create(**page_values)
        except Exception as e:
            task_log.error_log(
                task, {"request": page_values, "error": str(e)})
            return False

        form_id = followup_id = None
        if row['page_type'].lower() != 'import':
            task_log.activity_log(task, {"page": id, "form": form_values})
            form_values['page'] = '/rest/v1/%spage/%s/' % (
                row['page_type'].lower(), id)
            try:
                form_id = id = form_endpoint.create(**form_values)
            except Exception as e:
                task_log.error_log(
                    task, {"request": form_values, "error": str(e)})
                return False

            task_log.activity_log(
                task, {"page": page_id, "followup": followup_values})
            followup_values['page'] = '/rest/v1/%spage/%s/' % (
                row['page_type'].lower(), page_id)
            try:
                followup_id = rest.pagefollowup.create(**followup_values)
            except Exception as e:
                task_log.error_log(
                    task, {"request": followup_values, "error": str(e)})
                return False

        task_log.success_log(task, {
            "page_id": page_id, "form_id": form_id, "followup_id": followup_id,
        })
        return page_id
    
    def run(self, task, rows):
        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("page_type")

            page = None
            if row.get("page_unique_identifier"):
                page = self.find_page(task, row['page_unique_identifier'])

            if not page:
                result = self.create_page(task, row)
                if result:
                    n_success += 1
                else:
                    n_error += 1

        return n_rows, n_success, n_error
    
class PageModificationForm(BatchForm):
    help_text = """
The SQL must return columns named `page_id` and `page_type`

All columns with prefix `new_data_` will be treated as new values
for the core_[page_type]page attributes.  For example, `select id as page_id, 
"signup" as page_type, concat("purple-", title)
as new_data_title from core_page
where id in (100,101);` would cause two signup page records to have "purple-"
prepended to their titles.

Columns prefixed new_data_page_* can also be used to set or update pagefield
values.

A special columns new_data_page_tags can be used to tag the page with a comma-separated list of tag IDs.

Alternatively, a special column new_data_page_tag_unique_identifiers can be used to tag the page with a comma-separated list of tag UUIDs if a tag_mapping_database_page_id is provided.

All columns apart from page_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""

    tag_mapping_database_page_id = forms.IntegerField(required=False)    
    
    def find_page(self, page_uuid):
        return CorePage.objects.using("ak").filter(
                notes__endswith=page_uuid,
        ).first()

    def find_tag(self, tag_uuid):
        if hasattr(self, '_cached_find_tag'):
            fields = getattr(self, '_cached_find_tag')
        else:
            fields = CorePageField.objects.using("ak").filter(
                parent_id=self.cleaned_data['tag_mapping_database_page_id'],
                name='zyx_stashing_data_code',
            )
            fields = dict(
                f.value.split('=', 1) for f in fields
            )
            setattr(self, '_cached_find_tag', fields)

        return fields[str(tag_uuid)]

    def urlscrape(self, command):
        url, command = command.split('#', 1)
        command = parse_qs(command)
        resp = requests.get(
            url, headers={
                "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}
        )
        content = lxml.html.fromstring(resp.text)
        content = content.cssselect(command['selector'][0])[0]

        if 'removeClass' in command:
            eligible = content.cssselect('.' + command['removeClass'][0])
            for el in eligible:
                el.classes.discard(command['removeClass'][0])

        if 'append' in command and 'appendAfter' in command:
            el = content.cssselect(command['appendAfter'][0])[-1]
            el.addnext(ET.XML(command['append'][0]))

        content = lxml.html.tostring(content)
        return content.decode("utf-8")

    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            if row.get("page_unique_identifier") and not row.get("page_id"):
                page = self.find_page(row['page_unique_identifier'])
                if not page:
                    task_log.error_log(task, {
                        "message": "Could not find page",
                        "page_unique_identifier": row.get("page_unique_identifier"),
                        "row": row,
                    })
                    continue
                row['page_id'] = page.id
            
            assert row.get("page_id") and int(row['page_id'])
            assert row.get("page_type")
            
            new_values = {"id": row['page_id']}
            new_values['fields'] = {}
            new_values['tags'] = []
                        
            for key in row:
                if not key.startswith("new_data_"):
                    continue
                if key == "new_data_page_tag_unique_identifiers":
                    try:
                        new_values['tags'] = [
                            int(self.find_tag(k))
                            for k in row[key].split(",")
                        ]
                    except KeyError as e:
                        task_log.error_log(task, {
                            "row": row,
                            "message": "Could not find tag",
                            "error": str(e),
                        })
                        continue                        
                elif key == "new_data_page_tags":
                    new_values['tags'] = [int(k) for k in row[key].split(",")]
                elif key.startswith("new_data_urlscrape_page_"):
                    new_values['fields'][key.replace("new_data_urlscrape_page_", "", 1)] = self.urlscrape(row[key])
                elif key.startswith("new_data_page_"):
                    new_values['fields'][key.replace("new_data_page_", "", 1)] = row[key]
                elif key.startswith("new_data_urlscrape_"):
                    new_values[key.replace("new_data_urlscrape_", "", 1)] = self.urlscrape(row[key])
                else:
                    new_values[key.replace("new_data_", "", 1)] = row[key]
            if not new_values['fields']: new_values.pop("fields")
            if not new_values['tags']: new_values.pop('tags')

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                resp = getattr(rest, "%spage" % row['page_type'].lower()).patch(id=row['page_id'], **new_values)
                resp = {
                    'patch_response': resp
                }
                resp['log_id'] = row['page_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['page_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error


class EventSignupModificationForm(BatchForm):
    help_text = """
The SQL must return a column named `signup_id`

All columns with prefix `new_data_` will be treated as new values
for the events_eventsignup attributes.  For example, `select id as signup_id, 
"cancelled" as new_data_status
where id in (100,101);` would cause two event signup records 
to become cancelled.

All columns apart from signup_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""
    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("signup_id") and int(row['signup_id'])
            
            new_values = {"id": row['signup_id']}
            new_values['fields'] = {}
            for key in row:
                if not key.startswith("new_data_"):
                    continue
                if key.startswith("new_data_action_"):
                    new_values['fields'][key.replace("new_data_action_", "", 1)] = row[key]
                else:
                    new_values[key.replace("new_data_", "", 1)] = row[key]
            if not new_values['fields']: new_values.pop("fields")

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                resp = rest.eventsignup.patch(id=row['signup_id'], **new_values)
                resp = {
                    'patch_response': resp
                }
                resp['log_id'] = row['signup_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['signup_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1
                task_log.success_log(task, resp)

        return n_rows, n_success, n_error

class EventModificationForm(BatchForm):
    help_text = """
The SQL must return a column named `event_id``

All columns with prefix `new_data_` will be treated as new values
for the events_event attributes.  For example, `select id as event_id, 
true as new_data_host_is_confirmed, "Turn left" as new_data_directions from events_event
where id in (100,101);` would cause two event records to become confirmed
and have their directions updated.

Columns with prefix `tmpl_new_data_` will be run through Django's template engine
and then treated as new values for the evente_event attributes as above. A dictionary
representation of the row will be passed in as the template context. For example, 
`select "An event in {{city}}, {{state}}" as tmpl_new_data_title, city, state 
from events_event`.

TODO, UNTESTED: Columns prefixed new_data_action_* can also be used to set or update 
event custom field values.

All columns apart from event_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""
    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False
        xmlrpc = Client()
        
        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("event_id") and int(row['event_id'])
            
            new_values = {"id": row['event_id']}
            new_values['fields'] = {}
            for key in row:
                if key.startswith("tmpl_new_data_action_"):
                    new_values['fields'][key.replace("tmpl_new_data_action_", "", 1)] = (
                        Template(row[key]).render(Context(row))
                    )
                elif key.startswith("tmpl_new_data_"):
                    new_values[key.replace("tmpl_new_data_", "", 1)] = (
                        Template(row[key]).render(Context(row))
                    )
                elif not key.startswith("new_data_"):
                    continue
                elif key.startswith("new_data_action_"):
                    new_values['fields'][key.replace("new_data_action_", "", 1)] = row[key]
                else:
                    new_values[key.replace("new_data_", "", 1)] = row[key]
            fields = new_values.pop("fields")

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                if new_values:
                    resp = rest.event.put(id=row['event_id'], **new_values)
                    resp = {
                        'put_response': resp
                    }
                else:
                    resp = {"put_response": None}
                if fields:
                    fields['id'] = row['event_id']
                    resp2 = xmlrpc.Event.set_custom_fields(fields)
                    resp['custom_fields_response'] = resp2
                resp['log_id'] = row['event_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['event_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error


class EventHtmlEntitiesForm(BatchForm):

    def cleanupString(self, string):
        string = unquote(string).decode('utf8')
        return HTMLParser().unescape(string).encode('utf8')

    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("event_id") and int(row['event_id'])
            assert row.get("public_description")
            
            new_values = {"id": row['event_id']}
            new_values['public_description'] = self.cleanupString(row['public_description'])
            
            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                resp = rest.event.put(id=row['event_id'], **new_values)
                resp = {
                    'put_response': resp
                }
                resp['log_id'] = row['event_id']
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['event_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error
    

class UserModificationForm(BatchForm):

    actionkit_page = forms.CharField(label="Optional tracking page name",
                                     required=False)

    help_text = """
The SQL must return a column named `user_id`.

All columns with prefix `new_data_` will be treated as new values
for the core_user attributes.  For example, `select user_id, "United Kingdom"
as new_data_country, "London" as new_data_city, country, city from core_user
where id in (100,101,102,103);` would cause four user records to have their
country and city attributes set to "United Kingdom" and "London" respectively.

Columns prefixed new_data_user_* can also be used to set or update userfield
values.

All columns apart from user_id and new_data_* will be ignored by the job code
(but can be used to review records for accuracy, log old values, etc)
"""
    def run(self, task, rows):
        rest = RestClient()
        rest.safety_net = False

        task_log = get_task_log()

        n_rows = n_success = n_error = 0

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("user_id") and int(row['user_id'])

            new_values = {"id": row['user_id']}
            new_values['fields'] = {}
            for key in row:
                if not key.startswith("new_data_"):
                    continue
                if key.startswith("new_data_user_"):
                    new_values['fields'][key.replace("new_data_user_", "", 1)] = row[key]
                else:
                    new_values[key.replace("new_data_", "", 1)] = row[key]
            if not new_values['fields']: new_values.pop("fields")

            task_log.activity_log(task, new_values)
            new_values.pop("id")
            try:
                resp = rest.user.put(id=row['user_id'], **new_values)
                resp = {
                    'put_response': resp
                }
                resp['log_id'] = row['user_id']

                if self.cleaned_data.get("actionkit_page"):
                    page = self.cleaned_data['actionkit_page']
                    data = {"id": row['user_id'], "page": page}
                    for field in row:
                        if field.startswith("action_"):
                            data[field] = row[field]
                    api = Client()
                    tracking_resp = api.act(data)
                    resp['tracking_action'] = tracking_resp
                task_log.success_log(task, resp)
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['user_id']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error



class UnsubscribeAndActJobForm(BatchForm):
    help_text = """
<p>The SQL must return a column named `user_id` OR a column named `email`.</p>
<p>The SQL may return a column named `page_name_to_act_on` if this should vary per row.</p>
<p>Unsubscribes will only occur for rows with a `user_id`. The `email` column 
can only be used for the "act" portion of this job, either to create actions 
for not-yet-existing users, or to create actions without first looking up
their user IDs.</p>

<p>Any columns named like `act_*` will be treated as actionfields for the 
"act" portion of the job.</p>

<p>If the SQL returns a column named `caused_by_action`, the value of this column
will be stored in a custom actionfield `caused_by_action` on the action page that
each user is marked as acting on.</p>

<p>If unsubscribe_lists is not set, no unsubscriptions will occur.</p>
"""

    unsubscribe_lists = forms.CharField(label="List IDs to Unsubscribe From",
                                        required=False)
    action_page = forms.CharField(label="Page Names to Act On", required=False)

    def run(self, task, rows):
        lists = self.cleaned_data.get('unsubscribe_lists') or ""
        lists = [int(i.strip()) for i in lists.split(",") if i]

        ak = Client()

        n_rows = n_success = n_error = 0

        task_log = get_task_log()

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1
            assert (
                (row.get('user_id') and int(row['user_id']))
                or
                (row.get('email'))
            )

            try:
                user_id = row['user_id']
            except KeyError:
                user_id = None

            caused_by_action = row.get('caused_by_action') or None
            unsubs = []
            for list_id in lists:
                if not user_id:
                    continue
                try:
                    ak.User.unsubscribe({'id': user_id, 'list_id': list_id})
                except:
                    pass
                else:
                    unsubs.append(list_id)
            page = self.cleaned_data.get('action_page', '').strip() or None
            if page is None:
                page = row.get("page_name_to_act_on")        
            if page is None:
                continue
            if user_id:
                action = {'id': user_id, 'page': page}
            else:
                action = {'email': row['email'], 'page': page}
            if unsubs:
                action['action_unsubscribed_from_lists'] = unsubs
            if caused_by_action:
                action['action_caused_by_action'] = caused_by_action
            for key in row:
                if key.startswith("act_"):
                    action[key[4:]] = row[key]
            try:
                resp = ak.act(action)
                resp['log_id'] = row['user_id'] if user_id else row['email']
                assert resp['status']
            except Exception as e:
                n_error += 1
                resp = {}
                resp['log_id'] = row['user_id'] if user_id else row['email']
                resp['error'] = traceback.format_exc()
                task_log.error_log(task, resp)
            else:
                n_success += 1

        return n_rows, n_success, n_error
