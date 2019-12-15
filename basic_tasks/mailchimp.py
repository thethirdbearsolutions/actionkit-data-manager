from django import forms
from django.db import connections
import json
import requests
import hashlib
import traceback

from main.forms import BatchForm, get_task_log
from json import load

class MailchimpBaseForm(BatchForm):

    mailchimp_list_id = forms.CharField(label="List ID", required=True)
    mailchimp_username = forms.CharField(label="Username", required=True)
    mailchimp_token = forms.CharField(label="Token", required=True)
    mailchimp_endpoint = forms.CharField(label="Endpoint", required=True)        
    
    def request(self, method, path, data):
        return getattr(requests, method)(
            "{endpoint}/3.0/lists/{list_id}{path}".format(
                endpoint=self.cleaned_data['mailchimp_endpoint'].rstrip("/"),
                list_id=self.cleaned_data['mailchimp_list_id'],
                path=path,
            ), auth=(
                self.cleaned_data['mailchimp_username'],
                self.cleaned_data['mailchimp_token'],
            ), data=json.dumps(data)
        )
                
class MailchimpRemoveSubscriberTagForm(MailchimpBaseForm):
    mailchimp_segment_id = forms.CharField(label="Segment ID", required=True)
    
    def run(self, task, rows):
        n_rows = n_success = n_error = 0

        task_log = get_task_log()

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("email")

            obj = {
                "email_address": row['email'],
            }

            task_log.activity_log(task, obj)
            resp = None
            try:
                resp = self.request(
                    "delete",
                    "/segments/{segment_id}/members/{email_hash}".format(
                        segment_id=self.cleaned_data['mailchimp_segment_id'],
                        email_hash=hashlib.md5(row['email'].lower()).hexdigest(),
                    ),
                    obj
                )
                assert resp.status_code == 204
            except Exception as e:
                n_error += 1
                task_log.error_log(task, {
                    "row": obj,
                    "error": str(e),
                    "resp": resp.text if resp else None
                })
            else:
                n_success += 1
                task_log.success_log(task, {"row": obj})

        return n_rows, n_success, n_error


class MailchimpAddSubscriberTagForm(MailchimpBaseForm):

    mailchimp_segment_id = forms.CharField(label="Segment ID", required=True)
    
    def run(self, task, rows):
        n_rows = n_success = n_error = 0

        task_log = get_task_log()

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("email")

            obj = {
                "email_address": row['email'],
            }

            task_log.activity_log(task, obj)
            resp = None
            try:
                resp = self.request(
                    "post",
                    "/segments/%s/members" % self.cleaned_data['mailchimp_segment_id'],
                    obj
                )
                assert resp.status_code == 200
            except Exception as e:
                n_error += 1
                task_log.error_log(task, {
                    "row": obj,
                    "error": str(e),
                    "resp": resp.text if resp else None
                })
            else:
                n_success += 1
                task_log.success_log(task, {"row": obj, "resp": resp.json()})

        return n_rows, n_success, n_error


class MailchimpAddSubscriberForm(MailchimpBaseForm):
    def run(self, task, rows):
        n_rows = n_success = n_error = 0

        task_log = get_task_log()

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            assert row.get("email")
            assert row.get("status")

            obj = {
                "email_address": row['email'],
                "status_if_new": row['status'],
                "status": row['status'],
                "merge_fields": {},
            }

            for key in row:
                if key.startswith("new_data_merge_"):
                    obj["merge_fields"][key[15:]] = row[key]
                elif key.startswith("new_data_"):
                    obj[key[9:]] = row[key]

            if not obj['merge_fields']:
                obj.pop("merge_fields")

            task_log.activity_log(task, obj)
            resp = None
            try:
                resp = self.request(
                    "put",
                    "/members/%s" % hashlib.md5(row['email'].lower()).hexdigest(),
                    obj
                )
                assert resp.status_code == 200
            except Exception as e:
                n_error += 1
                task_log.error_log(task, {
                    "row": obj,
                    "error": str(e),
                    "resp": resp.text if resp else None
                })
            else:
                n_success += 1
                task_log.success_log(task, {"row": obj, "resp": resp.json()})

        return n_rows, n_success, n_error




