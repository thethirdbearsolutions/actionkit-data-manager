from main.forms import BatchForm, get_task_log
from django import forms
import google.oauth2.credentials
from googleapiclient.discovery import build
import requests
import re
import os.path
try:
    from urllib.parse import unquote as url_unquote
except:
    from urllib import unquote as url_unquote
import hashlib

from actionkit.rest import client as RestClient
import apiclient.errors
from apiclient.http import MediaFileUpload
import boto3
from googleapiclient.errors import HttpError
import gdata.spreadsheet.service

class MissingFileError(Exception):
    def __init__(self, url, resp=None):
        self.url = url
        self.status = resp.status_code if resp else None
        self.headers = resp.headers if resp else None

def get_md5_local(filename):
    try:
        f = open(filename, 'rb')
    except IOError:
        raise MissingFileError(filename)
    
    m = hashlib.md5()
    while True:
        data = f.read(10240)
        if len(data) == 0:
            break
        m.update(data)
    return m.hexdigest()

def get_md5_s3(url):
    resp = requests.head(url)
    if resp.status_code != 200 or 'Etag' not in resp.headers:
        raise MissingFileError(url, resp)
    return resp.headers['Etag'].strip('"')

def get_md5_gdrive(id, api):
    file = api.files().get(
        fileId=id, fields='md5Checksum',
    ).execute()
    return file['md5Checksum']

class ActionkitSpreadsheetForm(BatchForm):

    exclude = forms.CharField(label="Comma separated list of IDs to exclude (0 for auto appends, n/a to disable this feature)", required=True)
    google_client_id = forms.CharField(label="Client ID", required=True)
    google_client_secret = forms.CharField(label="Client secret", required=True)
    google_refresh_token = forms.CharField(label="Refresh token", required=True)
    google_spreadsheet_id = forms.CharField(label="Spreadsheet ID", required=True)
    google_worksheet_id = forms.CharField(label="Worksheet ID", required=True)

    primary_key = forms.CharField(label="Name of column to dedupe against (default: primary_key)", required=False)
    
    def run(self, task, rows):
        task_log = get_task_log()

        resp = requests.post("https://accounts.google.com/o/oauth2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": self.cleaned_data['google_refresh_token'],
            "client_id": self.cleaned_data['google_client_id'],
            "client_secret": self.cleaned_data['google_client_secret']})
        token = resp.json()['access_token']
        spr_client = gdata.spreadsheet.service.SpreadsheetsService(additional_headers={"Authorization": "Bearer %s" % token})

        if self.cleaned_data['exclude'] == 'n/a':
            exclude = []
        else:
            exclude = self.cleaned_data['exclude'].split(",")
        n_errors = n_rows = n_success = 0

        feed = spr_client.GetCellsFeed(self.cleaned_data['google_spreadsheet_id'], self.cleaned_data['google_worksheet_id'])
        existing = []

        cols = []
        for row in feed.entry:
            if int(row.cell.row) > 1:
                break
            cols.append(row.cell.text.lower().replace("_", ""))

        current_row = 2
        obj = {}
        for row in feed.entry:
            if int(row.cell.row) < current_row:
                continue
            if int(row.cell.row) > current_row:
                existing.append(obj)
                obj = {}
                current_row = int(row.cell.row)
            obj[ cols[int(row.cell.col) - 1] ] = row.cell.text
        existing.append(obj)

        primary_key = self.cleaned_data.get("primary_key") or "primary_key"
        existing_keys = [i[primary_key] for i in existing if primary_key in i]
        
        for row in rows:

            n_rows += 1
            obj = {}
            for key, val in row.items():
                obj[key.lower().replace("_", "")] = unicode(val)

            id = obj.get(primary_key.lower().replace("_", "")) if primary_key else None
            if id and id in exclude:
                continue
            elif id and id in existing_keys:
                continue
            
            try:
                spr_client.InsertRow(obj, 
                                     self.cleaned_data['google_spreadsheet_id'], 
                                     self.cleaned_data['google_worksheet_id'])
            except Exception as e:
                n_errors += 1
                task_log.error_log(task, {"row": obj, "error": str(e)})
            else:
                n_success += 1
                exclude.append(id)
        if self.cleaned_data['exclude'] != 'n/a':
            exclude = ','.join([str(i) for i in exclude])
            task.form_data = json.dumps({"exclude": exclude})
        task.save()

        return n_rows, n_success, n_errors

class DeleteMigratedFileFromS3Form(BatchForm):

    google_client_id = forms.CharField(label="Client ID", required=True)
    google_client_secret = forms.CharField(label="Client secret", required=True)
    google_refresh_token = forms.CharField(label="Refresh token", required=True)

    aws_access_key_id = forms.CharField(label="AWS Access Key", required=True)
    aws_secret_access_key = forms.CharField(label="AWS Secret Key", required=True)
    
    def run(self, task, rows):
        task_log = get_task_log()
        n_rows = n_success = n_error = 0

        resp = requests.post(
            "https://accounts.google.com/o/oauth2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": self.cleaned_data['google_refresh_token'],
                "client_id": self.cleaned_data['google_client_id'],
                "client_secret": self.cleaned_data['google_client_secret']
            }
        )
        token = resp.json()['access_token']

        credentials = google.oauth2.credentials.Credentials(token)

        rest = RestClient()
        rest.safety_net = False
        
        api = build('drive', 'v3', credentials=credentials)
        aws_session = boto3.Session(
            aws_access_key_id=self.cleaned_data['aws_access_key_id'],
            aws_secret_access_key=self.cleaned_data['aws_secret_access_key'],
        )
        s3 = aws_session.resource('s3')

        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1

            s3_md5 = get_md5_s3(row['s3_url'])
            gdrive_md5 = get_md5_gdrive(row['gdrive_id'], api)

            task_log.activity_log(task, {
                "row": row,
                "checksums": {
                    "s3": s3_md5,
                    "gdrive": gdrive_md5,
                }
            })

            if s3_md5 != gdrive_md5:
                n_error += 1
                task_log.error_log(task, {
                    "row": row,
                    "checksums": {
                        "s3": s3_md5,
                        "gdrive": gdrive_md5,
                    }
                })
                continue

            bucket, key = re.match(
                "^https://([A-Za-z0-9\-]+).s3.amazonaws.com/(.*)$",
                row['s3_url'],
            ).groups()
            key = url_unquote(key)

            s3_resp = s3.Object(bucket, key).delete()
            ak_resp = rest.actionfield.delete(row['s3url_field_id'])

            n_success += 1
            task_log.success_log(task, {
                "row": row, "s3": s3_resp, "ak": ak_resp
            })
        return n_rows, n_success, n_error

class AddSharedFolderToDriveForm(BatchForm):

    google_client_id = forms.CharField(label="Client ID", required=True)
    google_client_secret = forms.CharField(label="Client secret", required=True)
    google_refresh_token = forms.CharField(label="Refresh token", required=True)
        
    def run(self, task, rows):

        task_log = get_task_log()
        n_rows = n_success = n_error = 0

        resp = requests.post("https://accounts.google.com/o/oauth2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": self.cleaned_data['google_refresh_token'],
            "client_id": self.cleaned_data['google_client_id'],
            "client_secret": self.cleaned_data['google_client_secret']})
        token = resp.json()['access_token']

        credentials = google.oauth2.credentials.Credentials(token)

        api = build('drive', 'v3', credentials=credentials)
        
        for row in rows:
            task_log.sql_log(task, row)
            n_rows += 1
            q = ("(name='%s' or name='%s') "
                 "and mimeType='application/vnd.google-apps.folder' "
                 "and sharedWithMe " % (row['gdrive_folder'],
                                        url_unquote(row['gdrive_folder'])))
            folder = api.files().list(q=q, fields="files(id)",).execute()

            if len(folder['files']) != 1:
                task_log.error_log(task,
                                   {"row": row,
                                    "error": "cannot_find_folder",
                                    "query": q,
                                    "resp": folder})
                n_error += 1
                continue
            
            folder_id = folder['files'][0]['id']

            # @@TODO skip if already true
            resp = api.files().update(
                fileId=folder_id,
                addParents='root',
            ).execute()
            task_log.activity_log(task, {"row": row, "resp": resp})
            n_success += 1
        return n_rows, n_success, n_error
            

class CopyS3FilesToDriveForm(BatchForm):

    google_client_id = forms.CharField(label="Client ID", required=True)
    google_client_secret = forms.CharField(label="Client secret", required=True)
    google_refresh_token = forms.CharField(label="Refresh token", required=True)
    
    def run(self, task, rows):

        task_log = get_task_log()

        resp = requests.post("https://accounts.google.com/o/oauth2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": self.cleaned_data['google_refresh_token'],
            "client_id": self.cleaned_data['google_client_id'],
            "client_secret": self.cleaned_data['google_client_secret']})
        token = resp.json()['access_token']

        credentials = google.oauth2.credentials.Credentials(token)

        api = build('drive', 'v3', credentials=credentials)

        rest = RestClient()
        rest.safety_net = False

        n_rows = n_success = n_error = 0
        
        for row in rows:
            n_rows += 1
            try:
                self.maybe_download(row['s3_url'], row['local_dir'])
            except MissingFileError as e:
                filename = os.path.basename(row['s3_url'])
                filepath = os.path.join(row['local_dir'], filename)
                try:
                    get_md5_local(filepath)
                except MissingFileError as f:
                    n_error += 1
                    task_log.error_log(task, {"row": row,
                                              "error": "s3_file_missing",
                                              "url": e.url,
                                              "headers": e.headers,
                                              "status": e.status,
                    })
                    continue
                task_log.activity_log(task, {
                    "row": row,
                    "warning": "s3_file_missing_but_exists_locally",
                    "url": e.url,
                    "headers": e.headers,
                    "status": e.status,
                })
            try:
                googleInfo = self.maybe_upload(api, row['s3_url'],
                                               row['local_dir'],
                                               row['gdrive_folder'])
            except AssertionError as e:
                n_error += 1
                task_log.error_log(task, {"row": row,
                                          "error": "cannot_find_folder",
                                          "resp": e.message})
                continue
            except HttpError as e:
                n_error += 1
                task_log.error_log(task, {"row": row,
                                          "error": "unknown_error",
                                          "resp": str(e)})
                continue
            task_log.activity_log(task, {"row": row, "google": googleInfo})
            fields = {
                row['id_field']: googleInfo['id'] if not row['id_value'] else None,
                row['link_field']: googleInfo['webContentLink'] if not row['link_value'] else None,
            }
            parent = '/rest/v1/%saction/%s/' % (row['action_type'], row['action_id'])
            resp = {}
            errored = False
            for key in fields:
                if fields[key] is None:
                    continue
                try:
                    resp[key] = {
                        "value": fields[key],
                        "id": rest.actionfield.create(
                            action=parent,
                            name=key,
                            value=fields[key],
                        )
                    }
                except Exception as e:
                    resp[key] = str(e)
                    errored = True
            if errored:
                task_log.error_log(task, {"row": row, "resp": resp})
                n_error += 1
            else:
                task_log.success_log(task, {"row": row, "resp": resp})
                n_success += 1
        return n_rows, n_success, n_error        
            
    def maybe_upload(self, api, url, directory, folder_name):
        filename = os.path.basename(url)
        filepath = os.path.join(directory, filename)

        local_md5 = get_md5_local(filepath)

        folder = api.files().list(
            q="name='%s' and mimeType='application/vnd.google-apps.folder'" % folder_name,
            fields="files(id)",
        ).execute()
        if len(folder['files']) != 1:
            raise AssertionError(folder)
        
        folder_id = folder['files'][0]['id']
        
        files = api.files().list(
            q="'%s' in parents and (name='%s' or name='%s')" % (folder_id, filename.replace("'", "\\'"),
                                                                url_unquote(filename).replace("'", "\\'")),
            fields="files(id,name,md5Checksum,webContentLink)",
        ).execute()
        for file in files['files']:
            if file['md5Checksum'] == local_md5:
                return file
        result = self.actually_upload(api, filename, filepath, folder_id)
        assert result is not None
        return result

    def actually_upload(self, api, file_name, filepath, folder_id):
        body = {'name': url_unquote(file_name), 'parents': [folder_id]}
        #'mimeType': 'application/vnd.google-apps.document'}
        media = MediaFileUpload(filepath, resumable=True)
        file = api.files().create(body=body, media_body=media)
        response = None
        while response is None:
            try:
                status, response = file.next_chunk()
                if status:
                    print("Uploaded %d%%." % int(status.progress() * 100))
            except apiclient.errors.HttpError as e:
                if e.resp.status in [404]:
                    # Start the upload all over again.
                    return self.actually_upload(file_name, filepath) 
                elif e.resp.status in [500, 502, 503, 504]:
                    # Call next_chunk() again, but
                    # @@TODO use an exponential backoff for repeated errors.
                    response = None
                    continue 
                else:
                    # Do not retry. Log the error and fail.
                    raise
        googleId = response['id']
        googleLink = api.files().get(
            fileId=googleId, fields='webContentLink').execute()
        googleLink = googleLink['webContentLink']
        return {'id': googleId, 'webContentLink': googleLink}
        
    def maybe_download(self, url, directory):
        filename = os.path.basename(url)
        filepath = os.path.join(directory, filename)

        remote_md5 = get_md5_s3(url)
        try:
            local_md5 = get_md5_local(filepath)
        except MissingFileError as e:
            local_md5 = None

        if remote_md5 == local_md5:
            return
        
        r = requests.get(url, stream=True)

        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))
        with open(filepath, 'wb') as fp:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    fp.write(chunk)
        local_md5 = get_md5_local(filepath)

        assert local_md5 == remote_md5
        
    def query(self):
        """@@TODO"""
        query = ["mimeType='application/vnd.google-apps.folder'"]
        query.append("'1SpxRz0LLuirCdE0m3wlVpGZFHnpi3QGd' in parents")
        query = " and ".join(query)
        
        files = api.files().list(
            
        ).execute()
        
        import pdb; pdb.set_trace()

