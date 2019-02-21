from main.task_registry import register_task
from basic_tasks.google_tasks import (
    CopyS3FilesToDriveForm,
    AddSharedFolderToDriveForm,
    DeleteMigratedFileFromS3Form,
)
from basic_tasks.forms import (
    UnsubscribeAndActJobForm,
    UserMergeForm,
    PageModificationForm,
    ActionfieldRenameJobForm,
    UserfieldJobForm,
    EventFieldCreateForm,
    ActionCloneForm,
    EventGeolocationForm,
    PublishReportResultsForm,
    ActionkitSpreadsheetForm,
    ActionDeleteJobForm,
    ActionModificationForm,
    UserModificationForm,
    EventModificationForm,
    EventHtmlEntitiesForm,
    EventSignupModificationForm,
    CustomFieldJSONForm,
    CloudinaryImageForm,
    PageCustomFieldJSONForm,    
)
from basic_tasks.mailchimp import (
    MailchimpAddSubscriberForm,
    MailchimpAddSubscriberTagForm,
    MailchimpRemoveSubscriberTagForm,    
)

register_task("CloudinaryImageJob", "Cloudinary", CloudinaryImageForm)
register_task("MailchimpAddSubscriberJob", "Push subscriber info to Mailchimp",
              MailchimpAddSubscriberForm)
register_task("MailchimpAddSubscriberTagJob", "Add Mailchimp tag to subscriber",
              MailchimpAddSubscriberTagForm)
register_task("MailchimpRemoveSubscriberTagJob", "Remove Mailchimp tag from subscriber",
              MailchimpRemoveSubscriberTagForm)
register_task("UserMergeJob", "Merge users", UserMergeForm)
register_task("DeleteMigratedFileFromS3Job",
             "Delete file from s3 if checksum matches Google Drive",
             DeleteMigratedFileFromS3Form)
register_task("AddSharedFolderToDriveJob", "Add shared folders to drive",
              AddSharedFolderToDriveForm)
register_task("CopyS3FilesToDriveJob", "Copy s3 to drive", CopyS3FilesToDriveForm)
>>>>>>> wip-google-sheets
register_task("UserfieldJob", 
              "Apply Userfield to Batch", UserfieldJobForm)
register_task("CustomFieldJSONJob", 
              "Sync rows to JSON blob in a custom field", CustomFieldJSONForm)
register_task("EventGeolocationJob", 
              "Geolocate events", EventGeolocationForm)
register_task("UnsubscribeAndActJob", 
              "Subscribe/Unsubscribe: unsubscribe users from zero or more lists, "
              "and act on zero or one pages", UnsubscribeAndActJobForm),
register_task("ActionfieldRenameJob", 
              "Rename Actionfields", ActionfieldRenameJobForm)
register_task("PublishReportResultsJob",
              "Run and Publish Reports", PublishReportResultsForm)
register_task("ActionCloneJob", "Clone and potentially delete actions",
              ActionCloneForm)
register_task("ActionkitSpreadsheetJob", 
              "Stream sql results to a Google spreadsheet", ActionkitSpreadsheetForm)
register_task("ActionDeleteJob", "Delete Actions", ActionDeleteJobForm)
register_task("ActionModificationJob", "Modify Actions", ActionModificationForm)
register_task("PageModificationJob", "Modify Pages", PageModificationForm)
register_task("PageCustomFieldJSONJob", "Modify Page JSON Custom Field",
              PageCustomFieldJSONForm)
register_task("UserModificationJob", "Modify Users", UserModificationForm)
register_task("EventModificationJob", "Modify Events", EventModificationForm)
register_task("EventHtmlEntitiesJob", "Clean up HTML entities in event.public_description", EventHtmlEntitiesForm)
register_task("EventSignupModificationJob", "Modify Event signups",
              EventSignupModificationForm)
register_task("EventFieldCreateJob",
              "Apply event field to batch", EventFieldCreateForm)
