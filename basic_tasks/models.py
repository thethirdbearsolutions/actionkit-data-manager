from main.task_registry import register_task
from basic_tasks.forms import (
    UnsubscribeAndActJobForm,
    ActionfieldRenameJobForm,
    UserfieldJobForm,
    EventFieldCreateForm,
    PublishReportResultsForm,
    ActionkitSpreadsheetForm,
    ActionDeleteJobForm,
    UserModificationForm,
)

register_task("UserfieldJob", 
              "Apply Userfield to Batch", UserfieldJobForm)
register_task("UnsubscribeAndActJob", 
              "Subscribe/Unsubscribe: unsubscribe users from zero or more lists, "
              "and act on zero or one pages", UnsubscribeAndActJobForm),
register_task("ActionfieldRenameJob", 
              "Rename Actionfields", ActionfieldRenameJobForm)
register_task("PublishReportResultsJob",
              "Run and Publish Reports", PublishReportResultsForm)
register_task("ActionkitSpreadsheetJob", 
              "Stream sql results to a Google spreadsheet", ActionkitSpreadsheetForm)
register_task("ActionDeleteJob", "Delete Actions", ActionDeleteJobForm)
register_task("UserModificationJob", "Modify Users", UserModificationForm)
register_task("EventFieldCreateJob",
              "Apply event field to batch", EventFieldCreateForm)
