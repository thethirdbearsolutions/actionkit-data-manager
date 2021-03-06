import datetime
import json
import logging
from .models import LogEntry
logger = logging.getLogger('aktasks')

class TaskLogger(object):

    def _str(self, task, *args):
        log_str = [unicode(task)]
        for arg in args:
            log_str.append(repr(arg))
        return " ".join(log_str)

    def activity_log(self, task, *args):
        logger.debug(self._str(task, *args))

    def sql_log(self, task, *args):
        logger.info(self._str(task, *args))

    def error_log(self, task, *args):
        logger.error(self._str(task, *args))

    def success_log(self, task, *args):
        logger.warn(self._str(task, *args))

def dthandler(obj):
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    try:
        return str(obj)
    except Exception:
        return None

class SqlTaskLogger(object):

    def _log(self, task, type, *args):
        log_str = json.dumps(args, default=dthandler)
        LogEntry(task=task, type=type, data=log_str).save()

    def activity_log(self, task, *args):
        self._log(task, "activity", *args)

    def sql_log(self, task, *args):
        self._log(task, "sql", *args)

    def error_log(self, task, *args):
        self._log(task, "error", *args)

    def success_log(self, task, *args):
        self._log(task, "success", *args)
