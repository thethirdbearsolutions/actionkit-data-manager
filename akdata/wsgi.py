"""
WSGI config for akdata project.

It exposes the WSGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/howto/deployment/wsgi/
"""

import os

from django.core.wsgi import get_wsgi_application

import dotenv
dotenv.read_dotenv('./akdata/.env')

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "akdata.settings")

application = get_wsgi_application()
