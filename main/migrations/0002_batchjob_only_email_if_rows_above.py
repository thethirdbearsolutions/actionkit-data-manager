# -*- coding: utf-8 -*-
# Generated by Django 1.11.8 on 2017-12-11 08:59
from __future__ import unicode_literals

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='batchjob',
            name='only_email_if_rows_above',
            field=models.IntegerField(default=0),
        ),
    ]