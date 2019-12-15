# Generated by Django 2.2.6 on 2019-10-12 09:58

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('main', '0005_batchjob_run_via'),
    ]

    operations = [
        migrations.AlterField(
            model_name='batchjob',
            name='created_by',
            field=models.ForeignKey(null=True, on_delete=django.db.models.deletion.SET_NULL, to=settings.AUTH_USER_MODEL),
        ),
        migrations.AlterField(
            model_name='batchjob',
            name='database',
            field=models.CharField(default='ak', max_length=255),
        ),
        migrations.AlterField(
            model_name='batchjob',
            name='form_data',
            field=models.TextField(default='{}'),
        ),
        migrations.AlterField(
            model_name='batchjob',
            name='run_via',
            field=models.CharField(choices=[('client-db', 'client-db'), ('api', 'api')], default='client-db', max_length=10),
        ),
        migrations.AlterField(
            model_name='recurringtask',
            name='period_unit',
            field=models.CharField(choices=[('minutes', 'minutes'), ('hours', 'hours'), ('days', 'days')], max_length=255),
        ),
    ]
