from django.conf.urls.defaults import patterns, include, url
from django.conf import settings
from django.contrib import admin
admin.autodiscover()

urlpatterns = patterns(
    '',
    url(r'^$', 'main.views.home', name='home'),
    url(r'^batch-job/(?P<type>\w+)/$',
        'main.views.batch_job', 
        name='batch_job'),
    url(r'^schedule/(?P<id>\d+)/$',
        'main.views.schedule',
        name='schedule'),
    url(r'^logs/(?P<id>\d+)/(?P<type>\w+)/$',
        'main.views.get_logs',
        name='get_logs'),

    url(r'^admin/', include(admin.site.urls)),
    )

for app_name in settings.TASKMAN_PLUGIN_PACKAGES:
    urlpatterns += patterns(
        '',
        url(r'', include('%s.urls' % app_name)),
        )

urlpatterns += patterns(
    'django.contrib.auth.views',
    (r'^accounts/login/$', 'login'),
    (r'^accounts/logout/$', 'logout'),
    )
urlpatterns += patterns('',
    (r'^pages/', include('django.contrib.flatpages.urls')),
)
