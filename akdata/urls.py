from django.conf.urls import include, url
from django.conf import settings
from django.contrib import admin
from django.contrib.flatpages import urls as flatpages_urls
admin.autodiscover()

from main import views
urlpatterns = [
    url(r'^$', views.home, name='home'),
    url(r'^batch-job/(?P<type>\w+)/$',
        views.batch_job, 
        name='batch_job'),
    url(r'^schedule/(?P<id>\d+)/$',
        views.schedule,
        name='schedule'),
    url(r'^logs/(?P<id>\d+)/(?P<type>\w+)/$',
        views.get_logs,
        name='get_logs'),

    url(r'^logs/(?P<id>\d+)/$',
        views.get_logs_index,
        name='get_logs_index'),

    url(r'^admin/', include(admin.site.urls)),
    url(r'^pages/', include(flatpages_urls)),
]

if settings.DEBUG:
    import debug_toolbar
    
    urlpatterns += [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ]
        
