from django.conf.urls import url
from django.conf import settings
from django.contrib import admin
from django.contrib.auth.views import LoginView
from django.urls import include, path

if settings.ENFORCE_TWO_FACTOR:
    from django_otp.views import LoginView
    from two_factor.urls import urlpatterns as tf_urls
    from two_factor.gateways.twilio.urls import urlpatterns as tf_twilio_urls

    urlpatterns = [
        path('account/two_factor/login/', LoginView.as_view(),
             name='two_factor_login_step2'),
        url(r'', include(tf_urls)),
        url(r'', include(tf_twilio_urls)),
    ]
else:
    urlpatterns = [
        url(r'^account/login/$', LoginView.as_view(template_name='admin/login.html')),
    ]
    
admin.autodiscover()

from main import views

urlpatterns = urlpatterns + [
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

    url(r'^admin/', admin.site.urls),
    url(r'^pages/', include('django.contrib.flatpages.urls')),
]

if settings.DEBUG:
    import debug_toolbar
    
    urlpatterns += [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ]
        
