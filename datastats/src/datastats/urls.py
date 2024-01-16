from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView
from website.views import index, methodologie, about, dashboard, data, api, contact
from authentication.views import login_page, logout_user, signup_page, my_profile, admin_required
from django.contrib.auth.views import PasswordResetView, PasswordResetDoneView, PasswordResetConfirmView, PasswordResetCompleteView

handler404 = 'website.views.custom_404'

urlpatterns = [
    path('login/', include('django.contrib.auth.urls')),
    path('', RedirectView.as_view(url='index/', permanent=False)),
    path('index/', index, name="index"),
    path('login/', login_page, name="login"),
    path('adpanel/', admin_required(admin.site.urls)),
    path('about/', about, name="about"),
    path('dashboard/', dashboard, name="dashboard"),
    path('data/', data, name='data'),
    path('api/', api, name='api'),
    path('methodologie/', methodologie, name="methodologie"),
    path('logout/', logout_user, name='logout_user'),
    path('signup/', signup_page, name="signup"),
    path('profil/', my_profile, name='profil'),
    path('contact/', contact, name="contact")
]

password_reset_view = PasswordResetView.as_view(
    template_name='password_reset.html',
)

password_reset_done_view = PasswordResetDoneView.as_view(
    template_name='password_reset_done.html',
)

password_reset_confirm_view = PasswordResetConfirmView.as_view(
    template_name='password_reset_confirm.html',
)

password_reset_complete_view = PasswordResetCompleteView.as_view(
    template_name='password_reset_complete.html'
)

urlpatterns += [
    path('password_reset/', password_reset_view, name='password_reset'),
    path('password_reset_done/', password_reset_done_view, name='password_reset_done'),
    path('password_reset_confirm/<uidb64>/<token>/', password_reset_confirm_view, name='password_reset_confirm'),
    path('password_reset_complete/', password_reset_complete_view, name='password_reset_complete'),
]