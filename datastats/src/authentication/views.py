from django.conf import settings
from django.shortcuts import render, redirect
from django.contrib import messages
from django.contrib.auth import authenticate, login, logout, update_session_auth_hash
from django.contrib.auth.hashers import check_password
from django.contrib.auth.decorators import login_required, user_passes_test
from . import forms 
from .models import User

def is_admin(user):
    return user.groups.filter(name='Admins').exists()

admin_required = user_passes_test(is_admin, login_url='/index/')

# Page de connexion
def login_page(request):
    form = forms.LoginForm()
    if request.method == 'POST':
        form = forms.LoginForm(request.POST)
        if form.is_valid():
            user = authenticate(username=form.cleaned_data['username'], password=form.cleaned_data['password'])
            if user is not None:
                login(request, user)
                return redirect('index')
            else:
                messages.error(request, 'Identifiant ou mot de passe incorrect.')

    return render(request, 'login.html', {'form': form})


# Page invisible pour se déconnecter
def logout_user(request):
    logout(request)
    return redirect('index')


# Page pour créer son compte
def signup_page(request):
    form = forms.SignupForm()
    if request.method == 'POST':
        form = forms.SignupForm(request.POST)
        if form.is_valid():
            user = form.save()
            login(request, user)
            return redirect('index')

    return render(request, 'signup.html', {'form': form})


# Page pour accéder à son profil
@login_required(login_url='/index/')
def my_profile(request):

    # Récupération du nom d'utilisateur
    user = request.user

    if request.method == 'POST':

        password_confirmation = forms.PasswordConfirmationForm(request.POST)
        delete_account = forms.DeleteAccount(request.POST)
        change_username = forms.CustomUserChangeForm(request.POST)  
        change_email = forms.CustomEmailChangeForm(request.POST)
        change_password = forms.CustomPasswordChangeForm(user, request.POST)
        
        if "delete_account_submit" in request.POST:
            if password_confirmation.is_valid():
                if check_password(password_confirmation.cleaned_data['password'], request.user.password):
                    if delete_account.is_valid():
                        delete_bool = delete_account.cleaned_data['delete_account']
                        if delete_bool == True: 
                            request.user.delete()
                            logout(request)
                            # Indicateur de suppression de compte pour la page d'accueil
                            request.session['account_deleted'] = True
                            return redirect('index')
                        else: 
                            messages.success(request, "Vous n'avez pas coché la case, votre compte est conservé.")
                else:
                    messages.error(request, 'Le mot de passe ne correspond pas.')

        if "change_username_submit" in request.POST:
            if password_confirmation.is_valid():
                if check_password(password_confirmation.cleaned_data['password'], request.user.password):
                    if change_username.is_valid():
                        newusername = change_username.cleaned_data['username']
                        user.username = newusername
                        user.save()
                        messages.success(request, 'Votre nom d\'utilisateur a été mis à jour avec succès.')
                        return redirect('profil') 
                    else:
                        messages.error(request, "Le nom d'utilisateur renseigné est incorrect.")
                else: 
                    messages.error(request, 'Le mot de passe ne correspond pas.')

        if "change_email_submit" in request.POST:
            if password_confirmation.is_valid():
                if check_password(password_confirmation.cleaned_data['password'], request.user.password):
                    if change_email.is_valid():
                        newemail = change_email.cleaned_data['email']

                        # Vérification que l'email ne soit pas présent en base
                        if User.objects.filter(email=newemail).exists():
                            messages.error(request, 'Cet email ne peut pas être utilisé.')
                            return redirect('profil')
                        else:
                            user.email = newemail
                            user.save()
                            messages.success(request, 'Votre email a été mis à jour avec succès.')
                            return redirect('profil')
                    else:
                        messages.error(request, "L'email renseigné est incorrect.")
                else: 
                    messages.error(request, 'Le mot de passe ne correspond pas.')

        if "change_password_submit" in request.POST:
            if change_password.is_valid():
                user = change_password.save()
                update_session_auth_hash(request, user)
                messages.success(request, 'Votre mot de passe a été mis à jour avec succès.')
                return redirect('profil')
            else:
                messages.error(request, 'Les informations renseignées ne sont pas correctes.')
 
    else:
        change_email = forms.CustomEmailChangeForm()
        delete_account = forms.DeleteAccount()
        password_confirmation = forms.PasswordConfirmationForm()
        change_username = forms.CustomUserChangeForm()
        change_password = forms.CustomPasswordChangeForm(user)

    return render(request, 'profil.html', context={'current_username': request.user.username,
                                                    'current_email': request.user.email,
                                                    'password_confirmation': password_confirmation,
                                                    'delete_account': delete_account,
                                                    'change_username': change_username,
                                                    'change_email': change_email,
                                                    'change_password': change_password})