from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import UserCreationForm, UserChangeForm, PasswordChangeForm


class LoginForm(forms.Form):
    username = forms.CharField(max_length=63, 
                            label="Nom d'utilisateur",
                            widget=forms.TextInput(attrs={'class': 'form-control'}))
    
    password = forms.CharField(max_length=63, 
                            widget=forms.PasswordInput(attrs={'class': 'form-control'}),
                            label="Mot de passe")
    
class SignupForm(UserCreationForm):
    class Meta(UserCreationForm.Meta):
        model = get_user_model()
        fields = ['username', 'email']

    email = forms.EmailField(required=True)

class UserProfileForm(UserChangeForm):
    class Meta:
        model = get_user_model()
        fields = ['username', 'email']

    password = forms.CharField(label='Mot de passe', widget=forms.PasswordInput, required=False)
    delete_account = forms.BooleanField(label='Supprimer le compte', required=False)


class PasswordConfirmationForm(forms.Form):
    password = forms.CharField(max_length=63, 
                            label='Mot de passe', 
                            widget=forms.PasswordInput(attrs={'class': 'form-control'}),
                            required=True)

class DeleteAccount(forms.Form):
    delete_account = forms.BooleanField(label='Supprimer le compte', required=False)

class PasswordConfirmationForm(forms.Form):
    password = forms.CharField(max_length=63, 
                            label='Mot de passe', 
                            widget=forms.PasswordInput(attrs={'class': 'form-control'}),
                            required=True)

class CustomUserChangeForm(UserChangeForm):
    class Meta:
        model = get_user_model()
        fields = ['username']

class CustomEmailChangeForm(UserChangeForm):
    class Meta:
        model = get_user_model()
        fields = ['email']

class CustomPasswordChangeForm(PasswordChangeForm):
    class Meta:
        model = get_user_model()