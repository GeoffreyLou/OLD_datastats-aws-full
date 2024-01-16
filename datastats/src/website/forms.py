from django import forms
from datastats_variables_xyz import *
from django.db.models import Min, Max
from .models import Job
from io import BytesIO
import pandas as pd
import boto3

# --------------------------
# ------ Requêtes SQL ------
# --------------------------

job_choices = list(Job.objects.using('data_jobs').values_list('job_search', flat=True).distinct())

dates = Job.objects.using('data_jobs').aggregate(min_date=Min('date_of_search'), max_date=Max('date_of_search'))
min_date = dates['min_date']
max_date = dates['max_date']

region_choices = list(Job.objects.using('data_jobs').exclude(region=None).values_list('region', flat=True).distinct())

# Nom du bucket S3 et du dossier dans le bucket
bucket_name = aws_bucket_data_files
folder_name = aws_storage_csv_folder_name
file_name = 'reg_dep_com.csv'

# Crée une session AWS avec les informations du rôle IAM de l'instance EC2
session = boto3.Session()

# Crée un client S3 à partir de la session
s3_client = session.client('s3')

# Récupération des régions et départements avec les cheflieux associés
response = s3_client.get_object(Bucket=bucket_name, Key=f'{folder_name}/{file_name}')
df_region = pd.read_csv(BytesIO(response['Body'].read()))


unique_region_cheflieu = sorted(list((df_region['region'] + ' | ' + df_region['region_cheflieu']).unique()))
unique_departement_cheflieu = sorted(list((df_region['departement'] + ' | ' + df_region['departement_cheflieu']).unique()))

# -------------------------
# ------ Formulaires ------
# -------------------------

# Page "Login"
# Formulaire pour le login / mdp
class SignupForm(forms.Form):
    username = forms.CharField(max_length=10, required=True, widget=forms.TextInput(attrs={'class': 'form-control'}))
    email = forms.EmailField(required=True, widget=forms.TextInput(attrs={'class': 'form-control'}))
    password = forms.CharField(min_length=6, required=True, widget=forms.PasswordInput(attrs={'class': 'form-control'}))

# Page "Dashboard par métier"
# Formulaire pour le choix des métiers sur le dashboard personnalisé    
class JobChoicesForm(forms.Form):
    job_list = forms.ChoiceField(
        choices=[("All", "Tout")] + [(choice, choice) for choice in job_choices], 
        widget=forms.Select(attrs={'class': 'form-select'}),
        label="Métier(s) :",
        initial=["All"],
    )
    
# Page "Dashboard par métier"
# Formulaire pour le choix des régions du dashboard personnalisé
class RegionChoicesForm(forms.Form):
    region_list = forms.ChoiceField(
        choices=[("All", "Tout")] + [(choice, choice) for choice in region_choices], 
        widget=forms.Select(attrs={'class': 'form-select'}),
        label="Région(s) :",
        initial=["All"]
    )

# Page "Administration"
# Formulaire pour vérifier la présence de technos    
class CheckTechnoForm(forms.Form):
    TechnoToCheck = forms.CharField(label="Techno à vérifier",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )

# Page "Administration"
 # Formulaire pour ajouter des technos   
class TechnosToAddForm(forms.Form):
    techno_propre = forms.CharField(label="Techno correctement écrite",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    variation_1 = forms.CharField(label="Variation 1",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    variation_2 = forms.CharField(label="Variation 2",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    variation_3 = forms.CharField(label="Variation 3",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    variation_4 = forms.CharField(label="Variation 4",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    variation_5 = forms.CharField(label="Variation 5",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    variation_6 = forms.CharField(label="Variation 6",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )

# Page "Administration"
# Formulaire pour supprimer des technos    
class TechnoDeleteForm(forms.Form):
    techno_to_delete = forms.CharField(label="Techno à supprimer",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )

# Page "Administration"
# Formulaire pour ajouter des régions 
class RegionToAddFormOnCsv(forms.Form):
    bad_city = forms.CharField(label="Ville à corriger",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    good_city = forms.CharField(label="Ville correctement renseignée",
        max_length=100,
        widget=forms.TextInput(attrs={'type': 'text', 'class': 'form-control'}),
        required=False
    )
    region_and_cheflieu = forms.ChoiceField(
        choices=[(choice, choice) for choice in unique_region_cheflieu],
        required=False,
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    departement_and_cheflieu = forms.ChoiceField(
        choices=[(choice, choice) for choice in unique_departement_cheflieu],
        required=False,
        widget=forms.Select(attrs={'class': 'form-control'})
    )

# Page "API"
# Formulaires pour l'API
class ApiSearchForm(forms.Form):
    date_start = forms.DateField(
        label='Date de début: ',
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),                             
        required=False
    )
    date_end = forms.DateField(
        label='Date de fin',                    
        widget=forms.DateInput(attrs={'type': 'date', 'class': 'form-control'}),                               
        required=False
    )  
    job_tupled = [("All", "Tout")] + [(job, job) for job in job_choices]
    job = forms.ChoiceField(
        label='Métier(s)', 
        choices=job_tupled,  #
        widget=forms.Select(attrs={'class': 'form-control'}),
        required=False
    )
    region_tupled = [("All", "Tout")] + [(region, region) for region in region_choices]
    region = forms.ChoiceField(
        label='Région', 
        choices=region_tupled,  
        widget=forms.Select(attrs={'class': 'form-control'}),
        required=False
    )

# Page "Contact"
# Formulaire pour contact
class ContactForm(forms.Form):
    votre_email = forms.EmailField(required=True, label="Votre email", widget=forms.TextInput(attrs={'type': 'email', 'class': 'form-control'}))
    votre_message = forms.CharField(required=True, label="Votre message", widget=forms.Textarea(attrs={'type': 'text', 'class': 'form-control'}))
