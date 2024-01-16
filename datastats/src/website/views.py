from django.contrib.auth.decorators import login_required
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.conf import settings
from django.core.mail import send_mail
from django.db.models import Count, Avg, Case, When, Value, Q, Max
from django.db.models.functions import TruncDay
from django.utils import timezone
from datastats_variables_xyz import *
from .models import Job, Cloud
from datetime import datetime
from datetime import timedelta
import warnings
import json
import pandas as pd

# Retirer les warnings 
warnings.simplefilter(action='ignore', category=Warning)


# Formulaires
from .forms import JobChoicesForm, RegionChoicesForm, \
    CheckTechnoForm, TechnosToAddForm, TechnoDeleteForm, \
    ApiSearchForm, RegionToAddFormOnCsv, ContactForm


# Fonction pour définir si l'utilisateur connecté est un superuser
def est_superutilisateur(user):
    return user.is_authenticated and user.is_superuser


def custom_404(request, exception):
    return render(request, '404.html', status='404')


def index(request):
      
    # Vérifie la présence de l'indicateur de suppression de compte dans la session
    account_deleted = request.session.pop('account_deleted', False)

    # Définition d'une fenêtre temporelle de un an pour les requêtes
    one_year_ago = timezone.now() - timedelta(days=365)

    # Définir one_month_ago comme le premier jour du mois précédent
    now = timezone.now()
    first_day_this_month = now.replace(day=1)
    one_month_ago = first_day_this_month - timezone.timedelta(days=1)
    one_month_ago = one_month_ago.replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Premier jour du mois en cours
    first_day_current_month = timezone.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

    # Dernier jour du mois précédent
    last_day_previous_month = first_day_current_month - timedelta(days=1)

    # Date correspondant à "il y a un an à partir du dernier jour du mois précédent"
    one_year_ago_from_last_month = last_day_previous_month - timedelta(days=365)

    # Nombre total de jobs de la dernière année depuis le dernier jour du mois précédent
    html_total_jobs = Job.objects.using('data_jobs').filter(
        region__isnull=False, 
        date_of_search__gte=one_year_ago_from_last_month,
        date_of_search__lte=last_day_previous_month
    ).count()

    # Nombre de jobs avec AWS, Azure ou GCP pour la même période
    cloud_total_jobs = int(Cloud.objects.using('data_jobs').aggregate(Max('cloud_count'))['cloud_count__max'])
    percentage_cloud_jobs = round(cloud_total_jobs * 100 / html_total_jobs)

    # Moyenne quotidienne de nouveaux jobs
    avg_daily_jobs = Job.objects.using('data_jobs')\
        .filter(date_of_search__gte=one_year_ago)\
        .annotate(day=TruncDay('date_of_search'))\
        .values('day')\
        .annotate(count=Count('id'))\
        .aggregate(avg_daily=Avg('count'))['avg_daily']
    
    avg_daily_jobs = round(avg_daily_jobs)

    # Job avec le salaire maximum
    max_salary_job = Job.objects.using('data_jobs').filter(
        upper_salary__isnull=False,
        date_of_search__range=[one_month_ago, first_day_this_month]
    ).order_by('-upper_salary').first()

    if max_salary_job:
        max_salary_job_name = max_salary_job.job_name
        max_salary_company_name = max_salary_job.company_name
        max_salary_min_salary = max_salary_job.lower_salary
        max_salary_max_salary = max_salary_job.upper_salary

    max_salary_min_salary = int(max_salary_min_salary)
    max_salary_max_salary = int(max_salary_max_salary)

    # Job avec le salaire maximum
    min_salary_job = Job.objects.using('data_jobs').filter(
        upper_salary__isnull=False,
        date_of_search__range=[one_month_ago, first_day_this_month]
    ).order_by('upper_salary').first()

    if min_salary_job:
        min_salary_job_name = min_salary_job.job_name
        min_salary_company_name = min_salary_job.company_name
        min_salary_min_salary = min_salary_job.lower_salary
        min_salary_max_salary = min_salary_job.upper_salary

    min_salary_min_salary = int(min_salary_min_salary)
    min_salary_max_salary = int(min_salary_max_salary)


    # Type de contrat le plus courant

    # Compter le nombre total d'offres d'emploi pour le mois précédent
    total_jobs_last_month = Job.objects.using('data_jobs').filter(
        date_of_search__range=[one_month_ago, timezone.now()]
    ).count()

    contract_data = Job.objects.using('data_jobs').filter(
        date_of_search__range=[one_month_ago, timezone.now()],
        job_type__isnull=False
    ).annotate(
        normalized_job_type=Case(
            When(job_type='Autre', then=Value('Temps plein')),
            When(job_type='Travail temporaire', then=Value('Temps partiel')),
            default='job_type'
        )
    ).values('normalized_job_type').annotate(
        count=Count('id')
    ).order_by('-count')

    if contract_data:
        contract_type_type = contract_data[0]['normalized_job_type']
        contract_type_percentage = round(contract_data[0]['count'] * 100 / total_jobs_last_month, 1)

    return render(
        request, 
        'index.html', 
        context={
            'html_total_jobs': html_total_jobs,
            'cloud_total_jobs': cloud_total_jobs,
            'percentage_cloud_jobs': percentage_cloud_jobs,
            'avg_daily_jobs': avg_daily_jobs,
            'max_salary_job_name': max_salary_job_name,
            'max_salary_company_name': max_salary_company_name,
            'max_salary_min_salary': max_salary_min_salary,
            'max_salary_max_salary': max_salary_max_salary,
            'min_salary_job_name': min_salary_job_name,
            'min_salary_company_name': min_salary_company_name,
            'min_salary_min_salary': min_salary_min_salary,
            'min_salary_max_salary': min_salary_max_salary,  
            'contract_type_type': contract_type_type,
            'contract_type_percentage': contract_type_percentage,      
            'account_deleted': account_deleted
    })
    

def methodologie(request):
    return render(request, 'methodologie.html')


def about(request):
    return render(request, 'about.html')


@login_required(login_url='/index/')
def dashboard(request):

    post_bool = False
      
    if request.method == "POST":

        # Booléen pour le front (le formulaire a été renseigné)
        post_bool = True

        # Initialisation des formulaires
        job_choices_form = JobChoicesForm(request.POST)
        region_choices_form = RegionChoicesForm(request.POST) 
        
        # Cas des formulaire de filtre correctement renseignés
        if job_choices_form.is_valid() and region_choices_form.is_valid():

            # Condition pour sortir des formulaires tout est sélectionné dans les 2 filtres
            # Dans ce cas on affiche les images mensuelles de base
            if "All" in region_choices_form.cleaned_data.get('region_list', []) and "All" in job_choices_form.cleaned_data.get('job_list', []):

                job_choices_form = JobChoicesForm()
                region_choices_form = RegionChoicesForm() 

                post_bool = False

                #--------------------------------
                #------ Jobs count insight ------
                #--------------------------------

                # Premier jour du mois en cours
                first_day_current_month = timezone.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

                # Dernier jour du mois précédent
                last_day_previous_month = first_day_current_month - timedelta(days=1)

                # Date correspondant à "il y a un an à partir du dernier jour du mois précédent"
                one_year_ago_from_last_month = last_day_previous_month - timedelta(days=365)

                # Nombre total de jobs de la dernière année depuis le dernier jour du mois précédent
                jobs_count_insight = Job.objects.using('data_jobs').filter(
                    region__isnull=False, 
                    date_of_search__gte=one_year_ago_from_last_month,
                    date_of_search__lte=last_day_previous_month
                ).count()
                
                # Prise en compte du manque de données en attendant le mois de janvier (2 mois mini requis)
                if datetime.now() >= datetime.strptime('2024-02-01', '%Y-%m-%d'):
                    too_soon_salary_bool = False 
                else: 
                    too_soon_salary_bool = True

                return render(
                    request, 
                    'dashboard.html', 
                    {
                        'job_choices_form': job_choices_form,
                        'region_choices_form': region_choices_form,
                        "post_bool": post_bool,
                        'jobs_count_insight': jobs_count_insight,
                        'too_soon_salary_bool': too_soon_salary_bool
                    })    
            
            # Ici, tout n'est pas sélectionné sur les 2 graphiques
            else:

                # Vérification du filtre region

                if "All" in region_choices_form.cleaned_data.get('region_list', []):
                    one_region_salary_bool = False
                else: 
                    one_region_salary_bool = True
                
                job_variable_for_file = job_choices_form.cleaned_data.get('job_list').lower()\
                    .replace('ô', 'o')\
                    .replace('é', 'e')\
                    .replace("'", '_')\
                    .replace(' ', '_')\
                    .replace('î', 'i')\
                    .replace('-', '_')
                
                job_for_query = job_choices_form.cleaned_data.get('job_list')
                
                region_for_file = region_choices_form.cleaned_data.get('region_list').lower()\
                    .replace('ô', 'o')\
                    .replace('é', 'e')\
                    .replace("'", '_')\
                    .replace(' ', '_')\
                    .replace('î', 'i')\
                    .replace('-', '_')
                
                region_for_query = region_choices_form.cleaned_data.get('region_list')
                
                # Définition des variables à afficher en front pour chaque élément de graphique : 
                dashboard_linechart_jobs_evolution = f'static/charts/dashboard_linechart_jobs_evolution_{job_variable_for_file}_{region_for_file}.png'
                dashboard_top20_plus_chart = f'static/charts/dashboard_top20_plus_chart_{job_variable_for_file}_{region_for_file}.png'
                dashboard_top20_plus_table = f'static/charts/dashboard_top20_plus_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_top20_moins_chart = f'static/charts/dashboard_top20_moins_chart_{job_variable_for_file}_{region_for_file}.png'
                dashboard_top20_moins_table = f'static/charts/dashboard_top20_moins_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_med_salary_all_regions_table = f'static/charts/dashboard_med_salary_all_regions_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_med_salary_one_region_table = f'static/charts/dashboard_med_salary_one_region_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_avg_salary_one_region_table = f'static/charts/dashboard_avg_salary_one_region_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_median_salary_global_evolution = f'static/charts/dashboard_median_salary_global_evolution_{job_variable_for_file}_{region_for_file}.png'
                dashboard_cloud_providers_evolution = f'static/charts/dashboard_cloud_providers_evolution_{job_variable_for_file}_{region_for_file}.png'
                dashboard_donut_providers_cloud = f'static/charts/dashboard_donut_providers_cloud_{job_variable_for_file}_{region_for_file}.png'
                dashboard_techno_3m_table = f'static/charts/dashboard_techno_3m_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_ville_3m_table = f'static/charts/dashboard_ville_3m_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_entreprise_3m_table = f'static/charts/dashboard_entreprise_3m_table_{job_variable_for_file}_{region_for_file}.png'
                dashboard_techno_table_12m = f'static/charts/dashboard_techno_table_12m_{job_variable_for_file}_{region_for_file}.png'
                dashboard_ville_table_12m = f'static/charts/dashboard_ville_table_12m_{job_variable_for_file}_{region_for_file}.png'
                dashboard_entreprise_table_12m = f'static/charts/dashboard_entreprise_table_12m_{job_variable_for_file}_{region_for_file}.png'

                #--------------------------------
                #------ Jobs count insight ------
                #--------------------------------

                # Prise en compte des choix particuliers si "all" est présent dans un des deux filtres

                # Calcul des dates pour la fenêtre de 12 mois
                now = timezone.now()
                one_year_ago = now - timedelta(days=365)
                first_day_current_month = now.replace(day=1)
                last_day_previous_month = first_day_current_month - timedelta(days=1)

                # Filtres initiaux basés sur la plage de dates
                filters = Q(date_of_search__gte=one_year_ago) & Q(date_of_search__lt=last_day_previous_month)

                # Gestion du filtre région
                if region_choices_form.cleaned_data.get('region_list') != 'All':
                    filters &= Q(region=region_for_query)

                # Gestion du filtre job
                if job_choices_form.cleaned_data.get('job_list') != 'All':
                    filters &= Q(job_search=job_for_query)

                # Compter les jobs en utilisant les filtres définis
                jobs_count_insight = Job.objects.using('data_jobs').filter(filters).count()


                #-----------------------------------------
                #------ Evolution du salaire médian ------
                #-----------------------------------------

                # Prise en compte du manque de données en attendant le mois de janvier (2 mois mini requis)
                if datetime.now() >= datetime.strptime('2024-02-01', '%Y-%m-%d'):
                    too_soon_salary_bool = False 
                else: 
                    too_soon_salary_bool = True


        return render(
            request, 
            'dashboard.html', 
            {
                'post_bool': post_bool,
                'too_soon_salary_bool': too_soon_salary_bool,
                'one_region_salary_bool': one_region_salary_bool,
                "job_choices_form": job_choices_form,
                'region_choices_form': region_choices_form,
                'jobs_count_insight': jobs_count_insight,
                'dashboard_linechart_jobs_evolution' : dashboard_linechart_jobs_evolution,
                'dashboard_top20_plus_chart' : dashboard_top20_plus_chart,
                'dashboard_top20_plus_table' : dashboard_top20_plus_table,
                'dashboard_top20_moins_chart' : dashboard_top20_moins_chart,
                'dashboard_top20_moins_table' : dashboard_top20_moins_table,
                'dashboard_med_salary_all_regions_table' : dashboard_med_salary_all_regions_table,
                'dashboard_med_salary_one_region_table' : dashboard_med_salary_one_region_table,
                'dashboard_avg_salary_one_region_table' : dashboard_avg_salary_one_region_table,
                'dashboard_median_salary_global_evolution' : dashboard_median_salary_global_evolution,
                'dashboard_cloud_providers_evolution' : dashboard_cloud_providers_evolution,
                'dashboard_donut_providers_cloud' : dashboard_donut_providers_cloud,
                'dashboard_techno_3m_table' : dashboard_techno_3m_table,
                'dashboard_ville_3m_table' : dashboard_ville_3m_table,
                'dashboard_entreprise_3m_table' : dashboard_entreprise_3m_table,
                'dashboard_techno_table_12m' : dashboard_techno_table_12m,
                'dashboard_ville_table_12m' : dashboard_ville_table_12m,
                'dashboard_entreprise_table_12m' : dashboard_entreprise_table_12m,
            })
            
    else:

        post_bool = False

        # Initialisation des formulaires par défaut (aucun choix)
        job_choices_form = JobChoicesForm()
        region_choices_form = RegionChoicesForm()            
        
        #--------------------------------
        #------ Jobs count insight ------
        #--------------------------------

        # Premier jour du mois en cours
        first_day_current_month = timezone.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)

        # Dernier jour du mois précédent
        last_day_previous_month = first_day_current_month - timedelta(days=1)

        # Date correspondant à "il y a un an à partir du dernier jour du mois précédent"
        one_year_ago_from_last_month = last_day_previous_month - timedelta(days=365)

        # Nombre total de jobs de la dernière année depuis le dernier jour du mois précédent
        jobs_count_insight = Job.objects.using('data_jobs').filter(
            region__isnull=False, 
            date_of_search__gte=one_year_ago_from_last_month,
            date_of_search__lte=last_day_previous_month
        ).count()
        
        # Prise en compte du manque de données en attendant le mois de janvier (2 mois mini requis)
        if datetime.now() >= datetime.strptime('2024-02-01', '%Y-%m-%d'):
            too_soon_salary_bool = False 
        else: 
            too_soon_salary_bool = True

        return render(
            request, 
            'dashboard.html', 
            {
                "post_bool": post_bool,
                "job_choices_form": job_choices_form,
                'region_choices_form': region_choices_form,
                'too_soon_salary_bool': too_soon_salary_bool,
                'jobs_count_insight': jobs_count_insight
            })    


@login_required(login_url='/index/')
def data(request):
    
    empty_search_bool = False
    date_error_bool = False
    giga_error_bool = False
    
    if request.method == 'POST':

        apiform = ApiSearchForm(request.POST)
        
        if apiform.is_valid():
        
            date_min = apiform.cleaned_data['date_start']
            date_max = apiform.cleaned_data['date_end']
            job = apiform.cleaned_data['job']
            region = apiform.cleaned_data['region']
            url_job = apiform.cleaned_data['job'].replace(' ', '+')
            url_region = apiform.cleaned_data['region'].replace(' ', '+')

            # Gestion de tous les cas où l'utilisateur pourrait entrer une date incorrecte

            min_date = Job.objects.using('data_jobs').earliest('date_of_search').date_of_search
            max_date = Job.objects.using('data_jobs').latest('date_of_search').date_of_search

            if date_min is None:
                date_min = min_date

            if date_max is None:
                date_max = max_date

            if date_min < min_date or date_min > max_date:
                date_error_bool = True
                date_min = min_date
                
            if date_max > max_date or date_max < min_date:
                date_error_bool = True
                date_max = max_date
                
            if date_min > date_max:
                final_date_min = date_max
            else:
                final_date_min = date_min
                
            if date_max < date_min:
                final_date_max = date_min
            else:
                final_date_max = date_max

            # Récupération de la première valeur pour affichage

            job_list = [job] if job else []
            region_list = [region] if region else []

            queryset = Job.objects.using('data_jobs').filter(
                date_of_search__range=[final_date_min, final_date_max]
            )       

            if job and job != 'All':
                queryset = queryset.filter(job_search__in=job_list)

            if region and region != 'All':
                queryset = queryset.filter(region__in=region_list)

            queryset = queryset.order_by('id')

            # Conversion en DataFrame Pandas
            apidf = pd.DataFrame.from_records(queryset.values(
                'date_of_search', 'job_name', 'region', 'city', 'company_name', 'technos', 'description'
            ))

            number_of_results = len(apidf)

            apidf['date_of_search'] = apidf['date_of_search'].astype(str)

            try:
                first_value = json.dumps(apidf.iloc[0].to_dict(), indent=4, ensure_ascii=False)
            except IndexError:
                first_value = 'empty'
                empty_search_bool = True

            url = f"https://datastats.fr/api/?date_start={final_date_min}&date_end={final_date_max}&job={url_job}&region={url_region}"

            real_date_min = min_date
            real_date_max = max_date
            
            return render(
                request, 
                'data.html', 
                {
                    'first_value': first_value,
                    'apiform': apiform,
                    'url': url,
                    'empty_search_bool': empty_search_bool,
                    'date_error_bool': date_error_bool,
                    'final_date_min': final_date_min,
                    'final_date_max': final_date_max,
                    'number_of_results': number_of_results,
                    'real_date_min': real_date_min,
                    'real_date_max': real_date_max,
                    'giga_error_bool': giga_error_bool
                })
            
        else:
            giga_error_bool = True
            
            min_date = Job.objects.using('data_jobs').earliest('date_of_search').date_of_search
            max_date = Job.objects.using('data_jobs').latest('date_of_search').date_of_search

            apiform = ApiSearchForm(initial={
                'date_start': min_date.strftime('%Y-%m-%d'),
                'date_end': max_date.strftime('%Y-%m-%d')
            })
            
            final_date_min = max_date
            final_date_max = max_date
            
            try:
                first_record = Job.objects.using('data_jobs').order_by('id').values(
                    'date_of_search', 'job_search', 'region', 'city', 'company_name', 'technos', 'description'
                ).first()

                if first_record:
                    first_record['date_of_search'] = first_record['date_of_search'].strftime('%Y-%m-%d')
                    first_value = json.dumps(first_record, indent=4, ensure_ascii=False)
                else:
                    first_value = 'empty'
                    empty_search_bool = True

            except Job.DoesNotExist:
                first_value = 'empty'
                empty_search_bool = True
            
            number_of_results = Job.objects.using('data_jobs').count()
            
            url = f"https://datastats.fr/api/?date_start={max_date}&date_end={max_date}&job=All&region=All"
            
            real_date_min = max_date
            real_date_max = max_date
            
            return render(
                request, 
                'data.html', 
                {
                    'first_value': first_value,
                    'apiform': apiform,
                    'url': url,
                    'number_of_results': number_of_results,
                    'empty_search_bool': empty_search_bool,
                    'real_date_min': real_date_min,
                    'real_date_max': real_date_max,
                    'giga_error_bool': giga_error_bool,
                    'final_date_min': final_date_min,
                    'final_date_max': final_date_max
                })
                
    else:
        
        min_date = Job.objects.using('data_jobs').earliest('date_of_search').date_of_search
        max_date = Job.objects.using('data_jobs').latest('date_of_search').date_of_search

        apiform = ApiSearchForm(initial={
            'date_start': min_date.strftime('%Y-%m-%d'),
            'date_end': max_date.strftime('%Y-%m-%d')
        })
                
        try:
            first_record = Job.objects.using('data_jobs').order_by('id').values(
                'date_of_search', 'job_search', 'region', 'city', 'company_name', 'technos', 'description'
            ).first()

            if first_record:
                first_record['date_of_search'] = first_record['date_of_search'].strftime('%Y-%m-%d')
                first_value = json.dumps(first_record, indent=4, ensure_ascii=False)
            else:
                first_value = 'empty'
                empty_search_bool = True

        except Job.DoesNotExist:
            first_value = 'empty'
            empty_search_bool = True
        
        number_of_results = Job.objects.using('data_jobs').count()
        
        url = f"https://datastats.fr/api/?date_start={min_date}&date_end={max_date}&job=All&region=All"
        
        real_date_min = min_date
        real_date_max = max_date
        

        return render(
            request, 
            'data.html', 
            {
                'first_value': first_value,
                'apiform': apiform,
                'url': url,
                'number_of_results': number_of_results,
                'empty_search_bool': empty_search_bool,
                'real_date_min': real_date_min,
                'real_date_max': real_date_max,
                'giga_error_bool': giga_error_bool
            })


def api(request):

    date_start = request.GET.get('date_start')
    date_end = request.GET.get('date_end')
    job = request.GET.get('job')
    region = request.GET.get('region')

    # Récupération de la liste des métiers
    job_choices = list(Job.objects.using('data_jobs').values_list('job_search', flat=True).distinct())

    # Récupération de la liste des régions
    region_choices = list(Job.objects.using('data_jobs').values_list('region', flat=True).distinct())


    full_jobs_list = job_choices
    full_jobs_list.append('All')
    full_regions_list = region_choices
    full_regions_list.append('All')

    # Validation des dates
    try:
        if date_start:
            date_start = datetime.strptime(date_start, '%Y-%m-%d').date()
        if date_end:
            date_end = datetime.strptime(date_end, '%Y-%m-%d').date()
    except ValueError:
        return JsonResponse({'message': 'Format de date invalide'}, status=400)
     
    # Validation de 'job'
    if job not in full_jobs_list:
        return JsonResponse({'message': f'Le métier doit être parmi les éléments suivants {full_jobs_list}. Attention à bien respecter la casse.'}, status=400)

    # Validation de 'region'
    if region not in full_regions_list:
        return JsonResponse({'message': f'La région doit être parmi les éléments suivants {full_regions_list} Attention à bien respecter la casse.'}, status=400)

    # Créer une liste de valeurs pour utiliser dans la clause IN
    job_list = [job] if job else []
    region_list = [region] if region else []

    # Utilisation de Q objects pour construire la requête de manière dynamique
    filters = Q()
    if date_start:
        filters &= Q(date_of_search__gte=date_start)
    if date_end:
        filters &= Q(date_of_search__lte=date_end)
    if job and job != 'All':
        filters &= Q(job_search__in=job_list)
    if region and region != 'All':
        filters &= Q(region__in=region_list)

    results = Job.objects.using('data_jobs').filter(filters).order_by('id').values(
        'date_of_search',
        'job_name',
        'region',
        'city',
        'company_name',
        'technos',
        'description'
    )

    results = list(results)
    
    response_data = {
        'message': 'Succès',
        'data': results,
    }
    return JsonResponse(response_data, json_dumps_params={'indent':4, 'ensure_ascii':False})


def contact(request):
    if request.method == 'POST':
        form = ContactForm(request.POST)
        if form.is_valid():
            votre_email = form.cleaned_data['votre_email']
            votre_message = form.cleaned_data['votre_message']

            # Envoie de l'e-mail
            send_mail(
                'Nouveau message de contact',
                votre_message,
                votre_email,
                [settings.EMAIL_SEND_USER],
                fail_silently=False,
            )

            request.session['email_sent'] = True

            return redirect('contact')  
    else:
        # Vérifie la présence de l'indicateur de suppression de compte dans la session
        email_sent = request.session.pop('email_sent', False)
        form = ContactForm()

    return render(
        request, 
        'contact.html', 
        {
            'form': form,
            'email_sent': email_sent
        })