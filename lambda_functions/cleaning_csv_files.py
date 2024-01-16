import numpy as np
import pandas as pd
import psycopg2
import ast
import json
import os
from datetime import date
import botocore
import boto3


# Client AWS
s3 = boto3.client(
    service_name='s3',
    region_name=os.environ['SECRET_REGION_1'])


# Récupération des variables
def get_secret():

    secret_name_1 = os.environ['SECRET_NAME_1'] 
    region_name = os.environ['SECRET_REGION_1']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response_1 = client.get_secret_value(
            SecretId=secret_name_1
        )
    except Exception as e:
        raise e


    secret_1 = get_secret_value_response_1['SecretString']
    secret_dict_1 = json.loads(secret_1)

    # Retrieve specific variables (e.g., username and password)
    database = secret_dict_1.get('dbInstanceIdentifier')
    user = secret_dict_1.get('username')
    password_db = secret_dict_1.get('password')
    host = secret_dict_1.get('host')
    port = secret_dict_1.get('port')


    return database, user, password_db, host, port

database, user, password_db, host, port = get_secret()


# Fonction Lambda
def lambda_handler(event, context):

    try:

        # Informations de connexion à la BDD : 
        conn = psycopg2.connect(
            database=database, 
            user=user, 
            password=password_db, 
            host=host, 
            port=port)

        s3_event = event['Records'][0]['s3']
        bucket_name = s3_event['bucket']['name']
        file_name = s3_event['object']['key']
    
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(response['Body'], sep=',')

        # On ne souhaite pas de valeurs vides dans technos, on ne garde donc pas ces annonces. 
        df = df.dropna(subset=['technos'])
 
        df['job_type'] = df['job_type'].apply(lambda x: x.strip().replace('Contrat', 'Temps plein'))
        df['job_search'] = df['job_search'].replace('analyste de données', 'data analyst')\
                                        .replace('consultant data', 'data analyst')\
                                        .replace('ingénieur data', 'data engineer')\
                                        .replace('ingénieur de données', 'data engineer')\
                                        .replace('data ingénieur', 'data engineer')\
                                        .replace('machine learning engineer', 'ml engineer')\
                                        .replace('architecte data', 'data architect')\
                                        .replace('manager data', 'data manager')\
                                        .replace('consultant bi', 'data analyst')\
                                        .replace('analyste bi', 'data analyst')\
                                        .replace('analysis engineer', 'analytics engineer')\
                                        .replace('ingénieur machine learning', 'ml engineer')
        
        # On s'assure que chaque élément est en str, et qu'il n'y ait pas d'espaces avant et après
        for col in df.columns[1:]:
            if col in ['scrap_number', 'lower_salary', 'upper_salary']:
                pass
            else:
                df[col] = df[col].astype(str).apply(lambda x: x.strip())

        # On retire les /n   
        df['description'] = df['description'].apply(lambda x: x.replace('\n', ''))

        # On créé la colonne avec le nom de ville + traitement des valeurs KO
        df['city'] = df['city_name'].apply(lambda x: x.split(',')[0]
                                        .strip()
                                        .replace('greater', '')
                                        .replace('metropolitan', '')
                                        .replace('area', '')
                                        .replace('region', '')
                                        .replace(', france', '')
                                        .replace(' et périphérie', '')
                                        .replace('ville de', '')
                                        .replace('île-de-france', 'paris')
                                        .replace('france', 'paris')
                                        .replace('la défense', 'puteaux')
                                        .strip())

        # Création d'une colonne d'index temporaire pour éviter les pertes lors des merge à venir
        index = pd.Index(range(len(df)))
        df['index'] = list(index)

        # Récupérer le fichier CSV spécifié
        region_file_path = "data-files/reg_dep_com.csv"
        region_file_response = s3.get_object(Bucket=bucket_name, Key=region_file_path)
        df_region = pd.read_csv(region_file_response['Body'], sep=',')

        # suppression des doublons sur les noms de ville (principalement des villages)
        df_region = df_region[~df_region['manual_city'].duplicated()]
        df = df.merge(df_region, on='city', how='left').drop(columns=['region_cheflieu', 'departement', 'departement_cheflieu', 'manual_city'])


        # --------------------------------
        # ----------- CONTEXTE -----------
        # --------------------------------

        # Il peut y avoir des valeurs vides dans la région
        # L'entreprise peut mettre juste un département ou une région dans le nom de ville. 
        # Il faut donc réaliser 3 vérifications : 
        # Si le nom de ville est juste un département -> On corrige (par chef lieu du département)
        # Si le nom de ville est juste une région -> On corrige (par chef lieu de la région)
        # Si le nom de ville ne match avec rien -> On ajoute la valeur dans une table pour traitement manuel

        # --------------------------------
        # --------- FIN CONTEXTE ---------
        # --------------------------------        

        # On vérifie d'abord s'il s'agit de département
        if len(df[ df['region'].isna() ]) > 0:

            # Séparation des dataframes selon les valeurs vides
            df_full_1 = df[ ~df['region'].isna() ]
            df_empty_1 = df[ df['region'].isna() ]

            # Si la ville permet de récupérer la région, il faudra refaire la colonne city
            # à partir de la colonne région nouvellement remplie

            # On fait le dataframe nécessaire pour la jointure et on supprime les doublons
            df_departement = df_region[['departement', 'region']].drop_duplicates()

            # Réalisation de la jointure
            df_empty_1 = df_empty_1.merge(df_departement, left_on='city', right_on='departement', how='left').drop(columns=['region_x']).rename(columns={'region_y': 'region'})

            # On va maintenant refaire la colonne city 
            # On a besoin que de la région et du chef lieu associé
            df_region_cheflieu = df_region[['region', 'region_cheflieu']].drop_duplicates()

            df_empty_1 = df_empty_1.merge(df_region_cheflieu, on='region', how='left').drop(columns='city').rename(columns={'region_cheflieu': 'city'})[['date_of_search', 'scrap_number', 'day_of_week', 'job_search', 'job_name', 'company_name', 'city_name', 'city', 'region', 'technos', 'description', 'lower_salary', 'upper_salary', 'job_type', 'sector']]

            # On doit maintenant concaténer les 2 dataframes et récupérer de nouveau ce qui est vide
            df = pd.concat([df_full_1, df_empty_1], ignore_index = True)

            # Vérification par région
            if len(df[ df['region'].isna() ]) > 0:

                # Séparation des dataframes selon les valeurs vides
                df_full_2 = df[ ~df['region'].isna() ]
                df_empty_2= df[ df['region'].isna() ]

                # Il faut maintenant vérifier que ce n'est pas la région qui est à la place de la ville
                df_region_2 = df_region[['region', 'region_cheflieu']].drop_duplicates().rename(columns={'region_cheflieu': 'city'})

                df = pd.concat([df_full_2, df_empty_2], ignore_index = True)

                # S'il reste des valeurs vides, alors c'est peut-être un nom de ville particulier
                # Comme La Défense ou Cergy-Pontoise, on rentre dans le traitement manuel
                if len(df[ df['region'].isna() ]) > 0:

                    # Séparation des dataframes selon les valeurs vides
                    df_full_3 = df[ ~df['region'].isna() ]
                    df_empty_3= df[ df['region'].isna() ]

                    # Il faut maintenant vérifier que ce n'est pas la région qui est à la place de la ville
                    # Il faut maintenant vérifier que ce n'est pas la région qui est à la place de la ville
                    df_manual_city = df_region[['manual_city', 'city', 'region']].drop_duplicates()
                    df_empty_3 = df_empty_3.drop(columns=['city', 'region']).merge(df_manual_city, left_on='city_name', right_on='manual_city', how='left')[['date_of_search', 'scrap_number', 'day_of_week', 'job_search', 'job_name', 'company_name', 'city_name', 'city', 'region', 'technos', 'description', 'lower_salary', 'upper_salary', 'job_type', 'sector']]
                    
                    df = pd.concat([df_full_3, df_empty_3], ignore_index = True)

        # On peut maintenant supprimer l'index temporaire
        df = df.drop(columns='index')[['date_of_search', 'scrap_number', 'day_of_week', 'job_search', 'job_name', 'company_name', 'city_name', 'city', 'region', 'technos', 'description', 'lower_salary', 'upper_salary', 'job_type', 'sector']]

        # S'il reste des valeurs NULL dans la colonne région, il faudra les traiter manuellement
        # Elles seront ajoutées à une table, un script python vérifiera plus tard et enverra un mail s'il y a des valeurs
        if len(df[ df['region'].isna() ]) > 0:
            
            # Définition des valeurs à ajouter
            # Set pour supprimer les doublons, transformés en liste s'il y a plusieurs valeurs
            values_to_add_manually = list(set(df['city_name'][ df['region'].isna() ]))

            state_error_city = 'to process'

            # Définition de la requête
            error_city_query = "INSERT INTO city_error (value, status) VALUES (%s, %s)"  
            # Liste de tuples pour la mise à jour de la table
            error_data_to_insert = [(city, state_error_city) for city in values_to_add_manually]
            # Exécute la requête avec les valeurs de la liste
            cur = conn.cursor()
            cur.executemany(error_city_query, error_data_to_insert)
            conn.commit()
            cur.close()

        # --------------------------------
        # ----------- CONTEXTE -----------
        # --------------------------------

        # Les valeurs de salaire arrivent en float dans le dataframe car il y a des NaN
        # Il faut pouvoir garder des NULL en sortie, soit None
        # Certains salaires annuels sont indiqué avec 2 valeurs numériques (60 pour 60K)
        # Il faut donc remplir les NaN par des 0, pour convertir en INT puis en STR
        # Seulement après, on peut traiter selon la longueur de la string, puis remettre des None

        # --------------------------------
        # --------- FIN CONTEXTE ---------
        # -------------------------------- 

        df['upper_salary'] = df['upper_salary'].astype(str).replace('nan', '0').apply(lambda x: str(int(x) * 1000) if len(x) == 2 else x).apply(lambda x: None if x == '0' else x)
        df['lower_salary'] = df['lower_salary'].astype(str).replace('nan', '0').apply(lambda x: str(int(x) * 1000) if len(x) == 2 else x).apply(lambda x: None if x == '0' else x)

        # On doit garder un seul nom de technos car elles sont indiquées sous différentes appellations. Ces étapes
        # Permettent de garder un nom unique de techno, puis d'écrire le nom sous sa forme correcte

        # Ajout des données à la table
        cursor = conn.cursor()
        cursor.execute("""
        SELECT values
        FROM lists
        WHERE list = 'clean_list'
                        """)

        # 1. Liste des technos sous forme de dictionnaire : {nom complet : [appellations]}
        techno_dict = ast.literal_eval(cursor.fetchall()[0][0].replace('\n', ''))
        cursor.close()

        # 2. Création de la fonction qui remplace les appellations par le nom complet
        def replace_technos(row):
            technos_list = row.split(', ')
            for index, techno in enumerate(technos_list):
                for key, value in techno_dict.items():
                    if techno in value:
                        technos_list[index] = key
            return (', ').join(technos_list)

        df['technos'] = df['technos'].apply(replace_technos)
       
        # 3. Cette fonction va créer des duplicates, il faut donc les retirer
        def remove_duplicates(row):
            technos_list = row.split(', ')
            unique_technos = list(set(technos_list))
            return ', '.join(unique_technos)

        df['technos'] = df['technos'].apply(remove_duplicates)
         
        # Remise en forme des colonnes 
        df = df[['date_of_search', 'scrap_number', 'day_of_week', 'job_search', 'job_name', 'company_name', 'city_name', 'city', 'region', 'technos', 'description', 'lower_salary', 'upper_salary', 'job_type', 'sector']]
       
        # Traitement en deux étapes pour éviter les erreurs s'il y a des valeurs NULL sur région
        df['region'] = df['region'].replace(np.nan, 0).astype(str)

        # On capitalize ou title certaines colonnes pour la mise en forme
        for col in df.columns:
            if col in ['technos', 'description', 'lower_salary', 'upper_salary', 'job_type', 'sector', 'scrap_number']:
                pass
            elif col in ['city', 'region', 'job_search']:
                df[col] = df[col].astype(str).apply(lambda x: x.strip().title()).replace('Nan', None).replace('0', None)
            else:
                df[col] = df[col].apply(lambda x: x.strip().capitalize())

        # Deuxième étape, on remet les NULL pour traitement ultérieur
        df['region'] = df['region'].replace('0', None)
        
        # Traitement particulier pour ML Engineer : 
        
        df['job_search'] = df['job_search'].apply(lambda x: x.replace('Ml Engineer', 'ML Engineer'))
            
        # Mise à jour SQL
        # Nom de la table à mettre à jour
        table_name = 'jobs'

        # Conversion du dataframe en liste de tuples
        data_tuples = [tuple(row) for row in df.itertuples(index=False)]

        # Récupération de la date pour supprimer les métiers doublons
        date_of_search = df['date_of_search'].max()        


        # --------------------------------
        # ----------- CONTEXTE -----------
        # --------------------------------

        # La requête ci-dessous est en deux étapes : 
        # 1. Insertion des données du jour
        # 2. Suppression des doublons, une entreprise a le droit à 
        #    - Une offre d'emploi par mois
        #    - Par ville
        #    - Par intitulé
        # On garde pour chaque mois la première occurrence via l'ID

        # --------------------------------
        # --------- FIN CONTEXTE ---------
        # -------------------------------- 


        # Création de la requête d'insertion et de suppression des doublons
        columns = ', '.join(f'"{col}"' for col in df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f'INSERT INTO {table_name} ({columns}) VALUES ({placeholders})'
        delete_query = f''' 
            DELETE FROM {table_name}
            WHERE DATE_TRUNC('month', date_of_search) = DATE_TRUNC('month', DATE('{date_of_search}'))
            AND id NOT IN (
                SELECT MIN(id)
                FROM {table_name}
                WHERE DATE_TRUNC('month', date_of_search) = DATE_TRUNC('month', DATE('{date_of_search}'))
                GROUP BY job_name, company_name, city_name, EXTRACT(MONTH FROM date_of_search))
        '''

        # Exécution de la requête pour chaque ligne du dataframe
        with conn.cursor() as cursor:
            cursor.executemany(insert_query, data_tuples)
            conn.commit()
            cursor.execute(delete_query)
            conn.commit()

        # Vérification du scrap en cours 
        if df['scrap_number'].astype(int).max() > 1:
            first_scrap = False
            scrap_number = df['scrap_number'].astype(int).max()
        else:
            first_scrap = True
            scrap_number = 1        
            
        query = f"SELECT * FROM {table_name} WHERE date_of_search = '{date_of_search}' AND scrap_number = {scrap_number}"
        
        cursor = conn.cursor()
        cursor.execute(query)
        data_du_jour_df = cursor.fetchall()

        # On transforme les données en DataFrame
        data_du_jour_df = pd.DataFrame(data_du_jour_df, columns=['id',
                                                                'date_of_search',
                                                                'scrap_number',
                                                                'day_of_week',
                                                                'job_search',
                                                                'job_name',
                                                                'company_name',
                                                                'city_name',
                                                                'city',
                                                                'region',
                                                                'technos',
                                                                'description',
                                                                'lower_salary', 
                                                                'upper_salary', 
                                                                'job_type', 
                                                                'sector'])
                                                                
        daily_job_scrap = len(data_du_jour_df)

        # Transformation des technos en liste de mots en retirant les espaces avant et après
        data_du_jour_df['technos'] = data_du_jour_df['technos'].str.split(',').apply(lambda x: [s.strip() for s in x])

        # Explode sur les technos afin de faire 1 ligne par technos
        data_du_jour_df = data_du_jour_df.explode('technos')

        # Création de la colonne occurrences afin de faire une somme avec groupby ensuite
        data_du_jour_df['occurrences'] = 1

        # Que voilà ici, on met as_index=False sinon le group_by ne remet pas les valeurs dans chaque ligne
        data_du_jour_df = data_du_jour_df.groupby(['date_of_search', 'day_of_week', 'region', 'job_search', 'technos'], as_index=False).agg({'occurrences':'sum'})
        data_du_jour_df = data_du_jour_df.rename(columns={"technos": "technologie"})


        # --------------------------------
        # ----------- CONTEXTE -----------
        # --------------------------------

        # Récupération des données précédentes si on a un scrap supérieur au numéro 1 
        # Permet de récupérer ce qui existe dans jobsoccurrences
        # Le concaténer à ce qu'on va ajouter, mais avant on supprime les données du jour pour éviter les doublons
        # Cela permet d'avoir en fin de journée la somme des technos du jour, par région et métier.
        # Pour l'ensemble des jobs scrapés de la journée, sans prise en compte de doublons. 

        # --------------------------------
        # --------- FIN CONTEXTE ---------
        # -------------------------------- 


        nouvelle_table = 'jobsoccurrences'

        if first_scrap == False:
            query = f"SELECT * FROM {nouvelle_table} WHERE date_of_search = '{date_of_search}'"
            cursor = conn.cursor()
            cursor.execute(query)
            df_to_add = pd.DataFrame(cursor.fetchall(), columns = ['date_of_search', 'day_of_week', 'region', 'job_search', 'technologie', 'occurrences'])
            data_du_jour_df = pd.concat([data_du_jour_df, df_to_add]).groupby(['date_of_search', 'day_of_week', 'region', 'job_search', 'technologie'], as_index=False).agg({'occurrences':'sum'})
            cursor.execute(f"""DELETE FROM {nouvelle_table} WHERE date_of_search = '{date_of_search}'""")
            conn.commit()
            cursor.close() 

        # On obtient les données à insérer du DataFrame
        data_to_insert = data_du_jour_df[['date_of_search', 'day_of_week', 'region', 'job_search', 'technologie', 'occurrences']].values

        # On ajoute les données à la table jobsoccurrences
        insert_query = f"INSERT INTO {nouvelle_table} (date_of_search, day_of_week, region, job_search, technologie, occurrences) VALUES (%s, %s, %s, %s, %s, %s)"       

        # On execute la requête d'insertion pour chaque ligne de données
        with conn.cursor() as cursor:
            for row in data_to_insert:
                cursor.execute(insert_query, row)
            conn.commit()


        # -----------------------------------------------
        # ------ Mise à jour de la table reporting ------
        # -----------------------------------------------
            
        # Rappel/définition des variables nécessaires :
    
        # date du scrap
        date_of_search

        # Scrap number
        scrap_number = int(scrap_number)

        # Occurrences
        occurrences = int(data_du_jour_df['occurrences'].sum())

        # Daily_jobs_scrap
        daily_job_scrap = int(daily_job_scrap)

        # Lambda status
        lambda_status = 'success'

        cursor = conn.cursor()
        cursor.execute("""
            UPDATE reporting
            SET 
                occurrences  = %s,
                daily_job_scrap = %s,
                lambda_status = %s
            WHERE reporting_date = %s
            AND scrap_number = %s;
            """, (occurrences, daily_job_scrap, lambda_status, date_of_search, scrap_number))
        conn.commit()

        # On met à jour l'insight "cloud" pour la page d'accueil du site
        cursor.execute("""
            WITH AWS AS (
            	SELECT COUNT(*) AS nombre_vals
            	FROM jobs
            	WHERE technos LIKE '%AWS, %'
            		OR technos LIKE '%, AWS'
            		OR technos = 'AWS'
            ),
            GCP AS (
            	SELECT COUNT(*) AS nombre_vals
            	FROM jobs
            	WHERE technos LIKE '%GCP, %'
            		OR technos LIKE '%, GCP'
            		OR technos = 'GCP'
            ),
            Azure AS (
            	SELECT COUNT(*) AS nombre_vals
            	FROM jobs
            	WHERE technos LIKE '%Azure, %'
            		OR technos LIKE '%, Azure'
            		OR technos = 'Azure'
            )
            UPDATE cloud_count
            SET cloud_count = (
            	SELECT 
            		(SELECT nombre_vals FROM AWS) +
            		(SELECT nombre_vals FROM GCP) +
            		(SELECT nombre_vals FROM Azure)
            );
        """)
        conn.commit()

        # Fermeture de la connexion
        conn.close()

    except Exception as e:
        print(f'test failed: {str(e)}')
    finally:
        # Fermeture de la connexion dans le bloc finally
        conn.close()