from psycopg2 import sql
from botocore.exceptions import ClientError
import psycopg2
import pandas as pd
import re
import ast
import os
import time
import json
import boto3
import os

# Récupération des variables d'environnement
def get_secret():

    secret_name_1 = os.environ['SECRET_NAME_1']
    region_name = os.environ['SECRET_REGION_NAME']

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
    except ClientError as e:
        raise e
    

    secret_1 = get_secret_value_response_1['SecretString']
    secret_dict_1 = json.loads(secret_1)

    # Retrieve specific variables (e.g., username and password)
    database = secret_dict_1.get('dbInstanceIdentifier')
    user = secret_dict_1.get('username')
    password = secret_dict_1.get('password')
    host = secret_dict_1.get('host')
    port = secret_dict_1.get('port')

    return database, user, password, host, port

database, user, password, host, port = get_secret()


# Création de la connexion psycopg2 pour récupérer les listes
conn = psycopg2.connect(database=database, 
                        user=user, 
                        password=password, 
                        host=host, 
                        port=port)
cursor = conn.cursor() 

maintenance_bool = False

# Vérification de l'état de la base de données
for i in range(20):
        cursor.execute("SELECT * FROM maintenance")
        database_state = cursor.fetchall()[0][0]
        conn.commit()

        if database_state == 'available':
            maintenance_bool = False
            break
        else:
            time.sleep(10)
            continue

# La base est indisponible, on ne lance pas le script
if maintenance_bool == True:
    conn.close()

# La base de données est disponible, on lance donc le script. 
else: 

    # --------------------------------------------
    # ------ Récupération des listes en BDD ------
    # --------------------------------------------

    # Récupération de la liste des technos plus de 4 caractères
    cursor = conn.cursor()
    cursor.execute("""
    SELECT values
    FROM lists
    WHERE list = 'techno_list'
                    """)  
    techno_list = [i.replace('"', '').strip() for i in cursor.fetchall()[0][0].replace("\n","").replace("'", "").strip().split(',')]

    # Récupération de la liste des technos de 4 caractères et moins
    cursor.execute("""
    SELECT values
    FROM lists
    WHERE list = 'mini_list'
                    """)  
    mini_list = [i.replace('"', '').strip() for i in cursor.fetchall()[0][0].replace("\n","").replace("'", "").strip().split(',')]

    # Récupération de la liste des technos correctement écrites
    cursor.execute("""
    SELECT values
    FROM lists
    WHERE list = 'clean_list'
                    """)
    techno_dict = ast.literal_eval(cursor.fetchall()[0][0].replace('\n', ''))


    # --------------------------------------------------
    # ------ Déclaration de l'état de maintenance ------
    # --------------------------------------------------

    cursor.execute("""
        UPDATE maintenance 
        SET status = 'maintenance';
    """)
    conn.commit()


    # ------------------------------------
    # ------ Création des fonctions ------
    # ------------------------------------

    # Cette fonction permet de récupérer les technologies dans chaque description
    def technology_finder(description):
        # Récupération des technos dans la description
        technos = [i for i in techno_list if i in description.replace(' ', '')]

        # Les mots de 4 lettres et moins sont récupérés via du Regex sur la description
        for mot in mini_list:
            # re.escape permet de ne pas prendre en compte les caractères spéciaux, sinon erreur de code
            mot_escaped = re.escape(mot)
            if re.findall(r'(?<!\S){}(?!\S)'.format(mot_escaped), description):
                technos.append(mot)

        # On joint les deux listes de technos en une seule                                                                           
        technos = ', '.join(technos)
        return technos


    # Cette fonction va nettoyer les technos en mettant le nom correct
    def replace_technos(row):
        technos_list = row.split(', ')
        for index, techno in enumerate(technos_list):
            for key, value in techno_dict.items():
                if techno in value:
                    technos_list[index] = key
        return (', ').join(technos_list)


    # Cette fonction va supprimer les doublons de technos
    def remove_duplicates(row):
        technos_list = row.split(', ')
        unique_technos = list(set(technos_list))
        return ', '.join(unique_technos)


    # ------------------------------
    # ------ Création de jobs ------
    # ------------------------------

    # Table en production
    production_table = "jobs"

    # Colonnes à exporter 
    output_columns = ['id', 
                    'date_of_search', 
                    'scrap_number', 
                    'day_of_week', 
                    'job_search', 
                    'job_name', 
                    'company_name', 
                    'city_name', 
                    'city', 
                    'region', 
                    'description', 
                    'lower_salary', 
                    'upper_salary', 
                    'job_type', 
                    'sector']

    # Construction de la requête SELECT
    columns_str = ', '.join(output_columns)
    select_query = f"SELECT {columns_str} FROM {production_table} ORDER BY id DESC"

    cursor.execute(select_query)
    working_dataframe = pd.DataFrame(cursor.fetchall(), columns=columns_str.split(', '))

    # Création de la colonne technos à partir de la fonction. 
    working_dataframe['technos'] = working_dataframe['description'].apply(technology_finder)
    working_dataframe['technos'] = working_dataframe['technos'].apply(replace_technos)
    working_dataframe['technos'] = working_dataframe['technos'].apply(remove_duplicates)
    working_dataframe['scrap_number'] = working_dataframe['scrap_number'].astype(str).replace('nan', '0').astype(float).astype(int)

    # Remise en ordre du dataframe
    working_dataframe = working_dataframe[['id', 
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
                                            'sector']]

    # Création d'une table vide temporaire pour y insérer les données
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jobs_temp (
        id SERIAL,
        date_of_search DATE,
        scrap_number INT,
        day_of_week VARCHAR(20),
        job_search VARCHAR(30),
        job_name VARCHAR(300),
        company_name VARCHAR(300),
        city_name VARCHAR(120),
        city VARCHAR(120),
        region VARCHAR(120),
        technos TEXT,
        description TEXT,
        lower_salary FLOAT,
        upper_salary FLOAT,
        job_type VARCHAR(120),
        sector VARCHAR(300)
    )
    """)
    conn.commit()

    # Récupération de la séquence MAX +1 pour l'ID de séquence
    new_id_sequence = working_dataframe['id'].max() + 1

    # Mise en place du nouvel id de séquence dans la table 
    cursor.execute(f"""
        ALTER SEQUENCE jobs_temp_id_seq RESTART WITH {new_id_sequence};
    """)
    conn.commit()

    working_dataframe.to_csv(os.environ['JOBS_FILE_PATH'], index=False)
    time.sleep(5)

    # Copier les données depuis le fichier CSV en utilisant copy_expert
    with open(os.environ['JOBS_FILE_PATH'], 'r') as f:
        # Utiliser la méthode copy_expert pour copier les données depuis le fichier CSV
        cursor.copy_expert(sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
            sql.Identifier('jobs_temp')), f)
        # Fermer le fichier
        f.close()

    conn.commit()

    # Remplacement de la table originelle
    cursor.execute("""
    DELETE FROM jobs_temp WHERE technos IS NULL;
    """)
    conn.commit()

    # Remplacement de la table originelle
    cursor.execute("""
    ALTER TABLE jobs
    RENAME TO jobs_to_delete;
    """)
    conn.commit()

    cursor.execute("""
    ALTER TABLE jobs_temp
    RENAME TO jobs;
    """)
    conn.commit()

    cursor.execute("""
    DROP TABLE jobs_to_delete;
    """)
    conn.commit()


    # -----------------------------------------
    # ------ Création de jobsoccurrences ------
    # -----------------------------------------

    jobsoccurrences_dataframe = working_dataframe

    # On transforme la colonne techno en liste afin de récupérer chaque élément
    jobsoccurrences_dataframe['technos'] = jobsoccurrences_dataframe['technos'].str.split(',').apply(lambda x: [s.strip() for s in x])

    # Explode sur les technos afin de faire 1 ligne par technos
    jobsoccurrences_dataframe = jobsoccurrences_dataframe.explode('technos')

    # Création de la colonne occurrences afin de faire une somme avec groupby ensuite
    jobsoccurrences_dataframe['occurrences'] = 1

    # Que voilà ici, on met as_index=False sinon le group_by ne remet pas les valeurs dans chaque ligne
    jobsoccurrences_dataframe = jobsoccurrences_dataframe.groupby(['date_of_search', 'day_of_week', 'region', 'job_search', 'technos'], as_index=False).agg({'occurrences':'sum'})
    jobsoccurrences_dataframe = jobsoccurrences_dataframe.rename(columns={"technos": "technologie"})


    # Création d'une table vide temporaire pour y insérer les données
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobsoccurrences_temp (
            date_of_search DATE,
            day_of_week VARCHAR(20),
            region VARCHAR(120),
            job_search VARCHAR(30),
            technologie VARCHAR(120),
            occurrences INT
        );
    """)
    conn.commit()

    jobsoccurrences_dataframe.to_csv(os.environ['JOBSOCCURRENCE_FILE_PATH'], index=False)
    time.sleep(5)

    # Copier les données depuis le fichier CSV en utilisant copy_expert
    with open(os.environ['JOBSOCCURRENCE_FILE_PATH'], 'r') as f:
        # Utiliser la méthode copy_expert pour copier les données depuis le fichier CSV
        cursor.copy_expert(sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
            sql.Identifier('jobsoccurrences_temp')), f)
        # Fermer le fichier
        f.close()

    conn.commit()

    # Remplacement de la table originelle
    cursor.execute("""
    ALTER TABLE jobsoccurrences
    RENAME TO jobsoccurrences_to_delete;
    """)
    conn.commit()

    cursor.execute("""
    ALTER TABLE jobsoccurrences_temp
    RENAME TO jobsoccurrences;
    """)
    conn.commit()

    cursor.execute("""
    DROP TABLE jobsoccurrences_to_delete;
    """)
    conn.commit()


    # -----------------------------------------------------
    # ------ Déclaration de la fin de la maintenance ------
    # -----------------------------------------------------

    cursor.execute("""
        UPDATE maintenance 
        SET status = 'available';
    """)
    conn.commit()
    conn.close()

    os.remove(os.environ['JOBSOCCURRENCE_FILE_PATH'])
    os.remove(os.environ['JOBS_FILE_PATH'])