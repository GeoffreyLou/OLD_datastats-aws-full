import pandas as pd
import psycopg2
import os
import logging
import json
import boto3
from botocore.exceptions import ClientError
from datetime import date
from sqlalchemy import create_engine

# Configuration du logger
logging.basicConfig(filename='/home/ec2-user/logs/city_error_email.txt',
                    level=logging.ERROR,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filemode='a')

# Définition des variables pour mise à jour de la table reporting
scrap_number = os.environ['SCRAP_NUMBER']
date_of_search = date.today().strftime("%Y-%m-%d")

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
    except ClientError as e:
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

# Création de l'engine pour mettre à jour la BDD
engine = create_engine(f"postgresql://{user}:{password_db}@{host}:{port}/{database}")

try:
    # Connexion à la base de données
    conn = psycopg2.connect(database=database,
                            user=user,
                            password=password_db,
                            host=host,
                            port=port)

    # Ce script vérifie simplement si des traitements manuels doivent être réalisés sur les villes
    # Il récupère donc les données "to process"
    cursor = conn.cursor()
    cursor.execute("""
        SELECT value
        FROM city_error
        WHERE status = 'to process'
    """)
    city_df = pd.DataFrame(cursor.fetchall(), columns=['value'])


    if len(city_df) == 0:

        cities_to_add = 'no'

        cursor.execute("""
        UPDATE reporting
        SET
            cities_to_add = %s
        WHERE reporting_date = %s
        AND scrap_number = %s;
        """, (cities_to_add, date_of_search, scrap_number))
        conn.commit()

        # Fermeture de la connexion
        cursor.close()
        conn.close()

    else:
        # Liste des villes à traiter
        values_for_email = city_df['value'].drop_duplicates().to_list()

        cities_to_add = 'yes'

        cursor.execute("""
        UPDATE reporting
        SET
            cities_to_add = %s
        WHERE reporting_date = %s
        AND scrap_number = %s;
        """, (cities_to_add, date_of_search, scrap_number))
        conn.commit()

        # On refait la table pour retirer les doublons si la même erreur arrive dans plusieurs scripts
        cursor.execute("""
            (SELECT value, status
            FROM city_error
            WHERE status = 'to process'
            GROUP BY value, status)
            UNION ALL
            (SELECT value, status
            FROM city_error
            WHERE status = 'processed'
            GROUP BY value, status)
        """)
        new_df = pd.DataFrame(cursor.fetchall(), columns=['value', 'status'])

        cursor.execute("""
            DELETE FROM city_error
        """)
        conn.commit()


        # Mise à jour de la table
        try:
            # Insère le DataFrame dans la table SQL
            new_df.to_sql('city_error', engine, if_exists='replace', index=False)

        except psycopg2.Error as e:
            logging.error(f"Erreur lors de la mise à jour : {e}")

        finally:
            conn.close()

except Exception as e:
    logging.error(f'Erreur : {e}')
                                                                           