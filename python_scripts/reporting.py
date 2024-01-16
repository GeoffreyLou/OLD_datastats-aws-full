import psycopg2
import smtplib
import os
import boto3
import json
import pandas as pd
from datetime import date, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from botocore.exceptions import ClientError


# Récupération des variables
def get_secret():

    secret_name_1 = os.environ['SECRET_NAME_1']
    secret_name_2 = os.environ['SECRET_NAME_2']
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

    try:
        get_secret_value_response_2 = client.get_secret_value(
            SecretId=secret_name_2
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

    secret_2 = get_secret_value_response_2['SecretString']
    secret_dict_2 = json.loads(secret_2)

    sender_email = secret_dict_2.get('EMAIL_HOST_USER')
    receiver_email = secret_dict_2.get('EMAIL_SEND_USER')
    password_email = secret_dict_2.get('EMAIL_HOST_PASSWORD')

    return database, user, password_db, host, port, sender_email, receiver_email, password_email

database, user, password_db, host, port, sender_email, receiver_email, password_email = get_secret()

# Définition de la date du jour et de la veille
today = date.today().strftime('%Y-%m-%d')
yesterday = (date.today() - timedelta(days = 1)).strftime('%Y-%m-%d')


# Configuration connexion BDD
conn = psycopg2.connect(database=database,
                        user=user,
                        password=password_db,
                        host=host,
                        port=port)
cursor = conn.cursor()


# Configuration EMAIL
report_date = yesterday
sender_email = sender_email
receiver_email = receiver_email
password = password_email
subject = f"Datastats : rapport du {yesterday}"


# Insertion des données du jour dans la table
cursor.execute("""
INSERT INTO reporting (reporting_date, scrap_number, job_count, success_scrap, duration, scrap_status, occurrences ,daily_job_scrap, lambda_status, cities_to_add)
VALUES
    (%s, 1 , 0, 0, '', 'waiting', 0, 0, 'waiting', 'no'),
    (%s, 2 , 0, 0, '', 'waiting', 0, 0, 'waiting', 'no'),
    (%s, 3 , 0, 0, '', 'waiting', 0, 0, 'waiting', 'no'),
    (%s, 4 , 0, 0, '', 'waiting', 0, 0, 'waiting', 'no'),
    (%s, 5 , 0, 0, '', 'waiting', 0, 0, 'waiting', 'no');
""", [today for i in range(5)])
conn.commit()

# Récupération des données de la veille
cursor = conn.cursor()
cursor.execute("""
    SELECT
        reporting_date,
        scrap_number,
        job_count,
        success_scrap,
        duration,
        scrap_status,
        occurrences,
        daily_job_scrap,
        lambda_status,
        cities_to_add
    FROM reporting
    WHERE reporting_date = %s
    ORDER BY ID ASC
""", [yesterday])
result_df = pd.DataFrame(cursor.fetchall(), columns=['Date de rapport',
                                                     "Numéro de scrap",
                                                     "Nombre de métiers scrapés",
                                                     "Nombre de métiers réellement récupérés",
                                                     "Durée du scrap",
                                                     "Webscraping status",
                                                     "Cumul occurrences journalières ajoutées",
                                                     "Total de métiers scrapés pour la journée",
                                                     "Lambda status",
                                                     "Villes à ajouter"])


# Fonction pour créer le corps de l'e-mail à partir du dataframe
def create_email_body(df, report_date):
    # Ajoute du style HTML pour la mise en forme
    body = f"DATE DU RAPPORT : {report_date}\n\n"

    for index, row in df.iterrows():
        # Ajoute des puces et utilise le gras pour le texte avant la valeur
        body += f"Scrap numéro: {row['Numéro de scrap']}\n"
        body += f"    - Nombre de métiers à scraper: {row['Nombre de métiers scrapés']}\n"
        body += f"    - Nombre de métiers réellement scrapés: {row['Nombre de métiers réellement récupérés']}\n"
        body += f"    - Durée du scrap: {row['Durée du scrap']}\n"
        body += f"    - Webscraping status: {row['Webscraping status']}\n"
        body += f"    - Cumul occurrences journalières ajoutées: {row['Cumul occurrences journalières ajoutées']}\n"
        body += f"    - Total de métiers réellement ajoutés: {row['Total de métiers scrapés pour la journée']}\n"
        body += f"    - Status de la Lambda function: {row['Lambda status']}\n"
        body += f"    - Villes à ajouter: {row['Villes à ajouter']}\n\n"

    return body

# Créer le corps de l'e-mail
email_body = create_email_body(result_df, report_date)


# Configurer l'e-mail
msg = MIMEMultipart()
msg['From'] = sender_email
msg['To'] = receiver_email
msg['Subject'] = subject
msg.attach(MIMEText(email_body, 'plain'))


# Configurer le serveur SMTP (utilise le serveur et le port de ton fournisseur de messagerie)
smtp_server = "smtp.ionos.fr"
smtp_port = 465

# Se connecter au serveur SMTP et envoyer l'e-mail
with smtplib.SMTP_SSL(smtp_server, smtp_port) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, msg.as_string())

