import pandas as pd
import psycopg2
import os

conn = psycopg2.connect(database=os.environ['RDS_DATABASE'],
                        user=os.environ['RDS_USER'],
                        password=os.environ['RDS_PASSWORD'],
                        host=os.environ['RDS_ENDPOINT'],
                        port=os.environ['RDS_PORT'])

print('Connexion OK')

cursor = conn.cursor()
cursor.execute("""
    SELECT values
    FROM lists
    WHERE list = 'jobs_list'
    """)
original_techno_list = cursor.fetchall()
jobs_list = [i.replace('"', '').strip() for i in original_techno_list[0][0].replace("\n","").replace("'", "").strip().split(',')]

print('The job list is : ', jobs_list)

jobs_list.remove('consultant data')

jobs_list = list(set(jobs_list))
jobs_list = str(jobs_list).replace("'", '"').replace('[', '').replace(']', '')

print('The final job list is : ', jobs_list)

cursor.execute(
    f"""
    UPDATE lists
    SET values =  '{jobs_list}'
    WHERE list = 'jobs_list'
    """)
conn.commit()
cursor.close()

print('SUCCESS')

conn.close()
