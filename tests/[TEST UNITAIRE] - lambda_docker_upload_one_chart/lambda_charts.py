import plotly.express as px
import pandas as pd
import psycopg2
from io import BytesIO
import boto3
import os



def lamba_handler(event, context):

    # -----------------------
    # ------ Ouverture ------
    # -----------------------

    # Client AWS
    s3 = boto3.client(
        service_name='s3',
        region_name=os.environ['REGION'])
    
    print('Ouverture OK')


    # Informations de connexion à la BDD : 
    conn = psycopg2.connect(database=os.environ['DATABASE_DB'], 
                    user=os.environ['DATABASE_USER'], 
                    password=os.environ['DATABASE_PASSWORD'], 
                    host=os.environ['DATABASE_ENDPOINT'], 
                    port=os.environ['DATABASE_PORT'])
    
    bucket_name = os.environ['BUCKET_NAME']

    cursor = conn.cursor()

    print('Connexion OK')

    # Définition de la fonction pour uploader sur le S3
    def upload_to_s3(data, file_name, bucket):
        s3 = boto3.client('s3')
        try:
            s3.upload_fileobj(data, bucket_name, file_name)
            print(f'Upload réussi : {file_name}')
        except Exception as e:
            print(f'Erreur lors de l\'upload sur S3 : {e}')

    print('Function OK')

    # --------------------------
    # ------ Requêtes SQL ------
    # --------------------------


    # Line chart job count
    cursor.execute("""
        SELECT DATE(DATE_TRUNC('month', date_of_search)), count(*) 
        FROM jobs 
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE(DATE_TRUNC('month', NOW()))
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY DATE(DATE_TRUNC('month', date_of_search))           
    """)
    jobs_count_df = pd.DataFrame(cursor.fetchall(), columns=['date', 'count'])

    print('DF OK')
    print(jobs_count_df.iloc[0])

    # ------------------------
    # ------ Graphiques ------
    # ------------------------


    # Line chart job count
    jobs_count_df = jobs_count_df.groupby('date')['count'].sum().reset_index()

    jobs_count_df['date'] = pd.to_datetime(jobs_count_df['date'])
    jobs_count_df.sort_values(by=['date'], ascending=False, inplace=True)

    total_jobs_chart = px.line(
        jobs_count_df,
        x='date',
        y='count',
        custom_data=['count'],
        height=375
    )

    total_jobs_chart.update_traces(line=dict(color="#54a24b")) # e377c2 joli rose

    total_jobs_chart.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        xaxis=dict(title_text="Date", showline=True, color="#adb5bd"),
        yaxis=dict(visible=True, title_text=None, color="#adb5bd"),   
        xaxis_showgrid=False,  # Masquer la grille x-axis pour une meilleure apparence
        yaxis_showgrid=False,  # Masquer la grille y-axis pour une meilleure apparence
        title=None,  # Titre du graphique
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        xaxis_title=None
    )

    total_jobs_chart.update_xaxes(
        dtick="M1",
        tickformat="%B %Y"
    ) 

    total_jobs_chart.update_yaxes(ticksuffix='  ')  

    # Ajout du cadre blanc
    total_jobs_chart.update_layout(
        shapes=[
            dict(
                type='rect',
                xref='paper', yref='paper',
                x0=0, y0=0, x1=1, y1=1,
                line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                fillcolor="rgba(255,255,255,0)"  # Fond transparent
    )])

    total_jobs_chart.update_traces(
        mode='lines+text', 
        text=jobs_count_df['count'], 
        textposition='top center', 
        textfont=dict(color='white'),
        hovertemplate='%{customdata[0]} offres d\'emploi pour le mois de %{x} <extra></extra>'
    )

    total_jobs_chart.update_layout(
        xaxis_range=[jobs_count_df['date'].min() - pd.DateOffset(days=5), jobs_count_df['date'].max() + pd.DateOffset(days=5)],
        yaxis_range=[0, jobs_count_df['count'].max() + jobs_count_df['count'].max()*10/100]
    )

    print('Line Chart OK ')

    # Exporter le graphique en PNG
    image_stream = BytesIO()
    total_jobs_chart.write_image(image_stream, format='png', scale=2)
    image_stream.seek(0)

    print('BytesIO OK')

    upload_to_s3(image_stream, "chart3.png")

    print('SUCCESS')

    # -----------------------
    # ------ Fermeture ------
    # -----------------------

    cursor.close()
    conn.close()
