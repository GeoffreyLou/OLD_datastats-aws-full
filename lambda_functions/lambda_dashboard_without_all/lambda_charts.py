import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2
import boto3
import os
import json
from botocore.exceptions import ClientError

# Retrait des warnings dans les logs lambda
import warnings
warnings.filterwarnings("ignore")


def get_db_secret():

    secret_name = os.environ['SECRET_NAME_1']
    region_name = os.environ['SECRET_REGION_NAME']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)

    # Retrieve specific variables (e.g., username and password)
    database = secret_dict.get('dbInstanceIdentifier')
    user = secret_dict.get('username')
    password = secret_dict.get('password')
    host = secret_dict.get('host')
    port = secret_dict.get('port')

    return database, user, password, host, port

def get_other_secret():

    secret_name = os.environ['SECRET_NAME_2']
    region_name = os.environ['SECRET_REGION_NAME']

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret)

    aws_id = secret_dict.get('AWS_K_ID')
    aws_secret_id = secret_dict.get('AWS_S_K_ID')
    aws_bucket = secret_dict.get('AWS_STORAGE_BUCKET_NAME')
    
    return aws_id, aws_secret_id, aws_bucket


def lambda_handler(event, context):

    try:

        database, user, password, host, port = get_db_secret()
        aws_id, aws_secret_id, aws_bucket = get_other_secret()
        job_variable = event.get('start_variable')

        job_variable_for_file = job_variable.lower()\
                                        .replace('ô', 'o')\
                                        .replace('é', 'e')\
                                        .replace("'", '_')\
                                        .replace(' ', '_')\
                                        .replace('î', 'i')\
                                        .replace('-', '_')

        # Client AWS
        s3 = boto3.client(
            service_name='s3',
            region_name=os.environ['SECRET_REGION_NAME'],
            aws_access_key_id=aws_id, 
            aws_secret_access_key=aws_secret_id)


        # Informations de connexion à la BDD : 
        conn = psycopg2.connect(
            database=database, 
            user=user, 
            password=password, 
            host=host, 
            port=port)

        bucket_name = aws_bucket

        # Chemin vers le répertoire temporaire dans le conteneur
        tmp_directory = '/tmp/'

        # Si le répertoire n'existe pas, création de ce dernier
        if not os.path.exists(tmp_directory):
            os.makedirs(tmp_directory)

        # Création du curseur
        cursor = conn.cursor()

        cursor.execute("""
                    SELECT DISTINCT region 
                    FROM jobs
                    WHERE region IS NOT NULL    
                    """)

        regions = [i[0] for i in cursor.fetchall()]
        regions_tupled = tuple(regions)

        regions.append('all')

        for i in regions:
            if i == 'all':
                region = regions_tupled
                condition = 'IN %s'
                region_for_file = 'all'
            else:
                region = i 
                condition = '= %s'
                region_for_file = i.lower()\
                                .replace('ô', 'o')\
                                .replace('é', 'e')\
                                .replace("'", '_')\
                                .replace(' ', '_')\
                                .replace('î', 'i')\
                                .replace('-', '_')


            #-----------------------------
            #------ Line chart jobs ------
            #-----------------------------


            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    DATE(DATE_TRUNC('month', date_of_search)), 
                    job_search, 
                    count(*)
                FROM jobs
                WHERE job_search = %s
                AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
                GROUP BY DATE(DATE_TRUNC('month', date_of_search)), job_search
                ORDER BY DATE(DATE_TRUNC('month', date_of_search)), job_search
            """, (job_variable, region))
            dashboard_jobs_count_df = pd.DataFrame(cursor.fetchall(), columns=['control', 'date', 'job_search', 'count'])

            if dashboard_jobs_count_df['control'].sum() != 0:
                # Graphique
                dashboard_jobs_count_df = dashboard_jobs_count_df.groupby(['date', 'job_search'])['count'].sum().reset_index()
                dashboard_jobs_count_df['date'] = pd.to_datetime(dashboard_jobs_count_df['date'])

                dashboard_total_jobs_chart = go.Figure()

                line_color_map = {
                    "Data Engineer": "#636EFA",   
                    "Data Analyst": "#EF553B",    
                    "Data Scientist": "#00CC96", 
                    "Lead Data": "#FFA15A",     
                    "Data Manager": "#19D3F3",   
                    "Data Steward": "#FF6692",   
                    "ML Engineer": "#FECB52", 
                    "Analytics Engineer": "#B6E880",      
                    "Data Architect": "#FF97FF"   
                }

                # On ajoute une ligne par métier et on y applique tous les éléments visuels
                for dashboard_jobs_count_job_value in dashboard_jobs_count_df['job_search'].unique():
                    dashboard_total_jobs_chart.add_trace(go.Scatter( 
                    x=dashboard_jobs_count_df['date'][dashboard_jobs_count_df['job_search'] == dashboard_jobs_count_job_value ],
                    y=dashboard_jobs_count_df['count'][dashboard_jobs_count_df['job_search'] == dashboard_jobs_count_job_value ],
                    mode="lines+text",
                    text=dashboard_jobs_count_df['count'][dashboard_jobs_count_df['job_search'] == dashboard_jobs_count_job_value ],
                    textposition="top center",
                    textfont=dict(color='white'),
                    customdata=dashboard_jobs_count_df[['job_search', 'count']][dashboard_jobs_count_df['job_search'] == dashboard_jobs_count_job_value ],
                    name = dashboard_jobs_count_job_value,
                    line_color=line_color_map[dashboard_jobs_count_job_value],
                    hovertemplate='%{customdata[1]} offres d\'emploi %{customdata[0]} pour le mois de %{x} <extra></extra>'))

                # Mise à jour du layout
                dashboard_total_jobs_chart.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    xaxis=dict(title_text=None, showline=True, color="#adb5bd"),
                    yaxis=dict(visible=True, title_text=None, color="#adb5bd"),   
                    xaxis_showgrid=False,  
                    yaxis_showgrid=False, 
                    title=None,  
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    xaxis_title=None,
                    height=375,
                    width=1100,
                )                

                # Formattage de l'axe X     
                dashboard_total_jobs_chart.update_xaxes(
                    dtick="M1",
                    tickformat="%B %Y"
                ) 

                # Ajout d'un suffixe pour l'axe Y sinon c'est collé à l'axe
                dashboard_total_jobs_chart.update_yaxes(ticksuffix='  ')  

                # Ajout du cadre blanc
                dashboard_total_jobs_chart.update_layout(
                    shapes=[
                        dict(
                            type='rect',
                            xref='paper', yref='paper',
                            x0=0, y0=0, x1=1, y1=1,
                            line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                            fillcolor="rgba(255,255,255,0)"  # Fond transparent
                )])

                # Mise en place d'espaces entre le graphique et les lignes d'axes pour aérer
                dashboard_total_jobs_chart.update_layout(
                    xaxis_range=[dashboard_jobs_count_df['date'].min() - pd.DateOffset(days=5), dashboard_jobs_count_df['date'].max() + pd.DateOffset(days=5)],
                    yaxis_range=[0, dashboard_jobs_count_df['count'].max()+dashboard_jobs_count_df['count'].max()*10/100])
                
                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_linechart_jobs_evolution_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_total_jobs_chart.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Linechart jobs evolution terminé')


            #------------------------------------------
            #------ Top 20 positif technos chart ------
            #------------------------------------------

            cursor.execute(f"""                   
                WITH data_for_lag AS (
                    (SELECT
                        1 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
                    AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                    UNION ALL
                    (SELECT
                        2 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND date_of_search < DATE_TRUNC('month', NOW())
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}                  
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                ),
                lag_aggregation AS (
                    SELECT 
                        lag_value,
                        technologie,
                        CAST(total_occurrences AS DECIMAL) AS total_occurrences,
                        LAG(total_occurrences) OVER (PARTITION BY technologie ORDER BY lag_value) AS previous_total_occurrences
                    FROM data_for_lag 
                    WHERE total_occurrences > (SELECT PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY total_occurrences) FROM data_for_lag)
                    ORDER BY 
                        technologie, 
                        lag_value
                )
                SELECT
                    1 AS control,
                    technologie,
                    total_occurrences,
                    previous_total_occurrences,
                    COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) AS evolution
                FROM lag_aggregation
                WHERE COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) != 0
                AND COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) > 0
                ORDER BY evolution DESC
                LIMIT 20
            """, (job_variable, region, job_variable, region))
            dashboard_top20_plus_df = pd.DataFrame(cursor.fetchall(), columns=[
                'control', 
                'technologie', 
                'total_occurrences', 
                'previous_total_occurrences', 
                'evolution'
            ])
            
            dashboard_top20_plus_df = dashboard_top20_plus_df.sort_values(by='evolution', ascending=True)

            if dashboard_top20_plus_df['control'].sum() != 0:
                # Graphique

                dashboard_top20_plus_chart = go.Figure()

                # Ajouter les barres pour chaque technologie
                dashboard_top20_plus_chart.add_trace(
                    go.Bar(
                        y=dashboard_top20_plus_df['technologie'],
                        x=dashboard_top20_plus_df['evolution'],
                        textposition="outside",
                        textfont=dict(size=15),
                        cliponaxis=True,
                        textangle=0,
                        marker_line_width=0,
                        textfont_color="white",
                        orientation='h',
                        customdata=dashboard_top20_plus_df[['technologie', 'total_occurrences', 'previous_total_occurrences']],
                        hovertemplate='La technologie %{customdata[0]} a été demandée %{customdata[1]} fois les 3 derniers mois, comparativement à %{customdata[2]} les 3 mois les précédents.<extra></extra>',
                        text=dashboard_top20_plus_df['evolution'].apply(lambda x: f"{x:.2f}%"),
                        marker_color="#54a24b"
                ))

                # Mise à jour du layout
                dashboard_top20_plus_chart.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    title=None,
                    xaxis_title=None,
                    yaxis=dict(title=None, tickformat="%", color="#adb5bd"),
                    showlegend=False,
                    xaxis=dict(visible=False),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",
                    dragmode=False,
                    height=(len(dashboard_top20_plus_df)*25)+30,
                    width=700,
                    xaxis_range=[
                        0, 
                        dashboard_top20_plus_df['evolution'].max() + dashboard_top20_plus_df['evolution'].max()*20/100
                ])

                # Ajout d'un suffixe pour l'axe Y sinon c'est collé à l'axe
                dashboard_top20_plus_chart.update_yaxes(ticksuffix='  ')

                # Ajout du cadre blanc
                dashboard_top20_plus_chart.update_layout(
                    shapes=[
                        dict(
                            type='rect',
                            xref='paper', yref='paper',
                            x0=0, y0=0, x1=1, y1=1,
                            line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                            fillcolor="rgba(255,255,255,0)"  # Fond transparent
                )])

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_top20_plus_chart_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_top20_plus_chart.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Top 20 positif technos chart')


            #------------------------------------------
            #------ Top 20 positif technos table ------
            #------------------------------------------

            cursor.execute(f"""
                WITH data_for_lag AS (
                    (SELECT
                        1 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
                    AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                    UNION ALL
                    (SELECT
                        2 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND date_of_search < DATE_TRUNC('month', NOW())
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                ),
                lag_aggregation AS (
                    SELECT 
                        lag_value,
                        technologie,
                        CAST(total_occurrences AS DECIMAL) AS total_occurrences,
                        LAG(total_occurrences) OVER (PARTITION BY technologie ORDER BY lag_value) AS previous_total_occurrences
                    FROM data_for_lag 
                    WHERE total_occurrences > (SELECT PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY total_occurrences) FROM data_for_lag)
                    ORDER BY 
                        technologie, 
                        lag_value
                )
                SELECT
                    1 AS control,                
                    technologie,
                    total_occurrences,
                    previous_total_occurrences,
                    COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) AS evolution
                FROM lag_aggregation
                WHERE COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) != 0
                AND COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) > 0
                ORDER BY COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) DESC
                LIMIT 20
            """, (job_variable, region, job_variable, region))

            dashboard_top20_plus_table_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'technologie', 
                    'total_occurrences', 
                    'previous_total_occurrences', 
                    'evolution'
            ])

            dashboard_top20_plus_table_df['evolution'] = dashboard_top20_plus_table_df['evolution'].astype(str)
            dashboard_top20_plus_table_df['evolution'] = dashboard_top20_plus_table_df['evolution'].apply(
                lambda x: '+ ' + x + ' %'
            ) 


            if dashboard_top20_plus_table_df['control'].sum() != 0:
                # Graphique

                dashboard_top20_plus_table = go.Figure(
                    data=[go.Table(
                        columnwidth = [250,150],
                        header=dict(
                            values=['Techno', 'Dernier trim.', 'Vs trim. antérieur', 'Evolution'],
                            line_color='#212529',
                            fill_color='#3c4044',
                            height=30,
                            align=['center', 'center']),
                        cells=dict(
                            values=[
                                dashboard_top20_plus_table_df['technologie'].to_list(), 
                                dashboard_top20_plus_table_df['total_occurrences'].to_list(), 
                                dashboard_top20_plus_table_df['previous_total_occurrences'].to_list(),
                                dashboard_top20_plus_table_df['evolution'].to_list()
                            ],
                            line_color='#212529',
                            fill_color='#343a40',
                            align=['center', 'center'],
                            height=25,
                            font_color=['white'])
                )])

                dashboard_top20_plus_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=len(dashboard_top20_plus_table_df['technologie'].unique())*25 + 30,
                    width= 700
                )       

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_top20_plus_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_top20_plus_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Top 20 positif technos table')


            #------------------------------------------
            #------ Top 20 negatif technos chart ------
            #------------------------------------------

            cursor.execute(f"""
                WITH data_for_lag AS (
                    (SELECT
                        1 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
                    AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                    UNION ALL
                    (SELECT
                        2 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND date_of_search < DATE_TRUNC('month', NOW())
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                ),
                lag_aggregation AS (
                    SELECT 
                        lag_value,
                        technologie,
                        CAST(total_occurrences AS DECIMAL) AS total_occurrences,
                        LAG(total_occurrences) OVER (PARTITION BY technologie ORDER BY lag_value) AS previous_total_occurrences
                    FROM data_for_lag 
                    WHERE total_occurrences > (SELECT PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY total_occurrences) FROM data_for_lag)
                    ORDER BY 
                        technologie, 
                        lag_value
                )
                SELECT
                    1 AS control,
                    technologie,
                    total_occurrences,
                    previous_total_occurrences,
                    COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) AS evolution
                FROM lag_aggregation
                WHERE COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) != 0
                AND COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) < 0
                ORDER BY evolution ASC
                LIMIT 20
            """, (job_variable, region, job_variable, region))
            dashboard_top_20_moins_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control', 
                    'technologie', 
                    'total_occurrences', 
                    'previous_total_occurrences', 
                    'evolution'
            ])

            dashboard_top_20_moins_df = dashboard_top_20_moins_df.sort_values(by='evolution', ascending=True)


            if dashboard_top_20_moins_df['control'].sum() != 0:

                # Graphique
                dashboard_top20_moins_chart = go.Figure()

                # Ajouter les barres pour chaque technologie
                dashboard_top20_moins_chart.add_trace(
                    go.Bar(
                        y=dashboard_top_20_moins_df['technologie'],
                        x=dashboard_top_20_moins_df['evolution'],
                        textposition="outside",
                        textfont=dict(size=15),
                        cliponaxis=True,
                        textangle=0,
                        marker_line_width=0,
                        textfont_color="white",
                        orientation='h',
                        customdata=dashboard_top_20_moins_df[['technologie', 'total_occurrences', 'previous_total_occurrences']],
                        hovertemplate='La technologie %{customdata[0]} a été demandée %{customdata[1]} fois les 3 derniers mois, comparativement à %{customdata[2]} les 3 mois les précédents.<extra></extra>',
                        text=dashboard_top_20_moins_df['evolution'].apply(lambda x: f"{x:.2f}%"),
                        marker_color="#FF4444"
                ))

                # Mettre en forme le graphique
                dashboard_top20_moins_chart.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    title=None,
                    xaxis_title=None,
                    yaxis=dict(title=None, tickformat="%", color="#adb5bd"),
                    showlegend=False,
                    xaxis=dict(visible=False),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",
                    dragmode=False,
                    height=(len(dashboard_top_20_moins_df)*25)+30,
                    width=700,
                    xaxis_range=[dashboard_top_20_moins_df['evolution'].min() + dashboard_top_20_moins_df['evolution'].min()*20/100, 0]
                )

                dashboard_top20_moins_chart.update_yaxes(ticksuffix='  ')
                # Ajout du cadre blanc
                dashboard_top20_moins_chart.update_layout(
                    shapes=[
                        dict(
                            type='rect',
                            xref='paper', yref='paper',
                            x0=0, y0=0, x1=1, y1=1,
                            line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                            fillcolor="rgba(255,255,255,0)"  # Fond transparent
                )])

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_top20_moins_chart_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_top20_moins_chart.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Top 20 negatif technos chart')


            #------------------------------------------
            #------ Top 20 négatif technos table ------
            #------------------------------------------

            # Requête SQL
            cursor.execute(f"""
                WITH data_for_lag AS (
                    (SELECT
                        1 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
                    AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}                              
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                    UNION ALL
                    (SELECT
                        2 AS lag_value,
                        technologie,
                        SUM(occurrences) AS total_occurrences
                    FROM jobsoccurrences
                    WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
                    AND date_of_search < DATE_TRUNC('month', NOW())
                    AND technologie IS NOT NULL
                    AND job_search = %s
                    AND region {condition}                      
                    GROUP BY technologie
                    ORDER BY total_occurrences DESC)
                ),
                lag_aggregation AS (
                    SELECT 
                        lag_value,
                        technologie,
                        CAST(total_occurrences AS DECIMAL) AS total_occurrences,
                        LAG(total_occurrences) OVER (PARTITION BY technologie ORDER BY lag_value) AS previous_total_occurrences
                    FROM data_for_lag 
                    WHERE total_occurrences > (SELECT PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY total_occurrences) FROM data_for_lag)
                    ORDER BY 
                        technologie, 
                        lag_value
                )
                SELECT
                    1 AS control,
                    technologie,
                    total_occurrences,
                    previous_total_occurrences,
                    COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) AS evolution
                FROM lag_aggregation
                WHERE COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) != 0
                AND COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) < 0
                ORDER BY COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) ASC
                LIMIT 20
            """, (job_variable, region, job_variable, region))
            dashboard_top_20_moins_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=['control',
                         'technologie', 
                         'total_occurrences', 
                         'previous_total_occurrences', 
                         'evolution'
            ])

            dashboard_top_20_moins_df = dashboard_top_20_moins_df.sort_values(by=['evolution'], ascending=False)
            dashboard_top_20_moins_df['evolution'] = dashboard_top_20_moins_df['evolution'].astype(str).apply(
                lambda x: x + ' %'
            )

            if dashboard_top_20_moins_df['control'].sum() != 0:

                # Graphique
                dashboard_top20_moins_table = go.Figure(
                    data=[go.Table(
                        columnwidth = [250,150],
                        header=dict(
                            values=['Techno', 'Dernier trim.', 'Vs trim. antérieur', 'Evolution'],
                            line_color='#212529',
                            fill_color='#3c4044',
                            height=30,
                            align=['center', 'center']),
                        cells=dict(
                            values=[
                                dashboard_top_20_moins_df['technologie'].to_list(), 
                                dashboard_top_20_moins_df['total_occurrences'].to_list(), 
                                dashboard_top_20_moins_df['previous_total_occurrences'].to_list(),
                                dashboard_top_20_moins_df['evolution'].to_list()
                            ],
                            line_color='#212529',
                            fill_color='#343a40',
                            align=['center', 'center'],
                            height=25,
                            font_color=['white'])
                )])

                dashboard_top20_moins_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=len(dashboard_top_20_moins_df['technologie'].unique())*25 + 30,
                    width=700
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_top20_moins_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_top20_moins_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Top 20 négatif technos table')

                
            # ------------------------------
            # ------ Contexte salaires -----
            # ------------------------------

            # Les graphiques des salaires ne sont pas les mêmes si on choisit une région ou plusieurs
            # Toutes les régions = 1 graphique salaire médian par métier pour toutes les régions
            # 1 région choisie = salaire médian et moyen pour la région choisie

            # -----------------------------------------------
            # ------ Tableau salaire médian all regions -----
            # -----------------------------------------------

            # Toutes les régions
            if i == 'all':

                cursor.execute(f"""
                        SELECT
                        1 AS control, 
                        region,
                        COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY lower_salary) AS INT) AS TEXT) || ' €'), 'No data') AS tranche_min,
                        COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY upper_salary) AS INT) AS TEXT) || ' €'), 'No data' ) AS tranche_max
                    FROM jobs
                    WHERE job_search IS NOT NULL
                    AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
                    AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY region
                    ORDER BY region
                """, (job_variable, region))

                dashboard_med_salary_all_regions_df = pd.DataFrame(
                    cursor.fetchall(), columns=[
                        'control',
                        'region',
                        'tranche_min',
                        'tranche_max'
                ])

                # Cas particulier pour les jobs sans aucun salaire
                if dashboard_med_salary_all_regions_df['tranche_min'].unique().all() == 'No data':

                    dashboard_med_salary_all_regions_df = pd.DataFrame(
                        data={
                    'region': [region_data for region_data in region],
                    'tranche_min': ['No data' for no_data in range(len(region))],
                    'tranche_max': ['No data'for no_data in range(len(region))]
                    })

                dashboard_med_salary_all_regions_df['region'] = dashboard_med_salary_all_regions_df['region'].apply(lambda x: x.replace('Île-De-France', 'Ile-De-France'))
                dashboard_med_salary_all_regions_df = dashboard_med_salary_all_regions_df.sort_values(by='region')
                dashboard_med_salary_all_regions_df['region'] = dashboard_med_salary_all_regions_df['region'].apply(lambda x: x.replace('Ile-De-France', 'Île-De-France'))

                # Cas particulier pour l'ajout de la corse si absente
                if 'Corse' not in dashboard_med_salary_all_regions_df['region'].unique():

                    df_to_add = pd.DataFrame(
                        data={
                    'region': ['Corse'],
                    'tranche_min': ['No data'],
                    'tranche_max': ['No data']
                    })

                    dashboard_med_salary_all_regions_df = pd.concat([dashboard_med_salary_all_regions_df, df_to_add])
                    dashboard_med_salary_all_regions_df = dashboard_med_salary_all_regions_df.sort_values(by='region')

                dashboard_med_salary_all_regions_df['region'] = dashboard_med_salary_all_regions_df['region'].apply(lambda x: x.replace('Île-De-France', 'Ile-De-France'))
                dashboard_med_salary_all_regions_df = dashboard_med_salary_all_regions_df.sort_values(by='region')
                dashboard_med_salary_all_regions_df['region'] = dashboard_med_salary_all_regions_df['region'].apply(lambda x: x.replace('Ile-De-France', 'Île-De-France'))

                # Graphique
                dashboard_med_salary_all_regions_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [300,200],
                            header=dict(
                                values=[
                                    'Region', 
                                    'Min', 
                                    'Max'
                                    ],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30
                                ),
                            cells=dict(
                                values=[
                                    dashboard_med_salary_all_regions_df['region'].to_list(), 
                                    dashboard_med_salary_all_regions_df['tranche_min'].to_list(), 
                                    dashboard_med_salary_all_regions_df['tranche_max'].to_list()
                                    ],
                                line_color='#212529',
                                fill_color='#343a40',
                                align=['center'],
                                height=25,
                                font_color=[
                                    'white', 
                                    ['dimgray' if str(x.strip()) == "No data" else "white" for x in list(dashboard_med_salary_all_regions_df['tranche_min'])]
                    ]))])
                
                dashboard_med_salary_all_regions_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_med_salary_all_regions_df['region'])*25)+30,
                    width=700
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_med_salary_all_regions_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_med_salary_all_regions_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Tableau salaire médian all regions')

            # Une seule région
            else:

                # -----------------------------------------------
                # ------ Tableau salaire médian one region ------
                # -----------------------------------------------

                cursor.execute(f"""
                        SELECT 
                        1 AS control,
                        region,
                        COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY lower_salary) AS INT) AS TEXT) || ' €'), 'No data') AS tranche_min,
                        COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY upper_salary) AS INT) AS TEXT) || ' €'), 'No data' ) AS tranche_max
                    FROM jobs
                    WHERE job_search IS NOT NULL
                    AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
                    AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY region
                    ORDER BY region
                """, (job_variable, region))
                
                dashboard_med_salary_one_region_df = pd.DataFrame(
                    cursor.fetchall(), 
                    columns=[
                        'control',
                        'region',
                        'tranche_min',
                        'tranche_max'
                ])

                if dashboard_med_salary_one_region_df['control'].sum() == 0: 

                    dashboard_med_salary_one_region_df = pd.DataFrame(
                        data={
                            'region': [region],
                            'tranche_min': ['No data'],
                            'tranche_max': ['No data']
                        })

                dashboard_med_salary_one_region_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [300,200],
                            header=dict(
                                values=[
                                    'Region', 
                                    'Min', 
                                    'Max'
                                    ],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30
                                ),
                            cells=dict(
                                values=[
                                    dashboard_med_salary_one_region_df['region'].to_list(), 
                                    dashboard_med_salary_one_region_df['tranche_min'].to_list(), 
                                    dashboard_med_salary_one_region_df['tranche_max'].to_list()
                                    ],
                                line_color='#212529',
                                fill_color='#343a40',
                                align=['center'],
                                height=25,
                                font_color=[
                                    'white', 
                                    ['dimgray' if str(x.strip()) == "No data" else "white" for x in list(dashboard_med_salary_one_region_df['tranche_min'])]
                ]))])
            
                dashboard_med_salary_one_region_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_med_salary_one_region_df['region'])*25)+30,
                    width=700
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_med_salary_one_region_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_med_salary_one_region_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Tableau salaire médian one region')


                # ----------------------------------------------
                # ------ Tableau salaire moyen one region ------
                # ----------------------------------------------            
                
                cursor.execute(f"""
                        SELECT 
                        1 AS control,
                        region,
                        COALESCE((CAST(CAST( AVG(lower_salary) AS INT) AS TEXT) || ' €'), 'No data') AS tranche_min,
                        COALESCE((CAST(CAST( AVG(upper_salary) AS INT) AS TEXT) || ' €'), 'No data' ) AS tranche_max
                    FROM jobs
                    WHERE job_search IS NOT NULL
                    AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
                    AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                    AND job_search = %s
                    AND region {condition}
                    GROUP BY region
                    ORDER BY region
                """, (job_variable, region))
                dashboard_avg_salary_region_df = pd.DataFrame(
                    cursor.fetchall(), 
                    columns=[
                        'control',
                        'region',
                        'tranche_min',
                        'tranche_max'
                ])        

                if dashboard_avg_salary_region_df['control'].sum() == 0: 

                    dashboard_avg_salary_region_df = pd.DataFrame(
                        data={
                            'region': [region],
                            'tranche_min': ['No data'],
                            'tranche_max': ['No data']
                        })

                dashboard_avg_salary_one_region_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [300,200],
                            header=dict(
                                values=[
                                    'Region', 
                                    'Min', 
                                    'Max'
                                    ],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30
                                ),
                            cells=dict(
                                values=[
                                    dashboard_avg_salary_region_df['region'].to_list(), 
                                    dashboard_avg_salary_region_df['tranche_min'].to_list(), 
                                    dashboard_avg_salary_region_df['tranche_max'].to_list()
                                    ],
                                line_color='#212529',
                                fill_color='#343a40',
                                align=['center'],
                                height=25,
                                font_color=[
                                    'white', 
                                    ['dimgray' if str(x.strip()) == "No data" else "white" for x in list(dashboard_avg_salary_region_df['tranche_min'])]
                ]))])

                dashboard_avg_salary_one_region_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_avg_salary_region_df['region'])*25)+30,
                    width=700
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_avg_salary_one_region_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_avg_salary_one_region_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Tableau salaire moyen one region')


            # ------------------------------------------------
            # ------ Linechart evolution salaire global ------
            # ------------------------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    DATE(DATE_TRUNC('month', date_of_search)) AS date,
                    PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY lower_salary) AS tranche_min,
                    PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY upper_salary) AS tranche_max
                FROM jobs
                WHERE job_search = %s
                AND region {condition}
                AND date_of_search >= '2023-12-01'
                AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 month'
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())                 
                GROUP BY DATE(DATE_TRUNC('month', date_of_search))
            """, (job_variable, region))

            dashboard_median_salary_evolution_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'date', 
                    'tranche_min', 
                    'tranche_max'
                    ])

            if dashboard_median_salary_evolution_df['tranche_min'].sum() != 0:

                # Remplacement des valeurs nulles pour éviter des "trous"
                dashboard_median_salary_evolution_df['tranche_min'] = dashboard_median_salary_evolution_df['tranche_min']\
                    .fillna(method='bfill')\
                    .fillna(method='ffill')
                dashboard_median_salary_evolution_df['tranche_max'] = dashboard_median_salary_evolution_df['tranche_max']\
                    .fillna(method='bfill')\
                    .fillna(method='ffill')

                dashboard_median_salary_evolution_df['date'] = pd.to_datetime(dashboard_median_salary_evolution_df['date'])

                dashboard_median_salary_global_evolution = go.Figure()

                dashboard_median_salary_global_evolution.add_trace(
                    go.Scatter(
                        x=dashboard_median_salary_evolution_df['date'], 
                        y=dashboard_median_salary_evolution_df['tranche_min'], 
                        fill = None, 
                        line_color = 'green', 
                        mode='lines', 
                        hovertemplate="Tranche min : <b>%{y}</b>pour le mois de <b>%{x}</b><extra></extra>"
                ))

                dashboard_median_salary_global_evolution.add_trace(
                    go.Scatter(
                        x=dashboard_median_salary_evolution_df['date'], 
                        y=dashboard_median_salary_evolution_df['tranche_max'], 
                        fill = 'tonexty', 
                        line_color = 'green', 
                        mode='lines',
                        hovertemplate="Tranche max : <b>%{y}</b>pour le mois de <b>%{x}</b><extra></extra>"
                ))

                dashboard_median_salary_global_evolution.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    xaxis=dict(title_text=None, showline=True, color="#adb5bd", showgrid = False),
                    yaxis=dict(visible=True, title_text=None, color="#adb5bd", showgrid = True, gridcolor="darkgrey", gridwidth=0.5),   
                    title=None,  
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    xaxis_title=None,
                    height=355,
                    width=700,
                    showlegend=False,
                    xaxis_range=[dashboard_median_salary_evolution_df['date'].min() - pd.DateOffset(days=5), dashboard_median_salary_evolution_df['date'].max() + pd.DateOffset(days=5)],
                    yaxis_range=[0, dashboard_median_salary_evolution_df['tranche_max'].max() + dashboard_median_salary_evolution_df['tranche_max'].max()*20/100],
                    yaxis_ticksuffix = '  ',
                    xaxis_dtick="M1",
                    xaxis_tickformat="%B %Y"
                )

                # Ajout du cadre blanc
                dashboard_median_salary_global_evolution.update_layout(
                    shapes=[
                        dict(
                            type='rect',
                            xref='paper', yref='paper',
                            x0=0, y0=0, x1=1, y1=1,
                            line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                            fillcolor="rgba(255,255,255,0)"  # Fond transparent
                )])                

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_median_salary_global_evolution_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_median_salary_global_evolution.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Linechart evolution salaire global')


            # ---------------------------------------
            # ------ Linechart providers cloud ------
            # ---------------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    DATE(DATE_TRUNC('month', date_of_search)), 
                    technologie, 
                    SUM(occurrences) as count
                FROM jobsoccurrences 
                WHERE technologie IN ('AWS', 'GCP', 'Azure') 
                    AND job_search = %s
                    AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
                GROUP BY DATE(DATE_TRUNC('month', date_of_search)), technologie 
                ORDER BY DATE(DATE_TRUNC('month', date_of_search)) DESC, SUM(occurrences)  DESC              
            """, (job_variable, region))
            dashboard_df_cloud_providers_line = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'date', 
                    'provider', 
                    'count'
            ])

            if dashboard_df_cloud_providers_line['control'].sum() != 0 and len(dashboard_df_cloud_providers_line['date'].unique()) > 1:

                # Graphique
                color_map= {
                    'AWS': '#ff9900', 
                    'Azure': '#008ad7', 
                    'GCP': '#db4437'
                }
                
                dashboard_cloud_providers_evolution = px.line(
                    dashboard_df_cloud_providers_line, 
                    'date', 
                    'count',
                    custom_data=['provider', 'count'],
                    color='provider', 
                    color_discrete_map=color_map,
                    height=375,
                    width=1100
                )

                dashboard_cloud_providers_evolution.update_xaxes(
                    dtick="M1",
                    tickformat="%B %Y"
                )

                dashboard_cloud_providers_evolution.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    xaxis=dict(title_text="Date", showline=True, color="#adb5bd"),
                    yaxis=dict(visible=True, title_text=None, color="#adb5bd"),   
                    xaxis_showgrid=False,  # Masquer la grille x-axis pour une meilleure apparence
                    yaxis_showgrid=False,  # Masquer la grille y-axis pour une meilleure apparence
                    title=None,  # Titre du graphique
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    legend_title_text='Providers',
                    legend_title_font=dict(color='#adb5bd'),
                    legend_font=dict(color='#adb5bd'),
                    legend=dict(yanchor="top",xanchor="left", y=1, x=0),
                    legend_bgcolor="rgba(33, 37, 41, 0)",
                    xaxis_title=None
                )

                dashboard_cloud_providers_evolution.update_yaxes(ticksuffix='  ')  

                # Ajout du cadre blanc
                dashboard_cloud_providers_evolution.update_layout(
                    shapes=[
                        dict(
                            type='rect',
                            xref='paper', yref='paper',
                            x0=0, y0=0, x1=1, y1=1,
                            line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                            fillcolor="rgba(255,255,255,0)"  # Fond transparent
                )])

                # Ajouter un espace entre le début et la fin des linecharts (5% de chaque côté)
                dashboard_cloud_providers_evolution.update_layout(
                    xaxis_range=[
                        dashboard_df_cloud_providers_line['date'].min() - pd.DateOffset(days=10), 
                        dashboard_df_cloud_providers_line['date'].max() + pd.DateOffset(days=30)
                        ],
                    yaxis_range=[
                        0, 
                        dashboard_df_cloud_providers_line['count'].max() + dashboard_df_cloud_providers_line['count'].max()*10/100
                ])
                
                dashboard_cloud_providers_evolution.update_traces(
                    hovertemplate='<b>%{customdata[0]}</b><br><br>Date: %{x} <br>Demandé dans %{customdata[1]} offres d\'emploi<extra></extra>'
                )        

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_cloud_providers_evolution_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_cloud_providers_evolution.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Linechart providers cloud')


            # -----------------------------------
            # ------ Donut providers cloud ------
            # -----------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    technologie, 
                    SUM(occurrences) as occurrences 
                FROM jobsoccurrences
                WHERE technologie IN ('AWS', 'GCP', 'Azure') 
                    AND job_search = %s
                    AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '1 month'
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                GROUP BY technologie       
            """, (job_variable, region))

            dashboard_donut_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'provider',
                    'count'
            ])

            dashboard_donut_df['color'] = dashboard_donut_df['provider'].map(
                {
                    'AWS': '#ff9900', 
                    'Azure': '#008ad7', 
                    'GCP': '#db4437'
            })
            
            dashboard_donut_df['percentage'] = dashboard_donut_df['count'].apply(
                lambda x: round(x / dashboard_donut_df['count'].sum() * 100, 1)
            )


            if dashboard_donut_df['control'].sum() != 0:

                # Graphique
                dashboard_donut_providers_cloud = go.Figure(
                    data=[
                        go.Pie(
                            labels=dashboard_donut_df['provider'], 
                            values=dashboard_donut_df['count'], 
                            hole=0.6,
                            marker=dict(colors=dashboard_donut_df['color']),
                            textfont=dict(color='white', size=14),
                            hovertemplate="%{label} a été demandé dans <b>%{value}</b> offres d'emploi<extra></extra>"
                )])

                # Mise en forme du donut chart
                dashboard_donut_providers_cloud.update_layout(
                    title=None,
                    legend=dict(yanchor="middle",y=0.5,xanchor="center",x=0.5),
                    legend_font=dict(size=14, color="#adb5bd"),
                    legend_bgcolor="rgba(33, 37, 41, 0)",  # Fond transparent pour la légende
                    legend_bordercolor="rgba(33, 37, 41, 0)",  # Bordure transparente pour la légende
                    paper_bgcolor="#212529",  # Fond du graphique
                    margin=dict(t=30, b=60, r=20, l=20),
                    width=500,
                    height=370
                )
            
                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_donut_providers_cloud_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_donut_providers_cloud.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Donut providers cloud')


            #--------------------------------------
            #------ Table top technos 3 mois ------
            #--------------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    technologie, 
                    SUM(occurrences) AS offres
                FROM jobsoccurrences 
                WHERE job_search = %s
                AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                AND date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'                             
                GROUP BY technologie
                ORDER BY SUM(occurrences) DESC
                LIMIT 32
            """, (job_variable, region))

            dashboard_techno_table_3m_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'techno',
                    'occurrences'
            ])

            if dashboard_techno_table_3m_df['control'].sum() != 0:

                # Graphique
                dashboard_techno_3m_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [400,100],
                            header=dict(
                                values=[
                                    'Technologie', 
                                    'Demandes'
                                    ],
                            line_color='#212529',
                            fill_color='#3c4044',
                            height=30,
                            font=dict(size=14, color='white'),
                            ),
                            cells=dict(
                                values=[
                                    dashboard_techno_table_3m_df['techno'].to_list(), 
                                    dashboard_techno_table_3m_df['occurrences'].to_list()
                                    ],
                            line_color='#212529',
                            fill_color='#343a40',
                            align=['center'],
                            height=25,
                            font=dict(size=14, color='white'),   
                ))])
                
                # Mise à jour du layout
                dashboard_techno_3m_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_techno_table_3m_df['techno'])*25)+30,  # Ajustement auto de la hauteur
                    width=500
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_techno_3m_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_techno_3m_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Table top technos 3 mois')


            #-------------------------------------
            #------ Table top villes 3 mois ------
            #-------------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    city, 
                    COUNT(*) AS offres
                FROM jobs
                WHERE job_search = %s
                AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
                AND date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'     
                GROUP BY city
                ORDER BY COUNT(*) DESC
                LIMIT 32
            """, (job_variable, region))

            dashboard_ville_table_3m_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'ville',
                    'offres'
            ])

            if dashboard_ville_table_3m_df['control'].sum() != 0:

                # Création de la table
                dashboard_ville_3m_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [400,100],
                            header=dict(
                                values=[
                                    'Ville', 
                                    "Offres"
                                    ],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30,
                                font=dict(size=14, color='white'),
                                ),
                        cells=dict(
                            values=[
                                dashboard_ville_table_3m_df['ville'].to_list(), 
                                dashboard_ville_table_3m_df['offres'].to_list()
                                ],
                            line_color='#212529',
                            fill_color='#343a40',
                            align=['center'],
                            height=25,
                            font=dict(size=14, color='white'),   
                ))])

                # Mise à jour du layout
                dashboard_ville_3m_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_ville_table_3m_df['ville'])*25)+30,  # Ajustement auto de la hauteur
                    width=500
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_ville_3m_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_ville_3m_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Table top villes 3 mois')


            #------------------------------------------
            #------ Table top entreprises 3 mois ------
            #------------------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    company_name, 
                    COUNT(*) AS offres
                FROM jobs
                WHERE job_search = %s
                AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW()) 
                AND date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'     
                GROUP BY company_name
                ORDER BY COUNT(*) DESC
                LIMIT 32
            """, (job_variable, region))

            dashboard_entreprise_table_3m_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'entreprise',
                    'offres'
            ])

            if dashboard_entreprise_table_3m_df['control'].sum() != 0:

                # Création de la table
                dashboard_entreprise_3m_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [400,100],
                            header=dict(
                                values=[
                                    'Entreprise', 
                                    "Offres"
                                    ],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30,
                                font=dict(size=14, color='white'),
                                ),
                            cells=dict(
                                values=[
                                    dashboard_entreprise_table_3m_df['entreprise'].to_list(), 
                                    dashboard_entreprise_table_3m_df['offres'].to_list()
                                    ],
                                line_color='#212529',
                                fill_color='#343a40',
                                align=['center'],
                            height=25,
                            font=dict(size=14, color='white'),   
                    ))])

                # Mise à jour du layout
                dashboard_entreprise_3m_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_entreprise_table_3m_df['entreprise'])*25)+30,  # Ajustement auto de la hauteur
                    width=500
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_entreprise_3m_table_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_entreprise_3m_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Table top entreprises 3 mois')


            #--------------------------------------------
            #------ Table top technologies 12 mois ------
            #--------------------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    technologie, 
                    SUM(occurrences) AS offres
                FROM jobsoccurrences               
                WHERE job_search = %s
                AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())             
                AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'                  
                GROUP BY technologie
                ORDER BY SUM(occurrences) DESC
                LIMIT 32
            """, (job_variable, region))

            dashboard_techno_table_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control', 
                    'techno', 
                    'occurrences'
            ])

            if dashboard_techno_table_df['control'].sum() != 0:
                # Création de la table
                dashboard_techno_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [400,100],
                            header=dict(
                                values=[
                                    'Technologie', 
                                    'Demandes'
                                    ],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30,
                                font=dict(size=14, color='white'),
                                ),
                            cells=dict(
                                values=[
                                    dashboard_techno_table_df['techno'].to_list(), 
                                    dashboard_techno_table_df['occurrences'].to_list()
                                    ],
                                line_color='#212529',
                                fill_color='#343a40',
                                align=['center'],
                                height=25,
                                font=dict(size=14, color='white'), 
                ))])
                
                # Mise à jour du layout
                dashboard_techno_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_techno_table_df['techno'])*25)+30, # Ajustement auto de la hauteur  
                    width=500 
                )                            

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_techno_table_12m_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_techno_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Table top technologies 12 mois')


            #--------------------------------------
            #------ Table top villes 12 mois ------
            #--------------------------------------
            
            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    city, 
                    COUNT(*) AS offres
                FROM jobs
                WHERE job_search = %s
                AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW()) 
                AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
                GROUP BY city
                ORDER BY COUNT(*) DESC
                LIMIT 32
            """, (job_variable, region))

            dashboard_ville_table_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'ville',
                    'offres'
            ])

            if dashboard_ville_table_df['control'].sum() != 0:        

                # Graphique
                dashboard_ville_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [400,100],
                            header=dict(
                                    values=[
                                        'Ville', 
                                        "Offres"
                                    ],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30,
                                font=dict(size=14, color='white'),
                                ),
                            cells=dict(
                                values=[
                                    dashboard_ville_table_df['ville'].to_list(), 
                                    dashboard_ville_table_df['offres'].to_list()
                                    ],
                                line_color='#212529',
                                fill_color='#343a40',
                                align=['center'],
                                height=25,
                                font=dict(size=14, color='white'),
                ))])

                dashboard_ville_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height=(len(dashboard_ville_table_df['offres'])*25)+30,
                    width=500
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_ville_table_12m_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_ville_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Table top villes 12 mois')

            #--------------------------------------------
            #------ Table top entreprises 12 mois ------
            #--------------------------------------------

            cursor.execute(f"""
                SELECT 
                    1 AS control,
                    company_name, 
                    COUNT(*) AS offres
                FROM jobs
                WHERE job_search = %s
                AND region {condition}
                AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW()) 
                AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
                GROUP BY company_name
                ORDER BY COUNT(*) DESC
                LIMIT 32
            """, (job_variable, region))

            dashboard_entreprise_table_df = pd.DataFrame(
                cursor.fetchall(), 
                columns=[
                    'control',
                    'entreprise',
                    'offres'
            ])

            if dashboard_entreprise_table_df['control'].sum() != 0:

                # Graphique
                dashboard_entreprise_table = go.Figure(
                    data=[
                        go.Table(
                            columnwidth = [400,100],
                            header=dict(
                                values=['Entreprise', "Offres"],
                                line_color='#212529',
                                fill_color='#3c4044',
                                height=30,
                                font=dict(size=14, color='white'),
                                ),
                            cells=dict(
                                values=[
                                    dashboard_entreprise_table_df['entreprise'].to_list(), 
                                    dashboard_entreprise_table_df['offres'].to_list()
                                    ],
                                line_color='#212529',
                                fill_color='#343a40',
                                align=['center'],
                                height=25,
                                font=dict(size=14, color='white'),
                ))])

                dashboard_entreprise_table.update_layout(
                    margin=dict(t=0, b=0, r=0, l=0),
                    paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
                    plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
                    dragmode=False,
                    font=dict(color='white', size=14),
                    height = (len(dashboard_entreprise_table_df['entreprise'])*25)+30,
                    width=500
                )

                # Upload du fichier sur le bucket s3
                object_name = f'dashboard_entreprise_table_12m_{job_variable_for_file}_{region_for_file}.png'

                # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
                image_path = os.path.join(tmp_directory, object_name)
                dashboard_entreprise_table.write_image(image_path, format='png', engine='kaleido')

                s3.upload_file(image_path, bucket_name, f'static/charts/{object_name}')

                print(f'{job_variable}, {region_for_file} : Table top entreprises 12 mois')

    except Exception as e:
        print(f"Erreur: {str(e)}")
        raise e
