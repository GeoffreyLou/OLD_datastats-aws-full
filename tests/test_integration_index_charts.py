import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import psycopg2

# Retrait des warnings dans les logs lambda
import warnings
warnings.filterwarnings("ignore")


try:

    # -----------------------
    # ------ Ouverture ------
    # -----------------------


    # Informations de connexion à la BDD : 
    conn = psycopg2.connect(
        database="data_jobs", 
        user="postgres", 
        password="----", 
        host="localhost", 
        port="5432"
    )
    
    # Création du curseur
    cursor = conn.cursor()


    # --------------------------
    # --------------------------
    # ------ Index Charts ------
    # --------------------------
    # --------------------------


    # ----------------------------------
    # ------ Line chart job count ------
    # ----------------------------------


    # Requpete SQL
    cursor.execute("""
        SELECT DATE(DATE_TRUNC('month', date_of_search)), count(*) 
        FROM jobs 
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY DATE(DATE_TRUNC('month', date_of_search))           
    """)
    index_jobs_count_df = pd.DataFrame(cursor.fetchall(), columns=['date', 'count'])


    # Graphique
    index_jobs_count_df = index_jobs_count_df.groupby('date')['count'].sum().reset_index()

    index_jobs_count_df['date'] = pd.to_datetime(index_jobs_count_df['date'])
    index_jobs_count_df.sort_values(by=['date'], ascending=False, inplace=True)

    index_total_jobs_chart = px.line(
        index_jobs_count_df,
        x='date',
        y='count',
        custom_data=['count'],
        height=375,
        width=700
    )

    index_total_jobs_chart.update_traces(line=dict(color="#54a24b")) # e377c2 joli rose

    index_total_jobs_chart.update_layout(
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

    index_total_jobs_chart.update_xaxes(
        dtick="M1",
        tickformat="%B %Y"
    ) 

    index_total_jobs_chart.update_yaxes(ticksuffix='  ')  

    # Ajout du cadre blanc
    index_total_jobs_chart.update_layout(
        shapes=[
            dict(
                type='rect',
                xref='paper', yref='paper',
                x0=0, y0=0, x1=1, y1=1,
                line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                fillcolor="rgba(255,255,255,0)"  # Fond transparent
    )])

    index_total_jobs_chart.update_traces(
        mode='lines+text', 
        text=index_jobs_count_df['count'], 
        textposition='top center', 
        textfont=dict(color='white'),
        hovertemplate='%{customdata[0]} offres d\'emploi pour le mois de %{x} <extra></extra>'
    )

    index_total_jobs_chart.update_layout(
        xaxis_range=[index_jobs_count_df['date'].min() - pd.DateOffset(days=5), index_jobs_count_df['date'].max() + pd.DateOffset(days=30)],
        yaxis_range=[0, index_jobs_count_df['count'].max() + index_jobs_count_df['count'].max()*10/100]
    )


    object_name = 'index_line_chart_job_count.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_total_jobs_chart.write_image(object_name, format='png', engine='kaleido')

    print('Line chart job count terminé')


    # -----------------------------------
    # ------ Donut providers cloud ------
    # -----------------------------------


    # Requête SQL
    cursor.execute("""
        SELECT 
            technologie, 
            SUM(occurrences) as occurrences 
        FROM jobsoccurrences
        WHERE technologie IN ('AWS', 'GCP', 'Azure')
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '1 month'
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        GROUP BY technologie;   
    """)
    index_donut_df = pd.DataFrame(cursor.fetchall(), columns=['provider', 'count'])

    index_donut_df['color'] = index_donut_df['provider'].map({'AWS': '#ff9900', 'Azure': '#008ad7', 'GCP': '#db4437'})
    index_donut_df['percentage'] = index_donut_df['count'].apply(lambda x: round(x / index_donut_df['count'].sum() * 100, 1))

    # Graphique
    index_providers_donut_chart = go.Figure(
        data=[
            go.Pie(
                labels=index_donut_df['provider'], 
                values=index_donut_df['count'], 
                hole=0.6,
                marker=dict(colors=index_donut_df['color']),
                textfont=dict(color='white', size=14),
                hovertemplate="%{label} a été demandé dans <b>%{value}</b> offres d'emploi<extra></extra>"
    )])

    # Mise en forme du donut chart
    index_providers_donut_chart.update_layout(
        title=None,
        legend=dict(yanchor="middle",y=0.5,xanchor="center",x=0.5),
        legend_font=dict(size=14, color="#adb5bd"),
        legend_bgcolor="rgba(33, 37, 41, 0)",  # Fond transparent pour la légende
        legend_bordercolor="rgba(33, 37, 41, 0)",  # Bordure transparente pour la légende
        paper_bgcolor="#212529",  # Fond du graphique
        margin=dict(t=30, b=60, r=20, l=20),
        width=450,
        height=330
    )
        

    object_name = 'index_donut_providers_cloud.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_providers_donut_chart.write_image(object_name, format='png', engine='kaleido')

    print('Index Donut chart providers cloud terminé')


    # ---------------------------------------
    # ------ Linechart providers cloud ------
    # ---------------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT DATE(DATE_TRUNC('month', date_of_search)), technologie, SUM(occurrences) as count
        FROM jobsoccurrences 
        WHERE technologie IN ('AWS', 'GCP', 'Azure') 
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY DATE(DATE_TRUNC('month', date_of_search)), technologie 
        ORDER BY DATE(DATE_TRUNC('month', date_of_search)) DESC, SUM(occurrences)  DESC              
    """)
    index_df_cloud_providers_line = pd.DataFrame(cursor.fetchall(), columns=['date', 'provider', 'count'])


    # Graphique
    color_map= {
        'AWS': '#ff9900', 
        'Azure': '#008ad7', 
        'GCP': '#db4437'
    }
    
    index_cloud_providers_evolution = px.line(
        index_df_cloud_providers_line, 
        'date', 
        'count',
        custom_data=['provider', 'count'],
        color='provider', 
        color_discrete_map=color_map,
    )

    index_cloud_providers_evolution.update_xaxes(
        dtick="M1",
        tickformat="%B %Y"
    )

    index_cloud_providers_evolution.update_layout(
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
        xaxis_title=None,
        height=330,
        width=660
    )

    index_cloud_providers_evolution.update_yaxes(ticksuffix='  ')  

    # Ajout du cadre blanc
    index_cloud_providers_evolution.update_layout(
        shapes=[
            dict(
                type='rect',
                xref='paper', yref='paper',
                x0=0, y0=0, x1=1, y1=1,
                line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                fillcolor="rgba(255,255,255,0)"  # Fond transparent
    )])

    # Ajouter un espace entre le début et la fin des linecharts (5% de chaque côté)
    index_cloud_providers_evolution.update_layout(
        xaxis_range=[index_df_cloud_providers_line['date'].min() - pd.DateOffset(days=10), index_df_cloud_providers_line['date'].max() + pd.DateOffset(days=30)],
        yaxis_range=[0, index_df_cloud_providers_line['count'].max() + index_df_cloud_providers_line['count'].max()*10/100]
    )
    
    index_cloud_providers_evolution.update_traces(
        hovertemplate='<b>%{customdata[0]}</b><br><br>Date: %{x} <br>Demandé dans %{customdata[1]} offres d\'emploi<extra></extra>'
    )        

    object_name = 'index_linechart_providers_cloud.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_cloud_providers_evolution.write_image(object_name, format='png', engine='kaleido')

    print('Line chart providers cloud terminé')


    # --------------------------------
    # ------ Barchart job count ------
    # --------------------------------


    # Requête SQL
    cursor.execute("""
        SELECT job_search, COUNT(*) AS count
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY job_search
        ORDER BY COUNT(*) ASC
    """)
    index_jobs_count_barchart_df = pd.DataFrame(cursor.fetchall(), columns=['job_search', 'count'])
    index_jobs_count_barchart_df['job_search'] = index_jobs_count_barchart_df['job_search'].apply(lambda x: x.title())

    
    index_job_count_barchart = px.bar(
        index_jobs_count_barchart_df, 
        y='job_search', 
        x='count',
        text='count',
        color_discrete_sequence=['#54a24b'],
        height=450,
        width=700
    )

    index_job_count_barchart.update_traces(
        textposition="outside",
        textfont=dict(size=15), 
        cliponaxis=True, 
        textangle=0, 
        textfont_color="white",
        marker_line_width=0
    )

    index_job_count_barchart.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        xaxis=dict(visible=False),
        yaxis=dict(visible=True, color="#adb5bd"),  
        xaxis_showgrid=False,  # Masquer la grille x-axis pour une meilleure apparence
        yaxis_showgrid=False,  # Masquer la grille y-axis pour une meilleure apparence
        title=None,  # Titre du graphique
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        xaxis_title=None,
        yaxis_title=None
    )   

    index_job_count_barchart.update_yaxes(ticksuffix='  ')

    index_job_count_barchart.update_layout(
        xaxis_range=[0, index_jobs_count_barchart_df['count'].max() + index_jobs_count_barchart_df['count'].max()*20/100]
    )

    # Ajout du cadre blanc
    index_job_count_barchart.update_layout(
        shapes=[
            dict(
                type='rect',
                xref='paper', yref='paper',
                x0=0, y0=0, x1=1, y1=1,
                line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                fillcolor="rgba(255,255,255,0)"  # Fond transparent
    )])
    
    index_job_count_barchart.update_traces(hovertemplate='<b>%{y}</b>: %{x} offres d\'emploi<extra></extra>')

    object_name = 'index_barchart_job_count.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_job_count_barchart.write_image(object_name, format='png', engine='kaleido')

    print('Barchart job count terminé')


    # -----------------------------------
    # ------ Barchart region count ------
    # -----------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT region, COUNT(*) AS count
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY region
        ORDER BY COUNT(*) ASC
    """)
    index_region_count_df = pd.DataFrame(cursor.fetchall(), columns=['region', 'count'])

    # Graphique        
    index_region_job_count_barchart = px.bar(
        index_region_count_df, 
        y='region', 
        x='count',
        text='count',
        color_discrete_sequence=['#54a24b'],
        orientation="h",
        height=450,
        width=700
    )

    index_region_job_count_barchart.update_traces(
        textposition="outside",
        textfont=dict(size=15), 
        cliponaxis=True,
        textangle=0, 
        textfont_color="white",
        marker_line_width=0
    )

    index_region_job_count_barchart.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),  
        xaxis=dict(visible=False),
        yaxis=dict(title_text="Nombre d'offres d'emploi", showline=True, color="#adb5bd"),  
        xaxis_showgrid=False,  # Masquer la grille x-axis pour une meilleure apparence
        yaxis_showgrid=False,  # Masquer la grille y-axis pour une meilleure apparence
        title=None,  # Titre du graphique
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        xaxis_title=None,
        yaxis_title=None
    )   

    index_region_job_count_barchart.update_yaxes(ticksuffix='  ')
    
    index_region_job_count_barchart.update_layout(
        xaxis_range=[0, index_region_count_df['count'].max() + index_region_count_df['count'].max()*20/100]
    )

    # Ajout du cadre blanc
    index_region_job_count_barchart.update_layout(
        shapes=[
            dict(
                type='rect',
                xref='paper', yref='paper',
                x0=0, y0=0, x1=1, y1=1,
                line=dict(color="#5b6d82", width=2),  # Couleur et épaisseur du cadre blanc
                fillcolor="rgba(255,255,255,0)"  # Fond transparent
    )])
    
    index_region_job_count_barchart.update_traces(hovertemplate='%{x} offres d\'emploi en %{y} <extra></extra>')
        
    object_name = 'index_barchart_region_count.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_region_job_count_barchart.write_image(object_name, format='png', engine='kaleido')

    print('Barchart region count terminé')


    # ----------------------------------
    # ------ Tableau technologies ------
    # ----------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT 
            technologie, 
            SUM(occurrences) AS offres
        FROM jobsoccurrences
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY technologie
        ORDER BY SUM(occurrences) DESC
        LIMIT 10
    """)
    index_techno_table_df = pd.DataFrame(cursor.fetchall(), columns=['techno', 'occurrences'])
    
    # Graphique
    index_techno_table = go.Figure(
        data=[go.Table(
            columnwidth = [400,100],
            header=dict(
                values=['Technologie', 'Demandes'],
                line_color='#212529',
                fill_color='#3c4044',
                height=30,
                font=dict(size=14, color='white'),
                align=['center', 'center']),
            cells=dict(
                values=[index_techno_table_df['techno'].to_list(), 
                index_techno_table_df['occurrences'].to_list()],
                line_color='#212529',
                fill_color='#343a40',
                height=30,
                font=dict(size=14, color='white'),
                align=['center', 'center'])
    )])

    index_techno_table.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=330,
        width=500
    )


    object_name = 'index_tableau_top_technos.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_techno_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau technologies terminé')


    # ----------------------------
    # ------ Tableau villes ------
    # ----------------------------

    # Requête SQL
    cursor.execute("""
        SELECT city, COUNT(*) AS offres
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY city
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    index_ville_table_df = pd.DataFrame(cursor.fetchall(), columns=['ville', 'offres'])

    # Graphique
    index_ville_table = go.Figure(data=[go.Table(
        columnwidth = [400,100],
        header=dict(values=['Ville', "Offres"],
                line_color='#212529',
                fill_color='#3c4044',
                height=30,
                font=dict(size=14, color='white'),
                align=['center', 'center']),
        cells=dict(values=[index_ville_table_df['ville'].to_list(), 
                        index_ville_table_df['offres'].to_list()],
            line_color='#212529',
            fill_color='#343a40',
            height=30,
            font=dict(size=14, color='white'),
            align=['center', 'center'])
    )])

    index_ville_table.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=330,
        width=500
    )

    object_name = 'index_tableau_top_villes.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_ville_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau villes terminé')


    # ---------------------------------
    # ------ Tableau entreprises ------
    # ---------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT company_name, COUNT(*) AS offres
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY company_name
        ORDER BY COUNT(*) DESC
        LIMIT 10
    """)
    index_entreprise_table_df = pd.DataFrame(cursor.fetchall(), columns=['entreprise', 'offres'])

    # Graphique
    index_entreprise_table = go.Figure(data=[go.Table(
        columnwidth = [400,100],
        header=dict(values=['Entreprise', "Offres"],
                    line_color='#212529',
                    fill_color='#3c4044',
                    height=30,
                    font=dict(size=14, color='white'),
                    align=['center', 'center']),
        cells=dict(values=[index_entreprise_table_df['entreprise'].to_list(), 
                        index_entreprise_table_df['offres'].to_list()],
                line_color='#212529',
                fill_color='#343a40',
                height=30,
                font=dict(size=14, color='white'),
                align=['center', 'center'])
    )])

    index_entreprise_table.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=330,
        width=500
    )

    object_name = 'index_tableau_top_entreprises.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_entreprise_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau entreprises terminé')


    # -----------------------------------
    # ------ Tableau salaire moyen ------
    # -----------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT
            job_search,
            COALESCE((CAST(CAST(AVG(lower_salary) AS INT) AS TEXT) || ' €'), 'No data') AS tranche_min,
            COALESCE((CAST(CAST(AVG(upper_salary) AS INT) AS TEXT) || ' €'), 'No data') AS tranche_max
        FROM jobs
        WHERE job_search IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY job_search
        ORDER BY job_search
    """)
    index_avg_salary_df = pd.DataFrame(cursor.fetchall(), columns=['job_search', 'tranche_min', 'tranche_max'])

    # Graphique
    index_avg_salary_table = go.Figure(
        data=[go.Table(
            columnwidth = [250,150],
            header=dict(
                values=['Emploi', 'Min', 'Max'],
                line_color='#212529',
                fill_color='#3c4044',
                height=30,),
            cells=dict(
                values=[index_avg_salary_df['job_search'].to_list(), 
                index_avg_salary_df['tranche_min'].to_list(), 
                index_avg_salary_df['tranche_max'].to_list()],
                line_color='#212529',
                fill_color='#343a40',
                align=['center'],
                height=30,
                font_color=['white', ['dimgray' if str(x.strip()) == "No data" else "white" for x in list(index_avg_salary_df['tranche_min'])]])
    )])

    index_avg_salary_table.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=len(index_avg_salary_df['job_search'].unique())*30 + 30,
        width=700,
    )

    object_name = 'index_tableau_salaire_moyen.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_avg_salary_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau salaires moyens terminé')


    # ------------------------------------
    # ------ Tableau salaire médian ------
    # ------------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT 
            job_search,
            COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY lower_salary) AS INT) AS TEXT) || ' €'), 'No data') AS tranche_min,
            COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY upper_salary) AS INT) AS TEXT) || ' €'), 'No data' ) AS tranche_max
        FROM jobs
        WHERE job_search IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY job_search
        ORDER BY job_search
    """)
    index_med_salary_df = pd.DataFrame(cursor.fetchall(), columns=['job_search', 'tranche_min', 'tranche_max'])   

    # Graphique
    index_med_salary_table = go.Figure(
        data=[
            go.Table(
                columnwidth = [250,150],
                header=dict(
                    values=['Emploi', 'Min', 'Max'],
                    line_color='#212529',
                    fill_color='#3c4044',
                    height=30,),
                cells=dict(
                    values=[
                        index_med_salary_df['job_search'].to_list(), 
                        index_med_salary_df['tranche_min'].to_list(), 
                        index_med_salary_df['tranche_max'].to_list()],
                    line_color='#212529',
                    fill_color='#343a40',
                    align=['center'],
                    height=30,
                    font_color=['white', ['dimgray' if str(x.strip()) == "No data" else "white" for x in list(index_med_salary_df['tranche_min'])]
    ]))])

    index_med_salary_table.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=len(index_med_salary_df['job_search'].unique())*30 + 30,
        width=700
    )

    object_name = 'index_tableau_salaire_median.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    index_med_salary_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau salaires médians terminé')



    # ------------------------------
    # ------------------------------
    # ------ Dashboard Charts ------
    # ------------------------------
    # ------------------------------



    # --------------------------------------
    # ------ Linechart jobs evolution ------
    # --------------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT DATE(DATE_TRUNC('month', date_of_search)), job_search, count(*) 
        FROM jobs 
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY DATE(DATE_TRUNC('month', date_of_search)), job_search
        ORDER BY DATE(DATE_TRUNC('month', date_of_search)), job_search          
    """)
    dashboard_jobs_count_df = pd.DataFrame(cursor.fetchall(), columns=['date', 'job_search', 'count'])


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
    
    for i in dashboard_jobs_count_df['job_search'].unique():
        dashboard_total_jobs_chart.add_trace(go.Scatter( 
        x=dashboard_jobs_count_df['date'][dashboard_jobs_count_df['job_search'] == i ],
        y=dashboard_jobs_count_df['count'][dashboard_jobs_count_df['job_search'] == i ],
        mode="lines+text",
        text=dashboard_jobs_count_df['count'][dashboard_jobs_count_df['job_search'] == i ],
        textposition="top center",
        textfont=dict(color='white'),
        customdata=dashboard_jobs_count_df[['job_search', 'count']][dashboard_jobs_count_df['job_search'] == i ],
        name = i,
        line_color=line_color_map[i],
        hovertemplate='%{customdata[1]} offres d\'emploi %{customdata[0]} pour le mois de %{x} <extra></extra>'
    ))
        
    dashboard_total_jobs_chart.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        xaxis=dict(title_text=None, showline=True, color="#adb5bd"),
        yaxis=dict(visible=True, title_text=None, color="#adb5bd"),   
        xaxis_showgrid=False,  # Masquer la grille x-axis pour une meilleure apparence
        yaxis_showgrid=False,  # Masquer la grille y-axis pour une meilleure apparence
        title=None,  # Titre du graphique
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        xaxis_title=None,
        height=375,
        width=1100,
        showlegend=True,
        legend_font=dict(size=14, color="#adb5bd")
    )     
    
    dashboard_total_jobs_chart.update_xaxes(
        dtick="M1",
        tickformat="%B %Y"
    ) 
    
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
                
    dashboard_total_jobs_chart.update_layout(
        xaxis_range=[dashboard_jobs_count_df['date'].min() - pd.DateOffset(days=5), dashboard_jobs_count_df['date'].max() + pd.DateOffset(days=30)],
        yaxis_range=[0, dashboard_jobs_count_df['count'].max()+dashboard_jobs_count_df['count'].max()*10/100]
    )
    
    object_name = 'dashboard_linechart_jobs_evolution.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_total_jobs_chart.write_image(object_name, format='png', engine='kaleido')

    print('Linechart jobs evolution terminé')


    # -------------------------------------
    # ------ Top 20 positif technos ------
    # -------------------------------------

    # Requête SQL
    cursor.execute("""
        WITH data_for_lag AS (
            (SELECT
                1 AS lag_value,
                technologie,
                SUM(occurrences) AS total_occurrences
            FROM jobsoccurrences
            WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
            AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
            AND technologie IS NOT NULL
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
            technologie,
            total_occurrences,
            previous_total_occurrences,
            COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) AS evolution
        FROM lag_aggregation
        ORDER BY evolution DESC
        LIMIT 20
    """)
    dashboard_top20_plus_df = pd.DataFrame(cursor.fetchall(), columns=['technologie', 'total_occurrences', 'previous_total_occurrences', 'evolution'])
    dashboard_top20_plus_df = dashboard_top20_plus_df.sort_values(by='evolution', ascending=True)

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
            marker_color="#54a24b",
    ))

    # Mettre en forme le graphique
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
        xaxis_range=[0, dashboard_top20_plus_df['evolution'].max() + dashboard_top20_plus_df['evolution'].max()*20/100]
    )

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

    object_name = 'dashboard_top20_plus_chart.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_top20_plus_chart.write_image(object_name, format='png', engine='kaleido')

    print('Top 20 évolutions positives terminé')


    # --------------------------------------------------
    # ------ Tableau top 20 positif technos table ------
    # --------------------------------------------------

    # Requête SQL
    cursor.execute("""
        WITH data_for_lag AS (
            (SELECT
                1 AS lag_value,
                technologie,
                SUM(occurrences) AS total_occurrences
            FROM jobsoccurrences
            WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
            AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
            AND technologie IS NOT NULL
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
            technologie,
            total_occurrences,
            previous_total_occurrences,
            '+ ' || COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) || ' %' AS evolution
        FROM lag_aggregation
        ORDER BY COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) DESC
        LIMIT 20
    """)
    dashboard_top20_plus_table_df = pd.DataFrame(cursor.fetchall(), columns=['technologie', 'total_occurrences', 'previous_total_occurrences', 'evolution'])

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
        width=700
    )

    object_name = 'dashboard_top20_plus_table.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_top20_plus_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau top 20 évolutions positives terminé')


    # ------------------------------------
    # ------ Top 20 négatif technos ------
    # ------------------------------------

    # Requête SQL
    cursor.execute("""
        WITH data_for_lag AS (
            (SELECT
                1 AS lag_value,
                technologie,
                SUM(occurrences) AS total_occurrences
            FROM jobsoccurrences
            WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
            AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
            AND technologie IS NOT NULL
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
            WHERE total_occurrences > 50
            ORDER BY 
                technologie, 
                lag_value
        )
        SELECT
            technologie,
            total_occurrences,
            previous_total_occurrences,
            COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) AS evolution
        FROM lag_aggregation
        ORDER BY evolution ASC
        LIMIT 20
    """)
    dashboard_top_20_moins_df = pd.DataFrame(cursor.fetchall(), columns=['technologie', 'total_occurrences', 'previous_total_occurrences', 'evolution'])
    dashboard_top_20_moins_df = dashboard_top_20_moins_df.sort_values(by='evolution', ascending=True)

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

    object_name = 'dashboard_top20_moins_chart.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_top20_moins_chart.write_image(object_name, format='png', engine='kaleido')

    print('Top 20 évolutions négatives terminé')


    # ------------------------------------------
    # ------ Top 20 négatif technos table ------
    # ------------------------------------------

    # Requête SQL
    cursor.execute("""
        WITH data_for_lag AS (
            (SELECT
                1 AS lag_value,
                technologie,
                SUM(occurrences) AS total_occurrences
            FROM jobsoccurrences
            WHERE date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '6 month'
            AND date_of_search < DATE_TRUNC('month', NOW()) - INTERVAL '3 month'
            AND technologie IS NOT NULL
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
            technologie,
            total_occurrences,
            previous_total_occurrences,
            COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) AS evolution
        FROM lag_aggregation
        ORDER BY COALESCE(ROUND(((total_occurrences - previous_total_occurrences) / previous_total_occurrences) *100), 0) ASC
        LIMIT 20
    """)
    dashboard_top_20_moins_df = pd.DataFrame(cursor.fetchall(), columns=['technologie', 'total_occurrences', 'previous_total_occurrences', 'evolution'])
    dashboard_top_20_moins_df = dashboard_top_20_moins_df.sort_values(by=['evolution'], ascending=False)
    dashboard_top_20_moins_df['evolution'] = dashboard_top_20_moins_df['evolution'].astype(str).apply(
        lambda x: x + ' %'
    )

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


    object_name = 'dashboard_top20_moins_table.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_top20_moins_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau top 20 évolutions négatives terminé')


    # ---------------------------------------
    # ------ Linechart providers cloud ------
    # ---------------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT DATE(DATE_TRUNC('month', date_of_search)), technologie, SUM(occurrences) as count
        FROM jobsoccurrences 
        WHERE technologie IN ('AWS', 'GCP', 'Azure') 
        AND region IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE(DATE_TRUNC('month', NOW()))
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY DATE(DATE_TRUNC('month', date_of_search)), technologie 
        ORDER BY DATE(DATE_TRUNC('month', date_of_search)) DESC, SUM(occurrences)  DESC              
    """)
    dashboard_df_cloud_providers_line = pd.DataFrame(cursor.fetchall(), columns=['date', 'provider', 'count'])


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
        xaxis_range=[dashboard_df_cloud_providers_line['date'].min() - pd.DateOffset(days=10), dashboard_df_cloud_providers_line['date'].max() + pd.DateOffset(days=30)],
        yaxis_range=[0, dashboard_df_cloud_providers_line['count'].max() + dashboard_df_cloud_providers_line['count'].max()*10/100]
    )
    
    dashboard_cloud_providers_evolution.update_traces(
        hovertemplate='<b>%{customdata[0]}</b><br><br>Date: %{x} <br>Demandé dans %{customdata[1]} offres d\'emploi<extra></extra>'
    )        

    object_name = 'dashboard_cloud_providers_evolution.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_cloud_providers_evolution.write_image(object_name, format='png', engine='kaleido')

    print('Dashboard line chart providers cloud terminé')


    # -----------------------------------
    # ------ Donut providers cloud ------
    # -----------------------------------


    # Requête SQL
    cursor.execute("""
        SELECT 
            technologie, 
            SUM(occurrences) as occurrences 
        FROM jobsoccurrences
        WHERE technologie IN ('AWS', 'GCP', 'Azure')
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '1 month'
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        GROUP BY technologie;   
    """)
    dashboard_donut_df = pd.DataFrame(cursor.fetchall(), columns=['provider', 'count'])

    dashboard_donut_df['color'] = dashboard_donut_df['provider'].map(
        {
            'AWS': '#ff9900', 
            'Azure': '#008ad7', 
            'GCP': '#db4437'
    })
    
    dashboard_donut_df['percentage'] = dashboard_donut_df['count'].apply(
        lambda x: round(x / dashboard_donut_df['count'].sum() * 100, 1)
    )

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
        

    object_name = 'dashboard_donut_providers_cloud.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_donut_providers_cloud.write_image(object_name, format='png', engine='kaleido')

    print('Donut chart providers cloud terminé')



    # -----------------------------------------------
    # ------ Tableau salaire médian par région ------
    # -----------------------------------------------


    # Requête SQL
    cursor.execute(f"""
            SELECT 
            region,
            COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY lower_salary) AS INT) AS TEXT) || ' €'), 'No data') AS tranche_min,
            COALESCE((CAST(CAST(PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY upper_salary) AS INT) AS TEXT) || ' €'), 'No data' ) AS tranche_max
        FROM jobs
        WHERE job_search IS NOT NULL
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND region IS NOT NULL
        GROUP BY region
        ORDER BY region
    """)
    dashboard_df_median_salary_regions = pd.DataFrame(cursor.fetchall(), columns=['region', 'tranche_min', 'tranche_max'])

    # Graphique
    dashboard_med_salary_table = go.Figure(
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
                        dashboard_df_median_salary_regions['region'].to_list(), 
                        dashboard_df_median_salary_regions['tranche_min'].to_list(), 
                        dashboard_df_median_salary_regions['tranche_max'].to_list()
                        ],
                    line_color='#212529',
                    fill_color='#343a40',
                    align=['center'],
                    height=25,
                    font_color=[
                        'white', 
                        ['dimgray' if str(x.strip()) == "No data" else "white" for x in list(dashboard_df_median_salary_regions['tranche_min'])]
    ]))])

    dashboard_med_salary_table.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=(len(dashboard_df_median_salary_regions['region'])*25)+30,
        width=700
    )

    object_name = 'dashboard_med_salary_all_regions_table.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_med_salary_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau salaire médian par région terminé')


    # ------------------------------------------------
    # ------ Linechart evolution salaire global ------
    # ------------------------------------------------


    # Requête SQL
    cursor.execute(f"""
        SELECT 
            DATE(DATE_TRUNC('month', date_of_search)) AS date,
            PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY lower_salary) AS tranche_min,
            PERCENTILE_DISC(0.5) WITHIN GROUP(ORDER BY upper_salary) AS tranche_max
        FROM jobs
        WHERE date_of_search >= '2023-12-01'
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 month'
        AND DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        GROUP BY DATE(DATE_TRUNC('month', date_of_search))
    """)
    dashboard_median_salary_choice_df = pd.DataFrame(cursor.fetchall(), columns=['date', 'tranche_min', 'tranche_max'])

    dashboard_median_salary_choice_df['date'] = pd.to_datetime(dashboard_median_salary_choice_df['date'])

    dashboard_median_salary_global_evolution = go.Figure()

    dashboard_median_salary_global_evolution.add_trace(
        go.Scatter(
            x=dashboard_median_salary_choice_df['date'], 
            y=dashboard_median_salary_choice_df['tranche_min'], 
            fill = None, 
            line_color = 'green', 
            mode='lines', 
            hovertemplate="Tranche min : <b>%{y}</b>pour le mois de <b>%{x}</b><extra></extra>"
    ))

    dashboard_median_salary_global_evolution.add_trace(
        go.Scatter(
            x=dashboard_median_salary_choice_df['date'], 
            y=dashboard_median_salary_choice_df['tranche_max'], 
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
        xaxis_range=[dashboard_median_salary_choice_df['date'].min() - pd.DateOffset(days=5), dashboard_median_salary_choice_df['date'].max() + pd.DateOffset(days=5)],
        yaxis_range=[0, dashboard_median_salary_choice_df['tranche_max'].max() + dashboard_median_salary_choice_df['tranche_max'].max()*20/100],
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

    

    object_name = 'dashboard_median_salary_global_evolution.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_median_salary_global_evolution.write_image(object_name, format='png', engine='kaleido')

    print('Linechart salaire médian global terminé')


    # --------------------------------------------------
    # ------ Tableau top technos 12 derniers mois ------
    # --------------------------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT technologie, SUM(occurrences) AS offres
        FROM jobsoccurrences
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY technologie
        ORDER BY SUM(occurrences) DESC
        LIMIT 32
    """)
    dashboard_techno_table_df = pd.DataFrame(cursor.fetchall(), columns=['techno', 'occurrences'])

    # Graphique
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

    dashboard_techno_table.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=(len(dashboard_techno_table_df['techno'])*25)+30,
        width=500
    )


    object_name = 'dashboard_techno_table_12m.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_techno_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau top technos terminé')


    # -------------------------------------------------
    # ------ Tableau top villes 12 derniers mois ------
    # -------------------------------------------------

    # Requête SQL
    cursor.execute("""
        SELECT city, COUNT(*) AS offres
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY city
        ORDER BY COUNT(*) DESC
        LIMIT 32
    """)
    dashboard_ville_table_df = pd.DataFrame(cursor.fetchall(), columns=['ville', 'offres'])

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

    object_name = 'dashboard_ville_table_12m.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_ville_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau top villes terminé')


    # ------------------------------------------------------
    # ------ Tableau top entreprises 12 derniers mois ------
    # ------------------------------------------------------


    # Requête SQL
    cursor.execute("""
        SELECT company_name, COUNT(*) AS offres
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND DATE(DATE_TRUNC('month', date_of_search)) >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
        GROUP BY company_name
        ORDER BY COUNT(*) DESC
        LIMIT 32
    """)
    dashboard_entreprise_table_df = pd.DataFrame(cursor.fetchall(), columns=['entreprise', 'offres'])           

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
    
    object_name = 'dashboard_entreprise_table_12m.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_entreprise_table.write_image(object_name, format='png', engine='kaleido')

    print('Tableau top entreprises terminé')


    #--------------------------------------------------------
    #------ Table top technologies des 3 derniers mois ------
    #--------------------------------------------------------

    # Requête SQL
    cursor.execute(f"""
        SELECT technologie, SUM(occurrences) AS offres
        FROM jobsoccurrences   
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'                             
        GROUP BY technologie
        ORDER BY SUM(occurrences) DESC
        LIMIT 32
    """)
    dashboard_techno_table_3_m_df = pd.DataFrame(cursor.fetchall(), columns=['techno', 'occurrences'])

    # Graphique
    dashboard_techno_table_3_m = go.Figure(
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
                        dashboard_techno_table_3_m_df['techno'].to_list(), 
                        dashboard_techno_table_3_m_df['occurrences'].to_list()
                        ],
                line_color='#212529',
                fill_color='#343a40',
                align=['center'],
                height=25,
                font=dict(size=14, color='white'),   
    ))])
    
    # Mise à jour du layout
    dashboard_techno_table_3_m.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=(len(dashboard_techno_table_3_m_df['techno'])*25)+30,  # Ajustement auto de la hauteur
        width=500
    )


    object_name = 'dashboard_techno_3m_table.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_techno_table_3_m.write_image(object_name, format='png', engine='kaleido')

    print('Dashboard tableau top technos 3 months terminé')


    #------------------------------------------------------------
    #------ Table villes qui recrutent des 3 derniers mois ------
    #------------------------------------------------------------

    cursor.execute(f"""
        SELECT city, COUNT(*) AS offres
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW())
        AND date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'     
        GROUP BY city
        ORDER BY COUNT(*) DESC
        LIMIT 32
    """)
    dashboard_ville_table_df_last_3_m_df = pd.DataFrame(cursor.fetchall(), columns=['ville', 'offres'])

    # Création de la table
    dashboard_ville_table_3_m = go.Figure(
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
                    dashboard_ville_table_df_last_3_m_df['ville'].to_list(), 
                    dashboard_ville_table_df_last_3_m_df['offres'].to_list()
                    ],
                line_color='#212529',
                fill_color='#343a40',
                align=['center'],
                height=25,
                font=dict(size=14, color='white'),   
    ))])

    # Mise à jour du layout
    dashboard_ville_table_3_m.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=(len(dashboard_ville_table_df_last_3_m_df['ville'])*25)+30,  # Ajustement auto de la hauteur
        width=500
    )

    object_name = 'dashboard_ville_3m_table.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_ville_table_3_m.write_image(object_name, format='png', engine='kaleido')

    print('Dashboard tableau top villes 3 months terminé')


    #-----------------------------------------------------------------
    #------ Table entreprises qui recrutent des 3 derniers mois ------
    #-----------------------------------------------------------------

    cursor.execute(f"""
        SELECT company_name, COUNT(*) AS offres
        FROM jobs
        WHERE DATE(DATE_TRUNC('month', date_of_search)) < DATE_TRUNC('month', NOW()) 
        AND date_of_search >= DATE_TRUNC('month', NOW()) - INTERVAL '3 month'     
        GROUP BY company_name
        ORDER BY COUNT(*) DESC
        LIMIT 32
    """)
    dashboard_entreprise_table_df_last_3_m = pd.DataFrame(cursor.fetchall(), columns=['entreprise', 'offres'])

    # Création de la table
    dashboard_entreprise_table_3_m = go.Figure(
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
                        dashboard_entreprise_table_df_last_3_m['entreprise'].to_list(), 
                        dashboard_entreprise_table_df_last_3_m['offres'].to_list()
                        ],
                    line_color='#212529',
                    fill_color='#343a40',
                    align=['center'],
                height=25,
                font=dict(size=14, color='white'),   
        ))])

    # Mise à jour du layout
    dashboard_entreprise_table_3_m.update_layout(
        margin=dict(t=0, b=0, r=0, l=0),
        paper_bgcolor="#212529",  # Couleur de fond du graphique (autour de la bordure)
        plot_bgcolor="#1d2228",  # Couleur de fond du graphique (intérieur de la bordure)
        dragmode=False,
        font=dict(color='white', size=14),
        height=(len(dashboard_entreprise_table_df_last_3_m['entreprise'])*25)+30,  # Ajustement auto de la hauteur
        width=500
    )

    object_name = 'dashboard_entreprise_3m_table.png'

    # Utilisez le répertoire temporaire pour écrire l'image en tant que fichier temporaire
    dashboard_entreprise_table_3_m.write_image(object_name, format='png', engine='kaleido')

    print('Dashboard tableau top entreprises 3 months terminé')


    # -----------------------
    # ------ Fermeture ------
    # -----------------------

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Erreur: {str(e)}")
    raise e