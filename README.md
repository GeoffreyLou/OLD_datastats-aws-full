# IMPORTANT

🔴🔴🔴 Ce projet n'est plus déployé sur AWS en raison des coûts élevés d'architecture. Il est actuellement refactorisé sur GCP, suivre les repositories débutant par "datastats-gcp-" 🔴🔴🔴


# Datastats

Datastats est un projet Data end-to-end construit dasn le cloud AWS. Il est composé d'un back-end complètement autonome qui se charge de récupérer plusieurs fois par jours les métiers liés à la data sur un site de recritement. Des fonctions Lambda se chargent ensuite de nettoyer les fichiers pour alimenter une base de données, quand d'autres se chargent de réaliser les graphiques une fois par mois. 

Cela permet d'alimenter un site-web réalisé en Django, et optimisé via une CDN AWS qui se charge de mettre en cache la totalité du contenu statique. 

La scalabilité et résilience est assurée par un Application Load Balancer avec des instances EC2 & RDS Multi-AZ.

La sécurité de l'architrecture est assurée par un VPC composé de deux public subnets et deux private subnets, les entrées et sorties se font via des endpoints spéficique. Aucune variable d'environnement sensible n'est présente dans les fichiers de code, elles sont gérées par AWS Secrets Manager. Les seules variables d'environnement existent pour les noms des secrets à requêter ainsi que la région. 

<p align="center">
  <img src="https://i.ibb.co/jZncwJX/architecturev3.png" alt="Architecture">
</p>

    
# Contenu du repository

- datastats : le projet Django qui est composé de deux sous applications
  * **Website** : le back-end et front-end des pages web
  * **Authentication** : le module spécifique lié à la gestion des utilisateurs
 
- lambda functions : les différentes fonctions lambda utilisées dans ce projet.
  * **lambda_dashboard_all_jobs** : création des images quand on sélectionne tous les métiers pour chaque région disponible
  * **lambda_dashboard_without_all** : création des images pour chaque métier, on utilise ici AWS Stepfunction pour fournir une variable d'entrée à cette fonction qui est réutilisée pour chaque métier
  * **lambda_index_dashboard** : création des images par défaut sur la page d'accueil du site et du dashboard
  * **cleaning_csv_files** : fonction qui se charge de nettoyer chaque fichier csv provenant du webscraping
 
- python scripts : dossier comprenant des fichiers pythons
  * **database_cleaning** : nettoyage de la base de données après ajout ou retrait d'une technologie des listes
  * **reporting** : script qui permet de gérer l'envoi du reporting journalier
  * **city_error** : script qui gère la table des villes n'ayant pas matché avec une région en base
 
- tests : dossier comprenant divers tests unitaires et d'intégration
