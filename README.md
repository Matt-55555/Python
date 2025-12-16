<strong>Auteur</strong>  
Jean-Matthieu Charre  
Développeur Python  
Année 2025  
________________________________________
<strong>Licence</strong>  
Projet personnel.<br>
Le code présenté sur GitHub a pour but de donner un aperçu de mes pratiques et compétences techniques. 
________________________________________
<strong>Notes</strong>   
Présentation générale :<br>
Ce projet Python met en place un pipeline complet de validation et transformation de fichiers JSON représentant des données de machines de forage (drilling machines).
________________________________________
<br>
<h1>Développement Python
Transformation et enrichissement de données JSON avec une architecture modulaire</h1>
<br>
<strong>A) Objectif</strong>strong><br>
<br>
Nettoyer, normaliser, et enrichir des fichiers bruts avant de les enregistrer sous une forme propre et cohérente.<br>
<br>
Le projet est structuré en trois fichiers :<br>
•	Main_program.py : orchestration du pipeline, gestion des erreurs, lecture/écriture robuste,<br>
•	Functions_DM.py : ensemble de transformations fonctionnelles appliquées aux JSON,<br>
•	Logging.py : système de logging configurable et horodaté.<br>
<br>
Ce projet a été conçu pour démontrer une architecture Python répondant aux standards industriels : pipeline modulaire, exceptions métiers, atomicité d’écriture, logging avancé, typage fort, et séparation claire des responsabilités.<br>
<br>
<strong>B) Architecture générale</strong><br>
<br>
&nbsp;&nbsp;&nbsp;<strong>1. Pipeline de transformation</strong><br>
<br>
Le cœur du projet est un pipeline séquentiel : une liste de fonctions, chacune recevant un dictionnaire et renvoyant un nouveau dictionnaire transformé.<br><br>
Pipeline utilisé :<br>
pipeline = [<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;normalisation_casse_clefs,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;remove_irrelevant_data_points,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;format_dates,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;convert_miles_to_meters,<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;missing_contact_information<br>
]<br><br>
Atouts :<br>
•	Structure fonctionnelle et testable,<br>
•	Étapes indépendantes et facilement réorganisables,<br>
•	Validation typée (chaque étape doit renvoyer un dictionaire).<br>
<br>
&nbsp;&nbsp;&nbsp;<strong>2. Exceptions métiers ("Domain Exceptions")</strong><br>
<br>
Le fichier principal définit deux exceptions personnalisées :<br>
•	FileProcessingError → erreurs liées aux fichiers (lecture, écriture, JSON invalide),<br>
•	PipelineStepError → erreurs levées lorsqu’une étape du pipeline échoue.<br>
<br>
Ce système permet :<br>
•	un chaînage d’exceptions (raise ... from e) pour garder les stack traces,<br>
•	une remontée d’erreurs propre et lisible,<br>
•	une séparation claire entre erreurs techniques et erreurs métier.<br>
<br>
&nbsp;&nbsp;&nbsp;<strong>3. Gestion robuste des fichiers</strong><br>
<br>
La sortie est écrite de manière atomique :<br>
•	Écriture dans un fichier temporaire,<br>
•	fsync() pour garantir que l’écriture est persistée (les données en mémoire RAM sont écrites sur le disque dur),<br>
•	os.replace() pour un remplacement atomique du fichier cible.<br>
<br>
Objectif : éviter les fichiers corrompus en cas d’interruption brutale (crash, coupure, etc).<br>
<br>
&nbsp;&nbsp;&nbsp;<strong>4. Logging structuré</strong><br>
<br>
Le fichier Logging.py :<br>
•	crée un dossier de logs dédié (~/desktop/PYTHON-LOGS),<br>
•	génère un fichier unique par exécution (timestamp),<br>
•	définit un format standardisé (date, niveau, module, message),<br>
•	log vers fichier .txt et vers console.<br>
<br>
Ce logging est utilisé dans tout le pipeline pour :<br>
•	suivre les transformations,<br>
•	tracer les erreurs,<br>
•	mesurer les étapes critiques.<br>
<br>
&nbsp;&nbsp;&nbsp;<strong>5. Protection si : __name__ == "__main__"</strong><br>
<br>
Le script principal utilise une garde 'main' : if __name__ == "__main__"<br>
<br>
Cela permet :<br>
•	d’exécuter le fichier comme script,<br>
•	d'importer le fichier pour écrire des tests unitaires ou utiliser les fonctions sans déclencher le traitement principal.<br>
<br>
&nbsp;&nbsp;&nbsp;<strong>6. Métriques internes</strong><br>
<br>
Le code maintient un compteur : metrics = Counter({...})<br>
<br>
Le compteur traque :<br>
•	le nombre total de fichiers,<br>
•	le nombre de fichiers traités,<br>
•	le nombre de fichiers en erreur,<br>
•	et le nombre d'erreurs par étape dans le pipeline.<br>
<br>
Ce compteur est possiblement exploitable par un outil de monitoring (e.g.: Prometheus).<br>
<br>
<strong>C) Fonctionnalités majeures de transformation (Functions_DM.py)</strong><br>
<br>
•	'def normalisation_casse_clefs()' :<br>
&nbsp;&nbsp;&nbsp;→ passe toutes les clés en minuscules (y compris en profondeur).<br>
•	'def remove_irrelevant_data_points()' :<br>
&nbsp;&nbsp;&nbsp;→ filtre les clés non pertinentes selon une liste blanche.<br>
•	'def format_dates()' :<br>
&nbsp;&nbsp;&nbsp;→ convertit YYYY-MM-DD en format français DD/MM/YYYY.<br>
•	'def convert_miles_to_meters()' :<br>
&nbsp;&nbsp;&nbsp;→ conversion d’unités dans les spécifications.<br>
•	'def missing_contact_information()' :<br>
&nbsp;&nbsp;&nbsp;→ garantit la présence d’une structure minimale contact_information.<br>
<br>
Ces fonctions :<br>
•	ne modifient pas l’objet en input (pas de mutation),<br>
•	renvoient une nouvelle structure,<br>
•	sont loggées pour faciliter le debugging.<br>
<br>
<strong>D) Arborescence du projet</strong><br>
<br/>
project_folder/<br>
├── Main_program.py<br>
├── subfolder1/<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├── \_\_init\_\_.py<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;├── Functions_DM.py<br>
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;└── Logging.py<br>
raw/<br>
processed/
