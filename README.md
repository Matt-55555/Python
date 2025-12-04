# Python
Développements en Python

-- Présentation générale --

Ce projet Python met en place une pipeline complète de validation et transformation de fichiers JSON représentant des données de machines de forage (drilling machines).
L’objectif : nettoyer, normaliser et enrichir des fichiers bruts avant de les enregistrer sous une forme propre et cohérente.
Le projet est structuré en trois fichiers :
•	Main_program.py — orchestration du pipeline, gestion des erreurs, lecture/écriture robuste.
•	Functions_DM.py — ensemble de transformations fonctionnelles appliquées aux JSON.
•	Logging.py — système de logging configurable et horodaté.
Ce projet a été conçu pour démontrer une architecture Python professionnelle : pipeline modulaire, exceptions métiers, atomicité d’écriture, logging avancé, typage fort, et séparation claire des responsabilités.
________________________________________
A) Architecture générale

1. Pipeline de transformation
Le cœur du projet est un pipeline séquentiel : une liste de fonctions, chacune recevant un dictionnaire Python et renvoyant un dictionnaire transformé.
Pipeline utilisé :
pipeline = [
    normalisation_casse_clefs,
    remove_irrelevant_data_points,
    format_dates,
    convert_miles_to_meters,
    missing_contact_information,
]
Atouts :
•	Structure fonctionnelle et testable
•	Étapes indépendantes et facilement réorganisables
•	Validation typée (chaque étape doit renvoyer un dict)
________________________________________
2. Exceptions métiers ("Domain Exceptions")
Le fichier principal définit deux exceptions personnalisées :
•	FileProcessingError → erreurs liées aux fichiers (lecture, écriture, JSON invalide).
•	PipelineStepError → erreurs levées lorsqu’une étape du pipeline échoue.
Ce système permet :
•	un chaînage d’exceptions (raise ... from e) pour garder les stack traces,
•	une remontée d’erreurs propre et lisible,
•	une séparation claire entre erreurs techniques et erreurs métier.
________________________________________
3. Gestion robuste des fichiers
La sortie est écrite de manière atomique :
1.	Écriture dans un fichier temporaire
2.	fsync() pour garantir que l’écriture est réellement persistée
3.	os.replace() pour un remplacement atomique du fichier cible
Objectif : éviter les fichiers corrompus en cas d’interruption brutale (crash, coupure, etc.)
________________________________________
4. Logging structuré
Le fichier Logging.py :
•	crée un dossier de logs dédié (~/desktop/PYTHON-LOGS),
•	génère un fichier unique par exécution (timestamp),
•	définit un format standardisé (date, niveau, module, message),
•	log vers fichier et vers console.
Ce logging est utilisé dans tout le pipeline pour :
•	suivre les transformations,
•	tracer les erreurs,
•	mesurer les étapes critiques.
________________________________________
5. Protection if __name__ == "__main__"
Le script principal utilise la garde classique :
if __name__ == "__main__":
Ce choix permet :
•	d’exécuter le fichier comme script,
•	mais aussi de l’importer pour écrire des tests unitaires ou d’autres pipelines,
•	sans déclencher automatiquement le traitement.
________________________________________
6. Métriques internes
Le code maintient un compteur :
metrics = Counter({...})
Il suit :
•	nombre total de fichiers,
•	fichiers traités,
•	fichiers en erreur,
•	erreurs par étape.
Ce compteur est facilement exploitable pour un futur monitoring (ex : Prometheus, Grafana).
________________________________________
🔧 Fonctionnalités majeures de transformation (Functions_DM.py)
•	normalisation_casse_clefs
→ passe toutes les clés en minuscules (y compris en profondeur).
•	remove_irrelevant_data_points
→ filtre les clés non pertinentes selon une liste blanche.
•	format_dates
→ convertit YYYY-MM-DD en format français DD/MM/YYYY.
•	convert_miles_to_meters
→ conversion d’unités dans les spécifications.
•	missing_contact_information
→ garantit la présence d’une structure minimale contact_information.
Ces fonctions :
•	ne modifient jamais l’objet d’entrée (pas de mutation),
•	renvoient toujours une nouvelle structure,
•	sont loggées pour faciliter le debugging.
________________________________________
B) Objectif du projet
Ce projet a été conçu pour montrer à un recruteur :
•	Ma capacité à structurer un projet Python “production-like”
•	Ma maîtrise du logging, des exceptions et de la sécurité d'écriture
•	Ma compréhension du pattern pipeline, très utilisé en data engineering
•	Une approche professionnelle : typage, docstrings, architecture modulaire
•	Mon expérience combinée Python / VBA pour automatiser des workflows réels
________________________________________
C) Arborescence du projet
project/
│
├── Main_program.py
├── subfolder1/
│   ├── Functions_DM.py
│   └── Logging.py
└── raw/
    └── drilling_machine*.json
________________________________________
D) Exécution
Modifier les chemins dans le __main__ :
RAW = Path(r"C:\path\to\raw")
PROCESSED = Path(r"C:\path\to\processed")
Puis lancer :
python Main_program.py
________________________________________
