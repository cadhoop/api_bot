from flask import Flask, request, jsonify
from datetime import datetime
import mysql.connector
from mysql.connector import Error
from typing import Dict, Any, List, Optional
import os
from datetime import date, datetime
from typing import Dict, List, Tuple
import re
import logging
import sys
import time
import json
import copy
from functools import wraps
import socket
import unicodedata
import hashlib
import hmac
import pandas as pd
from decimal import Decimal
from openpyxl.utils import get_column_letter
import math
import argparse


app = Flask(__name__)
# Empêche Flask de trier les clés alphabétiquement
app.json.sort_keys = False

# nohup python3 API_bot_integration.py > app.log 2>&1 &

# -- 1️⃣ Drop the table if it already exists
# mysql> DROP TABLE IF EXISTS `sirene1225saasv9_bot`;
# Query OK, 0 rows affected (0,08 sec)

# mysql> 
# mysql> -- 2️⃣ Create the table
# mysql> CREATE TABLE `sirene1225saasv9_bot` (
#   `siren` bigint NOT NULL,
#   `Commune` varchar(255) DEFAULT NULL,
#   `Code_postal` int DEFAULT NULL,
#   `Departement` smallint DEFAULT NULL,
#   `Region` varchar(100) DEFAULT NULL,
#   `Activite_entreprise` varchar(200) DEFAULT NULL,
#   `Tranche_effectif_entreprise` varchar(50) DEFAULT NULL,
#   `Date_creation_entreprise` date DEFAULT NULL,
#   `Capital` bigint DEFAULT NULL,
#   `Nombre_etablissements` int DEFAULT NULL,
#   `Categorie_juridique` varchar(255) DEFAULT NULL,
#   `CA_le_plus_recent` bigint DEFAULT NULL,
#   `Resultat_net_le_plus_recent` bigint DEFAULT NULL,
#   `Rentabilite_la_plus_recente` decimal(10,4) DEFAULT NULL,
#   `Nom_entreprise_lemmatise` varchar(150) DEFAULT NULL,
#   `Nom_enseigne` varchar(120) DEFAULT NULL,
#   `Nom_entreprise` varchar(150) DEFAULT NULL,
#   PRIMARY KEY (`siren`),
#   KEY `idx_commune` (`Commune`),
#   KEY `idx_departement` (`Departement`),
#   KEY `idx_region` (`Region`),
#   KEY `idx_code_postal` (`Code_postal`),
#   KEY `idx_activite_entreprise` (`Activite_entreprise`),
#   KEY `idx_tranche_effectif` (`Tranche_effectif_entreprise`),
#   KEY `idx_date_creation` (`Date_creation_entreprise`),
#   KEY `idx_capital` (`Capital`),
#   KEY `idx_nombre_etablissements` (`Nombre_etablissements`),
#   KEY `idx_categorie_juridique` (`Categorie_juridique`),
#   KEY `idx_ca_recent` (`CA_le_plus_recent`),
#   KEY `idx_rentabilite` (`Rentabilite_la_plus_recente`),
#   KEY `idx_region_activite` (`Region`,`Activite_entreprise`),
#   KEY `idx_region_effectif` (`Region`,`Tranche_effectif_entreprise`),
#   KEY `idx_region_activite_effectif` (`Region`,`Activite_entreprise`,`Tranche_effectif_entreprise`),
#   KEY `idx_dept_activite` (`Departement`,`Activite_entreprise`),
#   KEY `idx_dept_effectif` (`Departement`,`Tranche_effectif_entreprise`),
#   KEY `idx_dept_activite_effectif` (`Departement`,`Activite_entreprise`,`Tranche_effectif_entreprise`),
#   KEY `idx_cp_activite_effectif` (`Code_postal`,`Activite_entreprise`,`Tranche_effectif_entreprise`),
#   KEY `idx_commune_activite_effectif` (`Commune`,`Activite_entreprise`,`Tranche_effectif_entreprise`),
#   KEY `idx_activite_ca_power` (`Activite_entreprise`,`CA_le_plus_recent`),
#   KEY `idx_activite_rentabilite_power` (`Activite_entreprise`,`Rentabilite_la_plus_recente`),
#   KEY `idx_region_ca_recent` (`Region`,`CA_le_plus_recent`),
#   KEY `idx_cp_ca_recent` (`Code_postal`,`CA_le_plus_recent`),
#   KEY `idx_ca_resultat` (`CA_le_plus_recent`,`Resultat_net_le_plus_recent`),
#   FULLTEXT KEY `ft_nom_entreprise_enseigne` (`Nom_entreprise_lemmatise`,`Nom_enseigne`)
# ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
# Query OK, 0 rows affected (0,38 sec)

# mysql> 
# mysql> -- 3️⃣ Insert data from sirene1225saasv9 where Siege_entreprise = '1'
# mysql> INSERT INTO sirene1225saasv9_bot (
#     ->     siren,
#     ->     Commune,
#     ->     Code_postal,
#     ->     Departement,
#     ->     Region,
#     ->     Activite_entreprise,
#     ->     Tranche_effectif_entreprise,
#     ->     Date_creation_entreprise,
#     ->     Capital,
#     ->     Nombre_etablissements,
#     ->     Categorie_juridique,
#     ->     CA_le_plus_recent,
#     ->     Resultat_net_le_plus_recent,
#     ->     Rentabilite_la_plus_recente,
#     ->     Nom_entreprise_lemmatise,
#     ->     Nom_enseigne
#     -> )
#     -> SELECT
#     ->     s.Siren,
#     ->     s.Commune,
#     ->     s.Code_postal,
#     ->     s.Departement,
#     ->     s.Region,
#     ->     s.Activite_entreprise,
#     ->     s.Tranche_effectif_entreprise,
#     ->     s.Date_creation_entreprise,
#     ->     s.Capital,
#     ->     s.Nombre_etablissements,
#     ->     s.Categorie_juridique,
#     ->     s.CA_le_plus_recent,
#     ->     s.Resultat_net_le_plus_recent,
#     ->     s.Rentabilite_la_plus_recente,
#     ->     s.Nom_entreprise_lemmatise,
#     ->     s.Nom_enseigne
#     -> FROM sirene1225saasv9 s
#     -> WHERE s.Siege_entreprise = 'oui';
# Query OK, 0 rows affected (0,00 sec)
# Records: 0  Duplicates: 0  Warnings: 0

# mysql> INSERT INTO sirene1225saasv9_bot (     siren,     Commune,     Code_postal,     Departement,     Region,     Activite_entreprise,     Tranche_effectif_entreprise,     Date_creation_entreprise,     Capital,     Nombre_etablissements,
#     Categorie_juridique,     CA_le_plus_recent,     Resultat_net_le_plus_recent,     Rentabilite_la_plus_recente,     Nom_entreprise_lemmatise,     Nom_enseigne ) SELECT     s.Siren,     s.Commune,     s.Code_postal,     s.Departement,
# s.Region,     s.Activite_entreprise,     s.Tranche_effectif_entreprise,     s.Date_creation_entreprise,     s.Capital,     s.Nombre_etablissements,     s.Categorie_juridique,     s.CA_le_plus_recent,     s.Resultat_net_le_plus_recent,     s.Rentabilite_la_plus_recente,     s.Nom_entreprise_lemmatise,     s.Nom_enseigne FROM sirene1225saasv9 s WHERE s.Siege_entreprise = 'oui';
# Query OK, 13065137 rows affected (2 hours 22 min 52,25 sec)
# Records: 13065137  Duplicates: 0  Warnings: 0

# ALTER TABLE `sirene1225saasv9_bot` ADD FULLTEXT KEY `ft_nom_entreprise_enseigne` (`Nom_entreprise_lemmatise`, `Nom_enseigne`);
# Query OK, 0 rows affected, 1 warning (27 min 11,86 sec)


# CREATE TABLE LogAPI_bot (
#     id INT AUTO_INCREMENT PRIMARY KEY,
#     timestamp DATETIME NOT NULL,
#     request_json JSON NOT NULL,
#     duration FLOAT NOT NULL,
#     response_json JSON NOT NULL
# );

# # Configure logging
# logging.basicConfig(
#     level=logging.DEBUG,  # or INFO in production
#     format='%(asctime)s [%(levelname)s] %(message)s',
#     handlers=[
#         logging.StreamHandler(sys.stdout),  # logs to console
#         logging.FileHandler("api.log")      # optional: logs to a file
#     ]
# )
# log = logging.getLogger('werkzeug')  # Flask’s HTTP request logger
# log.setLevel(logging.WARNING)

# logger = logging.getLogger(__name__)

VERBOSE = False


# Configuration de la base de données MySQL
DB_CONFIG_ekima = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 3306),
    'database': os.getenv('DB_NAME', 'webscan'),
    'user': os.getenv('DB_USER', 'webscan'),
    'password': os.getenv('DB_PASSWORD', 'Garenne92&&'),
    'charset': 'utf8mb4',
    'use_unicode': True
}

DB_CONFIG_ikoula3 = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 3306),
    'database': os.getenv('DB_NAME', 'webscan'),
    'user': os.getenv('DB_USER', 'webscan'),
    'password': os.getenv('DB_PASSWORD', 'Garenne2025&&'),
    'charset': 'utf8mb4',
    'use_unicode': True
}

#print(DB_CONFIG)
# Mapping des champs API vers les champs de la base de données
FIELD_MAPPING = {
    "post_code": "Code_postal",
    "departement": "Departement",
    "region": "Region",
    "city": "Commune",
    "activity_codes_list": "Activite_entreprise",
    "original_activity_request": "Original_request",
    "employees_number_range": "Tranche_effectif_entreprise",
    "turnover": "CA_le_plus_recent",
    "net_profit": "Resultat_net_le_plus_recent",
    "profitability": "Rentabilite_la_plus_recente",
    "legal_category": "Categorie_juridique",
    "headquarters": "Siege_entreprise",
    "company_creation_date_threshold": "Date_creation_entreprise",
    "capital": "Capital",
    "subsidiaries_number": "Nombre_etablissements"
}

TAB_STOPWORDS = [
    "a",
    "à",
    "alors",
    "au",
    "aucun",
    "aucuns",
    "aussi",
    "autre",
    "aux",
    "avant",
    "avec",
 #"avoir",
 #"bien",
   # "bon",
    "ça",
    "car",
    "ce",
    "cela",
    "ces",
    "cette",
    "ceux",
    "chaque",
    "ci",
    "comme",
    "comment",
    #"copyright",
    "dans",
    "de",
    "début",
    "dedans",
    "dehors",
    "depuis",
    "des",
    "devrait",
    #"dimanche",
    "doit",
    "donc",
    #"dos",
    "du",
    "elle",
    "elles",
    "en",
    "encore",
    "est",
    "et",
    "et/ou",
    "étaient",
    #"état",
    "été",
    "etes",
    "étions",
    #"etre",
    #"être",
    "eu",
    #"faire",
    "fait",
    "faites",
    #"fois",
    "font",
    #"hors",
    "ici",
    "il",
    "ils",
    "je",
    #"jeudi",
    #"jour",
    "juste",
    "la",
    "là",
    "le",
    "les",
    "leur",
    #"lundi",
    "ma",
    #"maintenant",
    "mais",
    #"mardi",
    "même",
    #"mercredi",
    "mes",
    "mettre",
    #"moins",
    "mon",
    #"mot",
    "ni",
    "nommés",
    "non",
    "nos",
    "notre",
    "nous",
    "ou",
    "où",
    "par",
    "parce",
    "pas",
    "peu",
    "peut",
    "plupart",
    "plus",
    "pour",
    "pourquoi",
    "quand",
    "que",
    "quel",
    "quelle",
    "quelles",
    "quels",
    "qui",
    "sa",
    #"samedi",
    "sans",
    "se",
    "ses",
    "seulement",
    "si",
    "sien",
    "son",
    "sont",
    "sous",
    "soyez",
    #"suite",
    "sur",
    "ta",
    "tandis",
    "tellement",
    "tels",
    "tes",
    "ton",
    "toujours",
    "tous",
    "tout",
    "toute",
    "très",
    "trop",
    "tu",
    "un",
    "une",
    #"vendredi",
    "voient",
    "vont",
    "vos",
    "votre",
    "vous",
    "vu",
]

TAB_MOTS_SUP100000_SANS_ACCENT = [
"acquisition",
"vente",
"location",
"immobilier",
"gestion",
"administration",
"immeuble",
"exploitation",
"achat",
"propriete",
"droit",
"construction",
"produit",
"activite",
"societe",
"bail",
"valeur",
"service",
"mobilier",
"apport",
"accessoire",
"objet",
"amenagement",
"prestation",
"operation",
"voie",
"prise",
"conseil",
"commerce",
"participation",
"travaux",
"entreprise",
"vehicule",
"transformation",
"materiel",
"realisation",
"bati",
"entretien",
"velo",
"social",
"restauration",
"moyen",
"titre",
"civil",
"article",
"industriel",
"livraison",
"batiment",
"alimentaire",
"proprietaire",
"creation",
"professionnel",
"domicile"
]

MOIS_ANNEE  = "1225"
TABLE_FAST  = f"sirene{MOIS_ANNEE}saasv9_bot"
TABLE_ALL   = f"sirene{MOIS_ANNEE}saasv9"
TABLE_AFNIC = f"Afnic_Light{MOIS_ANNEE}_full"

MIN_FULLTEXT_LENGTH             = 3
LIMIT_DISPLAY_INFO              = 5
UNITARY_PRICE_LEGAL_INFOS       = 0.10
MAX_DAILY_REQUESTS_NUMBER       = 1000
AUTH_WINDOW                     = 300
MEMORY_THRESHOLD_PERCENT        = 80.0



FRENCH_ELISIONS = r"\b(?:d|l|j|c|qu|n|s|t|m|jusqu|lorsqu|puisqu)'"

MESSAGE_WORD_TOO_COMMON_1 = "Desole, la recherche sur l'expression"
MESSAGE_WORD_TOO_COMMON_2 = "est trop commune. Merci de preciser votre demande."