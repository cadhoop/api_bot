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

# 1. Initialiser le parseur
parser = argparse.ArgumentParser(description="Script API Bot avec mode verbose")

# 2. Définir l'argument --verbose
# On utilise default="no" pour que le script fonctionne même sans l'argument
parser.add_argument('--verbose', type=str, default="no", help='Activer le mode verbose (yes/no)')

# 3. Récupérer les arguments
args = parser.parse_args()

VERBOSE = False
# 4. Utilisation dans votre code
if args.verbose == "yes":
    print("--- MODE VERBOSE ACTIVÉ ---")
    print(f"Arguments reçus : {args}")
    VERBOSE = True

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # keep DEBUG for file logging

# Remove default handlers added by basicConfig (if any)
if logger.hasHandlers():
    logger.handlers.clear()

# Console handler (WARNING+ only)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.WARNING)  # show only WARNING, ERROR, CRITICAL
console_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

# File handler (DEBUG+ everything)
file_handler = logging.FileHandler("api.log")
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Suppress Flask werkzeug INFO logs in console
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.WARNING)

logger = logging.getLogger(__name__)

# Load API_KEYS from env defense code
try:
    raw = os.getenv("API_KEYS", "{}")

    API_KEYS = json.loads(raw)
    flag_error_key_V1 = False

except json.JSONDecodeError:
    flag_error_key_V1 = True
    raise RuntimeError("API_KEYS environment variable must be valid JSON")

try:
    raw = os.getenv("API_KEYS_V2", "{}")
    API_KEYS = json.loads(raw)
    flag_error_key_V2 = False

except json.JSONDecodeError:
    flag_error_key_V2 = True
    raise RuntimeError("API_KEYS environment variable must be valid JSON")

if (flag_error_key_V2 or flag_error_key_V1):
    print("error lecture des clefs")
    sys.exit();
else:
    print("Chargement OK des clefs d'authentification")

# Defensive fix: unwrap list if accidentally wrapped
if isinstance(API_KEYS, list) and len(API_KEYS) == 1 and isinstance(API_KEYS[0], str):
    try:
        API_KEYS = json.loads(API_KEYS[0])
    except json.JSONDecodeError:
        raise RuntimeError("API_KEYS list contains invalid JSON")

if not isinstance(API_KEYS, dict):
    raise RuntimeError("API_KEYS must be a JSON object (dict)")
#print(f"API_KEYS:{API_KEYS}:")




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

hostname = socket.gethostname()
if hostname == "frhb96148ds":
    DB_CONFIG = DB_CONFIG_ikoula3
elif hostname == "dhoop-NS5x-NS7xAU":
    DB_CONFIG = DB_CONFIG_ekima

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

MOIS_ANNEE  = "1225"
TABLE_FAST  = f"sirene{MOIS_ANNEE}saasv9_bot"
TABLE_ALL   = f"sirene{MOIS_ANNEE}saasv9"
TABLE_AFNIC = f"Afnic_Light{MOIS_ANNEE}_full"

MIN_FULLTEXT_LENGTH = 3
LIMIT_DISPLAY_INFO = 5
UNITARY_PRICE_LEGAL_INFOS = 0.10




# === CHARGEMENT DU RÉFÉRENTIEL INSEE ===
def load_insee_referentiel(csv_path):
    """
    Charge le référentiel INSEE depuis un fichier CSV.
    
    Args:
        csv_path: Chemin vers le fichier CSV
        
    Returns:
        DataFrame avec les colonnes : nom_commune_complet, code_postal, 
        nom_region, code_departement
    """
    df_insee = pd.read_csv(
        csv_path,
        dtype={
            'code_postal': str,  # Important : garder en string pour éviter la perte des 0 initiaux
            'code_departement': str
        },
        encoding='utf-8'
    )
    
    # Nettoyer les espaces
    df_insee['nom_commune_complet'] = df_insee['nom_commune_complet'].str.strip()
    df_insee['nom_region'] = df_insee['nom_region'].str.strip()
    df_insee['code_postal'] = df_insee['code_postal'].str.strip()
    df_insee['code_departement'] = df_insee['code_departement'].str.strip()
    
    return df_insee



# Chargement du référentiel INSEE (à faire une seule fois au démarrage)
df_insee = load_insee_referentiel('communes-departement-region.csv')

def insert_api_log(timestamp, request_json, duration, response_json, ip_address):
    """
    Inserts an API call log into the LogAPI_bot table
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO LogAPI_bot (timestamp, request_json, duration, response_json, ip_address)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (
            timestamp,
            json.dumps(request_json, ensure_ascii=False),
            duration,
            json.dumps(response_json, ensure_ascii=False), 
            ip_address
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error("Failed to log API call: %s", e)


def filter_location_by_hierarchy(location_data, df_insee):
    # Normaliser les inputs en listes
    regions = location_data.get('region') or []
    regions = regions if isinstance(regions, list) else [regions]
    
    departements = location_data.get('departement') or []
    departements = departements if isinstance(departements, list) else [departements]
    
    post_codes = location_data.get('post_code') or []
    post_codes = post_codes if isinstance(post_codes, list) else [post_codes]
    
    cities = location_data.get('city') or []
    cities = cities if isinstance(cities, list) else [cities]

    # --- HIERARCHY LOGIC ---
    # Rule: If a child is inside a parent, drop the parent (keep the precision).
    # If they are independent, keep both (unless your rule is to strictly keep only one level)

    # 1. Check Cities vs Others
    if cities:
        df_cities = df_insee[df_insee['nom_commune_complet'].isin(cities)]
        if not df_cities.empty:
            # Get parents of these cities
            c_depts = df_cities['code_departement'].unique().tolist()
            c_regions = df_cities['nom_region'].unique().tolist()
            c_cps = df_cities['code_postal'].unique().tolist()

            # Remove parents if they contain these cities
            departements = [d for d in departements if d not in c_depts]
            regions = [r for r in regions if r not in c_regions]
            post_codes = [cp for cp in post_codes if cp not in c_cps]

    # 2. Check Postcodes vs Others
    if post_codes:
        df_cp = df_insee[df_insee['code_postal'].isin(post_codes)]
        if not df_cp.empty:
            cp_depts = df_cp['code_departement'].unique().tolist()
            cp_regions = df_cp['nom_region'].unique().tolist()

            departements = [d for d in departements if d not in cp_depts]
            regions = [r for r in regions if r not in cp_regions]

    # 3. Check Departements vs Regions
    if departements and regions:
        df_dept = df_insee[df_insee['code_departement'].isin(departements)]
        if not df_dept.empty:
            d_regions = df_dept['nom_region'].unique().tolist()
            regions = [r for r in regions if r not in d_regions]

    return {
        'region': regions if regions else None,
        'departement': departements if departements else None,
        'post_code': post_codes if post_codes else None,
        'city': cities if cities else None
    }

def get_db_connection():
    """Établit une connexion à la base de données MySQL"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        raise


def strip_activite_condition(sql: str) -> str:
    #print(f"sql before strip: {sql}")
    
    # On remplace la condition spécifique par une condition toujours vraie
    # Cela évite les problèmes de syntaxe (AND AND, WHERE AND, etc.)
    sql = re.sub(
        r"Activite_entreprise\s+IN\s*\([^)]*\)",
        "1=1",
        sql,
        flags=re.IGNORECASE
    )

    # Nettoyage des espaces doubles pour faire propre
    sql = re.sub(r"\s+", " ", sql).strip()

    #print(f"retour sql stripped: {sql}")
    return sql


def format_sql_for_debug(query: str, params: List[Any]) -> str:
    """
    Returns a printable SQL query with parameters interpolated.
    ⚠️ FOR DEBUGGING ONLY – NEVER execute this string.
    """

    def format_value(value: Any) -> str:
        if value is None:
            return "NULL"

        if isinstance(value, bool):
            return "1" if value else "0"

        if isinstance(value, (int, float)):
            return str(value)

        if isinstance(value, (date, datetime)):
            return f"'{value.isoformat()}'"

        # ✅ LIST → SQL IN (...)
        if isinstance(value, (list, tuple)):
            formatted_items = []
            for v in value:
                if v is None:
                    formatted_items.append("NULL")
                else:
                    formatted_items.append(
                        "'" + str(v).replace("'", "''") + "'"
                    )
            return "(" + ",".join(formatted_items) + ")"

        # STRING
        return "'" + str(value).replace("'", "''") + "'"

    formatted_query = query
    for param in params:
        formatted_query = formatted_query.replace("?", format_value(param), 1)

    return formatted_query


def add_scalar_or_list_filter(where_clauses, params, field_name, value):
    """
    Adds SQL filter for a scalar or list value.
    - scalar -> field = ?
    - list    -> field IN (?, ?, ...)
    """
    if value is None:
        return

    if isinstance(value, list):
        if not value:
            return
        placeholders = ",".join(["?"] * len(value))
        where_clauses.append(f"{field_name} IN ({placeholders})")
        params.extend(value)
    else:
        where_clauses.append(f"{field_name} = ?")
        params.append(value)

from typing import Dict, Any, List

def build_query(criteria: Dict[str, Any], flag_count = False) -> tuple[str, List[Any]]:
    """
    Builds the SQL query and parameters from targeting criteria.
    """
    where_clauses: List[str] = []
    params: List[Any] = []

    # ==============================
    # 1. Table Choice (Headquarters)
    # ==============================
    table = TABLE_ALL
    legal_criteria = criteria.get('legal_criteria', {})
    if legal_criteria.get('present') and legal_criteria.get('headquarters') is True:
        table = TABLE_FAST

    # ==============================
    # 2. Location (Hierarchical + OR logic)
    # ==============================
    if criteria.get('location', {}).get('present'):
        loc = criteria['location']
        filtered_loc = filter_location_by_hierarchy(loc, df_insee)
        
        loc_clauses = []
        # Process each level; if data exists, add to the OR group
        for key in ['city', 'post_code', 'departement', 'region']:
            values = filtered_loc.get(key)
            if values:
                if not isinstance(values, list):
                    values = [values]
                
                placeholders = ", ".join(["?"] * len(values))
                field_name = FIELD_MAPPING[key]
                loc_clauses.append(f"{field_name} IN ({placeholders})")
                params.extend(values)

        # Wrap all location filters in (OR) to avoid breaking other AND filters
        if loc_clauses:
            where_clauses.append(f"({' OR '.join(loc_clauses)})")

    # ==============================
    # 3. Activity
    # ==============================
    if criteria.get('activity', {}).get('present'):
        act = criteria['activity']
        if act.get('activity_codes_list'):
            codes = act['activity_codes_list']
            placeholders = ','.join(['?'] * len(codes))
            where_clauses.append(f"{FIELD_MAPPING['activity_codes_list']} IN ({placeholders})")
            params.extend(codes)

    # ==============================
    # 4. Company Size
    # ==============================
    if criteria.get('company_size', {}).get('present'):
        size = criteria['company_size']
        value = size.get('employees_number_range')
        if value:
            if isinstance(value, list):
                placeholders = ",".join(["?"] * len(value))
                where_clauses.append(f"{FIELD_MAPPING['employees_number_range']} IN ({placeholders})")
                params.extend(value)
            else:
                where_clauses.append(f"{FIELD_MAPPING['employees_number_range']} = ?")
                params.append(value)

    # ==============================
    # 5. Financial Criteria
    # ==============================
    if criteria.get('financial_criteria', {}).get('present'):
        fin = criteria['financial_criteria']
        for field in ['turnover', 'net_profit', 'profitability']:
            if fin.get(field) is not None:
                if fin.get(f'{field}_sup', True):
                    where_clauses.append(f"{FIELD_MAPPING[field]} >= ?")
                    params.append(fin[field])
                if fin.get(f'{field}_inf', False):
                    where_clauses.append(f"{FIELD_MAPPING[field]} <= ?")
                    params.append(fin[field])

    # ==============================
    # 6. Legal Criteria
    # ==============================
    if legal_criteria.get('present'):
        if legal_criteria.get('legal_category'):
            where_clauses.append(f"{FIELD_MAPPING['legal_category']} = ?")
            params.append(legal_criteria['legal_category'])

        # Creation Date
        if legal_criteria.get('company_creation_date_threshold'):
            d_field = FIELD_MAPPING['company_creation_date_threshold']
            val = legal_criteria['company_creation_date_threshold']
            if legal_criteria.get('company_creation_date_sup'):
                where_clauses.append(f"{d_field} >= ?")
                params.append(val)
            if legal_criteria.get('company_creation_date_inf'):
                where_clauses.append(f"{d_field} <= ?")
                params.append(val)

        # Capital
        if legal_criteria.get('capital') is not None:
            c_field = FIELD_MAPPING['capital']
            val = legal_criteria['capital']
            if legal_criteria.get('capital_threshold_sup'):
                where_clauses.append(f"{c_field} >= ?")
                params.append(val)
            if legal_criteria.get('capital_threshold_inf'):
                where_clauses.append(f"{c_field} <= ?")
                params.append(val)

        if legal_criteria.get('subsidiaries_number') is not None:
            where_clauses.append(f"{FIELD_MAPPING['subsidiaries_number']} >= ?")
            params.append(legal_criteria['subsidiaries_number'])

    # ==============================
    # Final Query Assembly
    # ==============================
    if flag_count:
        query = f"SELECT COUNT(*) AS count FROM {table}"
    else:
        query = f"SELECT siren FROM {table}"

    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    return query, params




def check_sql_params(query: str, params: List) -> None:
    """
    Vérifie que le nombre de paramètres correspond au nombre de placeholders '?' dans la requête SQL.
    Lève une exception si un mismatch est détecté.
    
    Args:
        query (str): La requête SQL avec placeholders '?'
        params (List): La liste des paramètres associés
    
    Raises:
        ValueError: Si le nombre de placeholders et de paramètres ne correspond pas
    """
    num_placeholders = query.count("?")
    num_params = len(params)
    
    if num_placeholders != num_params:
        raise ValueError(
            f"SQL placeholder mismatch detected!\n"
            f"Placeholders: {num_placeholders}, Params: {num_params}\n"
            f"Query: {query}\n"
            f"Params: {params}"
        )


def convert_employees_range_to_salaries(criteria: dict) -> dict:
    """
    Convertit 'X to Y employees' → 'X a Y salaries'
    Supporte string OU liste de strings
    """
    company_size = criteria.get("company_size", {})

    if not company_size.get("present"):
        return criteria

    value = company_size.get("employees_number_range")
    if not value:
        return criteria

    def convert(text: str) -> str:
        text = re.sub(r'\bto\b', 'a', text, flags=re.IGNORECASE)
        text = re.sub(r'\bemployees\b', 'salaries', text, flags=re.IGNORECASE)
        text = re.sub(r'\bemployee\b', 'salarie', text, flags=re.IGNORECASE)

        return text

    # ✅ Case 1: list of ranges
    if isinstance(value, list):
        company_size["employees_number_range"] = [
            convert(v) for v in value if isinstance(v, str)
        ]

    # ✅ Case 2: single string
    elif isinstance(value, str):
        company_size["employees_number_range"] = convert(value)

    return criteria

def test_criteria_mismatches(criteria: Dict) -> List[str]:
    """
    Analyse le JSON des critères et détecte les combinaisons impossibles
    qui pourraient générer un mismatch de paramètres SQL.
    
    Returns:
        List[str]: Liste de messages d'erreur sur les critères incohérents.
    """
    errors = []

    # Check legal criteria
    legal = criteria.get('legal_criteria', {})
    if legal.get('present'):
        # company_creation_date
        if legal.get('company_creation_date_threshold'):
            sup = legal.get('company_creation_date_sup', False)
            inf = legal.get('company_creation_date_inf', False)
            if sup and inf:
                errors.append(
                    "company_creation_date_threshold: both 'sup' and 'inf' are True — check if intended."
                )
            if (sup or inf) and not legal.get('company_creation_date_threshold'):
                errors.append(
                    "company_creation_date_threshold is missing but 'sup' or 'inf' is True"
                )

        # capital thresholds
        if legal.get('capital') is not None:
            sup = legal.get('capital_threshold_sup', False)
            inf = legal.get('capital_threshold_inf', False)
            if (sup or inf) and legal.get('capital') is None:
                errors.append(
                    "capital_threshold_sup or _inf is True but 'capital' is None"
                )
        else:
            if legal.get('capital_threshold_sup') or legal.get('capital_threshold_inf'):
                errors.append(
                    "'capital_threshold_sup' or '_inf' is True but 'capital' is missing"
                )
        
        # Add other dual-flag checks here as needed
        # e.g., financial criteria sup/inf, etc.

    # You can extend to other sections like company_size, financial_criteria if needed
    
    return errors



def lemmatize_expression(expression: str, conn) -> str:
    cursor = conn.cursor(dictionary=True)

    """
    Lemmatize each word in the expression using the DicoFrance table.
    Returns the lemmatized expression as a string.
    """
    if not expression:
        return ""

    # Split words by space
    words = expression.split()

    # Lowercase for consistency
    words_lower = [w.lower() for w in words]

    # Batch query to fetch lemmas
    format_strings = ",".join(["%s"] * len(words_lower))
    sql = f"SELECT entree, lemme FROM DicoFrance WHERE entree IN ({format_strings})"
    cursor.execute(sql, tuple(words_lower))
    results = cursor.fetchall()

    # Map: word -> lemma
    lemma_map = {r['entree']: r['lemme'] for r in results if r['lemme']}

    # Replace words with lemma if found
    lemmatized_words = [lemma_map.get(w, w) for w in words_lower]

    # Join back into a string
    return " ".join(lemmatized_words)

def remove_stop_words_french(texte, supprime_accent, verbose=False):
    """
    Remove French stop words and words shorter than MIN_FULLTEXT_LENGTH.
    """
    if texte is None:
        return "#"

    words = texte.split()
    result = []

    for w in words:
        original_w = w
        if supprime_accent:
            w, _ = removeaccent(w, verbose)
        w_lower = w.lower()

        # Skip stopwords and short words
        if w_lower not in TAB_STOPWORDS and len(w_lower) >= MIN_FULLTEXT_LENGTH:
            result.append(original_w)
        else:
            if verbose:
                print(f"Skipped: {w_lower}")

    return " ".join(result)



FRENCH_ELISIONS = r"\b(?:d|l|j|c|qu|n|s|t|m|jusqu|lorsqu|puisqu)'"

def normalize_french_text(text: str) -> tuple[str, str]:
    if not text:
        return "", ""

    # Lowercase
    text = text.lower()

    # Normalize accents (é → e)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")

    # Remove French elisions: d', l', qu', etc.
    text = re.sub(FRENCH_ELISIONS, "", text)

    # Remove remaining apostrophes
    text = re.sub(r"[’'`]", " ", text)

    # Keep dash (hyphen)
    text = re.sub(r"[^a-z0-9\s\-]", " ", text)

    # Normalize multiple dashes
    text = re.sub(r"-{2,}", "-", text)

    # Normalize spaces around dashes
    text = re.sub(r"\s*-\s*", "-", text)

    # Collapse spaces
    text = re.sub(r"\s+", " ", text).strip()

    # Variant 1: keep dash (for LIKE / exact)
    text_with_dash = text

    # Variant 2: remove dash (for FULLTEXT)
    text_without_dash = text.replace("-", " ")

    return text_with_dash, text_without_dash

def removeaccent(word, verbose=False):
    normalized = unicodedata.normalize("NFD", word)
    no_accent = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    if verbose:
        print(f"{word} -> {no_accent}")
    return no_accent, word



def count_semantic(original_request, debug_sql, conn, flag_count=False):

    # print(f"original_request:{original_request}")
    #print(f"debug_sql debut fct count_semantic:{debug_sql}")
    idx = debug_sql.upper().rfind("WHERE")
    if idx == -1:
        where_cmd_with_and      = "AND 1 = 1"
        where_cmd_without_and   = "1 = 1"
    else: 
        where_cmd_with_and = "AND " + debug_sql[idx + len("WHERE"):].strip()
        where_cmd_without_and = debug_sql[idx + len("WHERE"):].strip()

    #print(f"****where_cmd_with_and:{where_cmd_with_and}")
    # ---- 1️⃣ Your lemmatization function ----
    def lemmatize(text: str) -> str:
        # Replace with your actual lemmatization
        return text.lower()


    # ---- 3️⃣ Lemmatize the expression ----
    # print(f"original_request:{original_request}")
    # print("normalisze")
    # print(normalize_french_text(original_request), True, False)
    original_request_normalized_with_dash,original_request_normalized_without_dash = normalize_french_text(original_request)

    if "-" in original_request:
        expr_lem_with_dash      = lemmatize_expression( remove_stop_words_french(original_request_normalized_with_dash, True, False), conn)  # e.g., "directeur immobilier"
        expr_lem_without_dash   = lemmatize_expression( remove_stop_words_french(original_request_normalized_without_dash, True, False), conn)  # e.g., "directeur immobilier"
        # print(f"expr_lem_with_dash:{expr_lem_with_dash}")
        # print(f"expr_lem_without_dash:{expr_lem_without_dash}")
    else:
        expr_lem = lemmatize_expression( remove_stop_words_french(original_request_normalized_without_dash, True, False), conn)  # e.g., "directeur immobilier"
        expr_boolean = ' '.join(f'+{word}' for word in expr_lem.split())
        #print(f"expr_lem:{expr_lem}")
   

    # ---- 4️⃣ Convert to Boolean mode format ----
    # Example result: "+directeur +immobilier"

    cursor = conn.cursor()

   
    # ---- 6️⃣ SQL query with HAVING ----
    if "-" in original_request:
         query = f"""
                SELECT DISTINCT siren AS matching_companies
                FROM (

                /* -------------------------------
                   1️⃣ AFNIC
                -------------------------------- */
                SELECT
                    s.siren
                FROM (
                    SELECT
                        COALESCE(
                            sirentrouve,
                            sirentrouveaadresse,
                            sirentrouve_semantique
                        ) AS siren
                    FROM {TABLE_AFNIC}
                    WHERE MATCH(
                            title_lemmatise,
                            description_lemmatise,
                            keywords_lemmatise,
                            TexteHome_lemmatise,
                            keyword_nomdedomaine_lemmatise
                          )
                        AGAINST('{expr_lem_without_dash}' IN BOOLEAN MODE)
                      AND (
                            (sirentrouve IS NOT NULL and sirentrouve != 0)
                            OR (sirentrouveaadresse IS NOT NULL and sirentrouveaadresse != 0)
                            OR (sirentrouve_semantique IS NOT NULL and sirentrouve_semantique != 0)
                          )
                           AND (
                            title LIKE '%{expr_lem_with_dash}%'
                            OR description LIKE '%{expr_lem_with_dash}%'
                            OR keywords LIKE '%{expr_lem_with_dash}%'
                            OR TexteHome LIKE '%{expr_lem_with_dash}%'
                            OR keywords LIKE '%{expr_lem_with_dash}%'
                            )
                ) af
                INNER JOIN {TABLE_FAST} s
                    ON s.siren = af.siren
                WHERE
                    {where_cmd_without_and}

                UNION ALL

                /* -------------------------------
                   2️⃣ BODACC
                -------------------------------- */
                SELECT
                    s.siren
                FROM Bodacc_Light{MOIS_ANNEE}_full b
                INNER JOIN {TABLE_FAST} s
                    ON s.siren = b.siren
                WHERE MATCH(b.Objet_Social_lemmatisee)
                      AGAINST('{expr_lem_without_dash}' IN BOOLEAN MODE)
                  {where_cmd_with_and}
                AND Objet_Social LIKE '%{expr_lem_with_dash}%'


                UNION ALL

                /* -------------------------------
                   3️⃣ SIRENE
                -------------------------------- */
                SELECT
                    s.siren
                FROM {TABLE_FAST} s
                WHERE MATCH(s.Nom_entreprise_lemmatise, s.Nom_enseigne)
                      AGAINST('{expr_lem_without_dash}' IN BOOLEAN MODE)
                  {where_cmd_with_and}
                   AND (
                    Nom_entreprise LIKE '{expr_lem_with_dash}'
                 OR Nom_enseigne LIKE '{expr_lem_with_dash}'
              )

            ) t
            WHERE siren IS NOT NULL and siren != 0
                """
    else:

        query = f"""
        SELECT DISTINCT siren AS matching_companies
        FROM (

        /* -------------------------------
           1️⃣ AFNIC
        -------------------------------- */
        SELECT
            s.siren
        FROM (
            SELECT
                COALESCE(
                    sirentrouve,
                    sirentrouveaadresse,
                    sirentrouve_semantique
                ) AS siren
            FROM {TABLE_AFNIC}
            WHERE MATCH(
                    title_lemmatise,
                    description_lemmatise,
                    keywords_lemmatise,
                    TexteHome_lemmatise,
                    keyword_nomdedomaine_lemmatise
                  )
                AGAINST('{expr_boolean}' IN BOOLEAN MODE)
              AND (
                    (sirentrouve IS NOT NULL and sirentrouve != 0)
                    OR (sirentrouveaadresse IS NOT NULL and sirentrouveaadresse != 0)
                    OR (sirentrouve_semantique IS NOT NULL and sirentrouve_semantique != 0)
                  )
        ) af
        INNER JOIN {TABLE_FAST} s
            ON s.siren = af.siren
        WHERE
            {where_cmd_without_and}

        UNION ALL

        /* -------------------------------
           2️⃣ BODACC
        -------------------------------- */
        SELECT
            s.siren
        FROM Bodacc_Light{MOIS_ANNEE}_full b
        INNER JOIN {TABLE_FAST} s
            ON s.siren = b.siren
        WHERE MATCH(b.Objet_Social_lemmatisee)
              AGAINST('{expr_boolean}' IN BOOLEAN MODE)
          {where_cmd_with_and}

        UNION ALL

        /* -------------------------------
           3️⃣ SIRENE
        -------------------------------- */
        SELECT
            s.siren
        FROM {TABLE_FAST} s
        WHERE MATCH(s.Nom_entreprise_lemmatise, s.Nom_enseigne)
              AGAINST('{expr_boolean}' IN BOOLEAN MODE)
          {where_cmd_with_and}

    ) t
    WHERE siren IS NOT NULL and siren != 0
        """

    # ---- 7️⃣ Print query for debugging ----
    if VERBOSE:
        print("DEBUG: SQL Query to execute:")
        print(query)

    # ---- 8️⃣ Execute query ----
    cursor.execute(query)
    result = cursor.fetchall()
    cleaned_list = [siren[0] for siren in result]
    # print(f"cleaned_list: {cleaned_list}")

    matching_rows = len(cleaned_list)
    # print(f"matching_rows: {matching_rows}")


    #print(f"Number of matching rows: {matching_rows}")
    return matching_rows, cleaned_list
    # discard < 3 letters and figures
    # remove stop words
    # count siren from WEB
    # count siren from BODACC
    # count siren from Nom_entreprise et Nom_enseigne
    # 

def require_api_key_v1(func):
    @wraps(func)
    def decorated(*args, **kwargs):
        # Normalize headers to lowercase for case-insensitive lookup
        headers = {k.lower(): v for k, v in request.headers.items()}

        # Try X-Api-Key first
        api_key = headers.get("x-api-key")

        #print(f"api_key:{api_key}")
        # Fallback to Authorization header: Bearer <token>
        if not api_key:
            auth_header = headers.get("authorization")
            if auth_header and auth_header.lower().startswith("bearer "):
                api_key = auth_header.split(" ", 1)[1]

        # print("All headers received:", dict(request.headers))
        # print(f"API key received:{api_key}:")
        # print(f"API_KEYS:{API_KEYS}:")
        # print(not api_key or api_key not in API_KEYS)
        if not api_key or api_key not in API_KEYS:
            return jsonify({"error": "Unauthorized: Invalid or missing API key"}), 401

        return func(*args, **kwargs)
    return decorated




def require_api_key_v2(func):
   
    AUTH_WINDOW = 300  # 5 minutes

    @wraps(func)
    def decorated(*args, **kwargs):
        headers = {k.lower(): v for k, v in request.headers.items()}

        api_key     = headers.get("x-api-key")
        timestamp   = headers.get("x-timestamp")
        signature   = headers.get("x-signature")

        if not api_key or not timestamp or not signature:
            return jsonify({"error": "Missing authentication headers"}), 401

        #print(os.getenv("API_KEYS_V2", "{}"))
        API_KEYS = json.loads(os.getenv("API_KEYS_V2", "{}"))

        secret = API_KEYS.get(api_key)
        if not secret:
            return jsonify({"error": "Invalid API key"}), 401

        # Timestamp validation
        try:
            timestamp = int(timestamp)
        except ValueError:
            return jsonify({"error": "Invalid timestamp"}), 401

        if abs(time.time() - timestamp) > AUTH_WINDOW:
            return jsonify({"error": "Expired request"}), 401

        # Signature validation
        message = f"{api_key}{timestamp}".encode()
        expected = hmac.new(
            secret.encode(),
            message,
            hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected, signature):
            return jsonify({"error": "Invalid signature"}), 401

        return func(*args, **kwargs)

    return decorated

def count_companies_logic(criteria: dict):
    """
    Pure business logic.
    Returns a Python dict ONLY.
    """
    start_time = time.perf_counter()
    timestamp = datetime.now()

    response = None

    try:
        if not criteria:
            raise ValueError("Request body is empty")

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)

        criteria_dict = criteria.get("criteria", criteria)
        security_block = criteria_dict.get("security", {})
        if security_block:
            ip_address = security_block.get("ip_address")
        else:
            ip_address = "0.0.0.0"

        criteria_dict = convert_employees_range_to_salaries(criteria_dict)
        # Step 1: Get the inner dictionary (execution_mode)
        execution_block = criteria_dict.get("execution_mode", {})

        # Step 2: Get the specific value (output_type)
        # We provide "count" as a second argument so it has a default value
        mode = execution_block.get("output_type", "count")
        original_activity_request = (
            criteria_dict.get('activity', {})
            .get('original_activity_request', [])
        )
        semantic_count_requested = (
            criteria_dict.get('activity', {})
            .get('semantic_count_requested', False)
        )
        #print(f"semantic_count_requested:{semantic_count_requested}")
        # --- Total count legal ---
        query, params = build_query(criteria_dict, mode == "count")
        check_sql_params(query, params)

        debug_sql = format_sql_for_debug(query, params)

        if VERBOSE:
            print(f"debug_sql:{debug_sql}")
        cursor.execute(debug_sql)


        #print(f"mode:{mode}")

        if mode == "count":

            result = cursor.fetchone()

            total_count_legal = result['count'] if result else 0
       
            list_siren_semantic = None
            # --- Semantic ---
            if semantic_count_requested:
               
                total_count_semantic = count_semantic(original_activity_request, strip_activite_condition(debug_sql),conn, True)
            else:
                total_count_semantic = 0

            # --- Individual counts ---
            activity_individual_counts = {}
            activity_codes = criteria_dict.get('activity', {}).get('activity_codes_list', [])

            for code in activity_codes:
                criteria_single = copy.deepcopy(criteria_dict)
                criteria_single['activity'] = {
                    'present': True,
                    'activity_codes_list': [code]
                }

                query_code, params_code = build_query(criteria_single, True)
                debug_sql_code = format_sql_for_debug(query_code, params_code)

                cursor.execute(debug_sql_code)
                result_code = cursor.fetchone()

                activity_individual_counts[code] = {
                    "count_legal": result_code['count'] if result_code else 0
                }


            response = {
                'count_legal': total_count_legal,
                'count_semantic': total_count_semantic,
                'activity_individual_counts': activity_individual_counts or None,
                'debug_sql': debug_sql
            }

        elif mode == "display":

            #print("mode : display")
            result = cursor.fetchall()
            #print(f"toto result:{result}")
            if isinstance(result, list):
                siren_list_legal = [
                    row['siren'] if isinstance(row, dict) else row 
                    for row in result
                ]
            else:
                siren_list_legal = []

            # Votre logique de flag
            if semantic_count_requested:
                list_siren_semantic = siren_list_legal
            else:
                list_siren_semantic = []

            response = {
                "count": len(siren_list_legal),
                "results_semantic": list_siren_semantic,
                "results_legal": siren_list_legal
            }

        elif mode == "big_file":
            result = cursor.fetchall()
            #print(f"result:{result}")
            siren_list = [row['siren'] for row in result]

            # Mapping to a dictionary first
            response = {
                "count": len(siren_list),
                "siren_list": siren_list
            }

        return response

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

        duration = time.perf_counter() - start_time
        try:
            #print(f"ip_address:{ip_address}")
            insert_api_log(timestamp, criteria, duration, response, ip_address)
        except Exception:
            pass


def get_company_info(list_siren):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)  

    try:
        # 1. Sécurité : vérifier si la liste est vide
        if not list_siren:
            #print("return")
            # Retourne un dictionnaire cohérent avec la structure attendue
            return {"data": [], "metadata": {"total_found": 0, "requested_count": 0, "price": 0}}

        # 2. Générer les placeholders
        placeholders = ', '.join(['%s'] * len(list_siren))
        
        sql = f"""
            SELECT 
                siren, Nom_entreprise, Commune, Code_postal, Departement, Region, 
                Activite_entreprise, Tranche_effectif_entreprise, 
                Date_creation_entreprise, Capital, CA_le_plus_recent, 
                Resultat_net_le_plus_recent, Rentabilite_la_plus_recente
            FROM {TABLE_ALL}
            WHERE siren IN ({placeholders}) AND Siege_entreprise = 'oui' 
            LIMIT {LIMIT_DISPLAY_INFO}
        """

        cursor.execute(sql, tuple(list_siren)) 
        rows = cursor.fetchall()

        # 4. Data Transformation
        rows_display = rows[:LIMIT_DISPLAY_INFO]
        price = len(rows) * UNITARY_PRICE_LEGAL_INFOS
       # --- Dans votre fonction get_company_info ---
        rows_display = []
        for row in rows:
            # On définit l'ordre manuellement ici
            new_row = {
                "Origine de la recherche": "origine sémantique",
                "siren": row.get("siren"),
                "Nom_entreprise": row.get("Nom_entreprise"),
                "Commune": row.get("Commune"),
                "Code_postal": row.get("Code_postal"),
                "Departement": row.get("Departement"),
                "Region": row.get("Region"),
                "Activite_entreprise": row.get("Activite_entreprise"),
                "Tranche_effectif_entreprise": row.get("Tranche_effectif_entreprise"),
                "Date_creation_entreprise": row.get("Date_creation_entreprise"),
                "Capital": row.get("Capital"),
                "CA_le_plus_recent": row.get("CA_le_plus_recent"),
                "Resultat_net_le_plus_recent": row.get("Resultat_net_le_plus_recent"),
                "Rentabilite_la_plus_recente": row.get("Rentabilite_la_plus_recente")
            }
            rows_display.append(new_row)
        

        output = {
            "metadata": {
                "total_found": len(rows),
                "requested_count": len(list_siren),
                "price": math.ceil(price)
            },
            "data": rows_display,
        }
        return output # On renvoie l'objet dict proprement

    finally:
        cursor.close()
        conn.close()


def get_company_file(criteria, list_siren):
    #print(f"Critéres reçus : {criteria}")
    
    
    # On récupère les champs dans execution_mode

    exec_mode = criteria.get('execution_mode', {})
    raw_email = exec_mode.get('email_address', 'inconnu')

    #print(f"exec_mode:{exec_mode}")
    web_email_req = exec_mode.get('web_site_email_requested', False)
    web_phone_req = exec_mode.get('web_site_phone_requested', False)
    #print(f"web_email_req:{web_email_req}")
    #print(f"web_phone_req:{web_phone_req}")

    # On nettoie l'email (remplacement des caractères spéciaux par des underscores)
    clean_email = re.sub(r'[^a-zA-Z0-9]', '_', raw_email)
    
    # 2. Génération du Timestamp (format: 20240522_143005)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 3. Construction du nom de fichier dynamique
    file_name = f"export_{clean_email}_{timestamp}.xlsx"
    #print(file_name)
    file_path = f"./customer_files/{file_name}"

    # Initialisation pour éviter les UnboundLocalError
    output = {"metadata": {"total_found": 0, "file_link": None}}
    # Assurez-vous que ce dossier existe sur votre serveur
    
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        # dictionary=True est crucial pour que Pandas transforme les lignes en colonnes
        cursor = conn.cursor(dictionary=True)

        # 1. Extraction de la liste de SIREN
        if isinstance(list_siren, dict):
            list_siren = list_siren.get('results', [])

        if not list_siren:
            return json.dumps({"metadata": {"total_found": 0, "file_link": None}})

        placeholders = ', '.join(['%s'] * len(list_siren))

        if (web_email_req and not web_phone_req):
            #print("web_email_req and not web_phone_req")
            # 2. Requête SQL avec email
            sql = f"""
                SELECT 
                    siren, Commune, Code_postal, Departement, Region, 
                    Activite_entreprise, Libelle_activite_entreprises, Tranche_effectif_entreprise, 
                    Date_creation_entreprise, Capital, CA_le_plus_recent, 
                    Resultat_net_le_plus_recent, Rentabilite_la_plus_recente, Best_email
                FROM {TABLE_ALL}
                WHERE siren IN ({placeholders}) AND Siege_entreprise = 'oui'  AND Best_Email like '%@%'
            """
        elif (web_phone_req and not web_email_req):
            # 2. Requête SQL avec telephone
            sql = f"""
                SELECT 
                    siren, Commune, Code_postal, Departement, Region, 
                    Activite_entreprise, Libelle_activite_entreprises, Tranche_effectif_entreprise, 
                    Date_creation_entreprise, Capital, CA_le_plus_recent, 
                    Resultat_net_le_plus_recent, Rentabilite_la_plus_recente, Telephone_fixe, Telephone_mobile
                FROM {TABLE_ALL}
                WHERE siren IN ({placeholders}) AND Siege_entreprise = 'oui' AND Presence_numeros_de_telephone = 'oui'
            """

        elif (web_email_req and web_phone_req):
            # 2. Requête SQL avec telephone
            sql = f"""
                SELECT 
                    siren, Commune, Code_postal, Departement, Region, 
                    Activite_entreprise, Libelle_activite_entreprises, Tranche_effectif_entreprise, 
                    Date_creation_entreprise, Capital, CA_le_plus_recent, 
                    Resultat_net_le_plus_recent, Rentabilite_la_plus_recente, Best_Email, Telephone_fixe, Telephone_mobile
                FROM {TABLE_ALL}
                WHERE siren IN ({placeholders}) AND Siege_entreprise = 'oui' AND Best_Email like '%@%' AND Presence_numeros_de_telephone = 'oui'
            """
        else:
            # 2. Requête SQL
            sql = f"""
                SELECT 
                    siren, Commune, Code_postal, Departement, Region, 
                    Activite_entreprise, Libelle_activite_entreprises, Tranche_effectif_entreprise, 
                    Date_creation_entreprise, Capital, CA_le_plus_recent, 
                    Resultat_net_le_plus_recent, Rentabilite_la_plus_recente
                FROM {TABLE_ALL}
                WHERE siren IN ({placeholders}) AND Siege_entreprise = 'oui' 
            """

        # 3. Exécution
        cursor.execute(sql, tuple(list_siren)) 
        rows = cursor.fetchall()

        if rows:
            df_data = pd.DataFrame(rows)

            # --- PRÉPARATION ONGLET PARAMÉTRAGE ---
            setup_rows = []
            for section, details in criteria.items():
                if isinstance(details, dict):
                    status = "ACTIF" if details.get('present') else "INACTIF"
                    # On filtre 'present' pour ne garder que les valeurs des filtres
                    params = ", ".join([f"{k}: {v}" for k, v in details.items() if k != 'present'])
                    setup_rows.append({
                        "Section": section.upper(), 
                        "Statut": status, 
                        "Détails des filtres": params if params else "Aucun filtre spécifique"
                    })
            df_setup = pd.DataFrame(setup_rows)

            # --- GÉNÉRATION EXCEL ---
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                # Création des feuilles
                df_setup.to_excel(writer, index=False, sheet_name='Parametrage')
                df_data.to_excel(writer, index=False, sheet_name='Entreprises')


                # Mise en forme 'Entreprises'
                ws_data = writer.sheets['Entreprises']
                ws_data.freeze_panes = 'A2' # Fige les titres
                
                # Mapping des largeurs personnalisées
                column_widths = {
                    'siren': 15, 'Commune': 30, 'Code_postal': 12, 'Departement': 15,
                    'Region': 30, 'Activite_entreprise': 10, 'Libelle_activite_entreprises': 50,
                    'Tranche_effectif_entreprise': 27, 'Date_creation_entreprise': 20,
                    'Capital': 15, 'CA_le_plus_recent': 20, 'Resultat_net_le_plus_recent': 25,
                    'Rentabilite_la_plus_recente': 30, 'Best_Email': 40, 'Telephone_fixe':40, 'Telephone_mobile':40
                }

                for i, col_name in enumerate(df_data.columns):
                    col_letter = get_column_letter(i + 1) # Utilisation de la fonction importée
                    width = column_widths.get(col_name, 15)
                    ws_data.column_dimensions[col_letter].width = width

                # Mise en forme 'Parametrage'
                ws_setup = writer.sheets['Parametrage']
                ws_setup.column_dimensions['A'].width = 25
                ws_setup.column_dimensions['B'].width = 15
                ws_setup.column_dimensions['C'].width = 80

            file_link = f"https://www.markethings.io/customer_files/{file_name}"
        else:
            file_link = None

        output = {
            "metadata": {
                "total_found": len(rows),
                "file_link": file_link
            }
        }

    except Exception as e:
        print(f"Erreur lors de la génération du fichier : {e}")
        output = {"error": True, "message": str(e), "metadata": {"total_found": 0, "file_link": None}}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    return json.dumps(output, indent=4, default=str, ensure_ascii=False)


@app.route('/count_bot_v1', methods=['POST'])
@require_api_key_v1

def get_companies_v1():

    if not request.is_json:
        return jsonify({'error': 'Content-Type must be application/json'}), 400

    criteria = request.get_json()
    criteria_dict = criteria.get("criteria", criteria)

    execution_block = criteria_dict.get("execution_mode", {})
    security_block = criteria_dict.get("security", {})
    if security_block:
        ip_address = security_block.get("ip_address"):
    else:
        ip_address = "0.0.0.0"

    #print(f"execution_block:{execution_block}")

    # Step 2: Get the specific value (output_type)
    # We provide "count" as a second argument so it has a default value
    mode = execution_block.get("output_type", "count")

    #print(f"mode:{mode}")
    result_count_companies_logic = count_companies_logic(criteria)

    if mode == "count":
        try:

            return jsonify({
                "status": "success",
                **result_count_companies_logic
            }), 200

        except ValueError as e:
            return jsonify({"error": str(e)}), 400

        except Exception as e:
            logger.exception("Internal server error")
            return jsonify({
                'error': 'Internal server error',
                'message': str(e)
            }), 500

        return jsonify({
            "status": "success",
            **result_count_companies_logic
        }), 200

    elif mode == "display":

        #print(f"result:{result}")
            #print(f"result display:{result_count_companies_logic}")

            # On extrait la liste directement depuis le dictionnaire 'result'
        if isinstance(result_count_companies_logic, dict):
            # On récupère la liste d'entiers (on utilise results_legal ou results_semantic)
            siren_list_legal = result_count_companies_logic.get('results_legal', [])
        else:
            siren_list_legal = []

        # Maintenant on appelle votre fonction SQL avec cette liste d'entiers
        result_legal = get_company_info(siren_list_legal)

        result_semantic = result_legal
        # 2. On construit la réponse
        # Puisque result_legal est un dictionnaire, .get() fonctionne parfaitement
        return jsonify({
            "status": "success",
            "company_info_semantic": result_semantic,
            "company_info_legal": result_legal.get("data", []),
            "metadata": result_legal.get("metadata", {})
        }), 200

    elif mode == "big_file":

        #print(f"result:{result}")
        file_link = get_company_file(criteria, result_count_companies_logic)
        #print(f"company_info:{company_info}")

        return jsonify({
            "status": "success",
            "file_link": file_link
        }), 200
    else:
        logger.exception("Incorrect data return mode")
        return jsonify({
        'error': 'Incorrect data return mode',
        'message':  'Error server'
        }), 500
    


@app.route('/count_bot_v2', methods=['POST'])
@require_api_key_v2

def get_companies_v2():

    if not request.is_json:
        return jsonify({'error': 'Content-Type must be application/json'}), 400

    criteria = request.get_json()

    try:
        result_count_companies_logic = count_companies_logic(criteria)
        criteria_dict = criteria.get("criteria", criteria)
        # Step 1: Get the inner dictionary (execution_mode)
        # doc try to be compatible with 
        
        execution_block = criteria_dict.get("execution_mode", {})
        #print(f"execution_block:{execution_block}")

        # Step 2: Get the specific value (output_type)
        # We provide "count" as a second argument so it has a default value
        mode = execution_block.get("output_type", "count")

        #print(f"mode:{mode}")
        if mode == "count":

            return jsonify({
                "status": "success",
                **result_count_companies_logic
            }), 200

        elif mode == "display":

            #print(f"result:{result}")
            #print(f"result display:{result_count_companies_logic}")

            # On extrait la liste directement depuis le dictionnaire 'result'
            if isinstance(result_count_companies_logic, dict):
                # On récupère la liste d'entiers (on utilise results_legal ou results_semantic)
                siren_list_legal = result_count_companies_logic.get('results_legal', [])
            else:
                siren_list_legal = []

            # Maintenant on appelle votre fonction SQL avec cette liste d'entiers
            result_legal = get_company_info(siren_list_legal)

            # 2. On construit la réponse
            # Puisque result_legal est un dictionnaire, .get() fonctionne parfaitement
            return jsonify({
                "status": "success",
                "company_info_semantic": result_legal.get("data", []),
                "company_info_legal": result_legal.get("data", []),
                "metadata": result_legal.get("metadata", {})
            }), 200

        elif mode == "big_file":

            result = count_companies_logic(criteria)

            #print(f"result:{result}")
            file_link = get_company_file(criteria, result)
            #print(f"company_info:{company_info}")

            return jsonify({
                "status": "success",
                "file_link": file_link
            }), 200
        else:
            logger.exception("Incorrect data return mode")
            return jsonify({
            'error': 'Incorrect data return mode',
            'message': str(e)
        }), 500

       


    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.exception("Internal server error")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

@app.route('/health', methods=['GET'])
def health_check():
    """Endpoint pour vérifier que l'API fonctionne"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat()
    }), 200


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        'error': 'Endpoint not found'
    }), 404


@app.errorhandler(405)
def method_not_allowed(error):
    return jsonify({
        'error': 'Method not allowed'
    }), 405


if __name__ == '__main__':
    # Pour le développement
    app.run(host='0.0.0.0', port=5001, debug=True, threaded=True)

    # Pour la production, utilisez un serveur WSGI comme Gunicorn:
    # gunicorn -w 4 -b 0.0.0.0:5000 app:app