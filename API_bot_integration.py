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
import hmac
import hashlib



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


AUTH_WINDOW_SECONDS = 300  # 5 minutes
SECRET              = os.getenv("SECRET", "").split(",")


app = Flask(__name__)

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

LEGAL_FORM_MAPPING = {
  "legal_forms": [
    {
      "code": "1000",
      "label": "Entrepreneur individuel"
    },
    {
      "code": "2",
      "label": "Personne morale de droit privé non dotée de la personnalité morale"
    },
    {
      "code": "3",
      "label": "Personne morale de droit étranger"
    },
    {
      "code": "4",
      "label": "Personne morale de droit public soumise au droit commercial"
    },
    {
      "code": "5",
      "label": "Société commerciale"
    },
    {
      "code": "6",
      "label": "Autre personne morale immatriculée au RCS"
    },
    {
      "code": "7",
      "label": "Personne morale et organisme soumis au droit administratif"
    },
    {
      "code": "8",
      "label": "Organisme privé spécialisé"
    },
    {
      "code": "9",
      "label": "Groupement de droit privé"
    }
  ]
}

LEGAL_FORM_CODE_TO_LABEL = {
    item["code"]: item["label"]
    for item in LEGAL_FORM_MAPPING["legal_forms"]
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
    "legal_form": "Categorie_juridique",
    "headquarters": "Siege_entreprise",
    "company_creation_date_threshold": "Date_creation_entreprise",
    "capital": "Capital",
    "subsidiaries_number": "Nombre_etablissements"
}

MOIS_ANNEE = "1225"
TABLE_FAST = f"sirene{MOIS_ANNEE}saasv9_bot"
TABLE_ALL  = f"sirene{MOIS_ANNEE}saasv9"

def insert_api_log(timestamp, request_json, duration, response_json):
    """
    Inserts an API call log into the LogAPI_bot table
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = """
        INSERT INTO LogAPI_bot (timestamp, request_json, duration, response_json)
        VALUES (%s, %s, %s, %s)
        """
        cursor.execute(query, (
            timestamp,
            json.dumps(request_json, ensure_ascii=False),
            duration,
            json.dumps(response_json, ensure_ascii=False)
        ))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        logger.error("Failed to log API call: %s", e)



def get_db_connection():
    """Établit une connexion à la base de données MySQL"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except Error as e:
        print(f"Erreur de connexion à MySQL: {e}")
        raise

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


def convert_legal_form_code_to_label(code: str) -> str | None:
    """
    Convertit un code legal_form (API) en libellé stocké en base MySQL
    """
    if code is None:
        return None
    return LEGAL_FORM_CODE_TO_LABEL.get(str(code))

def build_query(criteria: Dict[str, Any]) -> tuple[str, List[Any]]:
    """
    Construit la requête SQL et les paramètres à partir des critères de ciblage

    Returns:
        tuple: (query_string, parameters_list)
    """
    where_clauses: List[str] = []
    params: List[Any] = []

    # ==============================
    # Choix de la table selon headquarters
    # ==============================

    table = TABLE_ALL  # table par défaut

    legal_criteria = criteria.get('legal_criteria', {})
    if legal_criteria.get('present') and legal_criteria.get('headquarters') is not None:
        if legal_criteria['headquarters'] is True:
            table = TABLE_FAST
        else:
            table = TABLE_ALL

    # ==============================
    # Traitement de la localisation
    # ==============================
    if criteria.get('location', {}).get('present'):
        loc = criteria['location']

        add_scalar_or_list_filter(
            where_clauses,
            params,
            FIELD_MAPPING['post_code'],
            loc.get('post_code')
        )

        add_scalar_or_list_filter(
            where_clauses,
            params,
            FIELD_MAPPING['departement'],
            loc.get('departement')
        )

        add_scalar_or_list_filter(
            where_clauses,
            params,
            FIELD_MAPPING['region'],
            loc.get('region')
        )

        add_scalar_or_list_filter(
            where_clauses,
            params,
            FIELD_MAPPING['city'],
            loc.get('city')
        )

    # ==============================
    # Traitement de l'activité
    # ==============================
    if criteria.get('activity', {}).get('present'):
        act = criteria['activity']

        if act.get('activity_codes_list'):
            codes = act['activity_codes_list']
            placeholders = ','.join(['?'] * len(codes))
            where_clauses.append(
                f"{FIELD_MAPPING['activity_codes_list']} IN ({placeholders})"
            )
            params.extend(codes)

    # ==============================
    # Traitement de la taille de l'entreprise
    # ==============================
    if criteria.get('company_size', {}).get('present'):
        size = criteria['company_size']
        value = size.get('employees_number_range')

        if value:
            if isinstance(value, list):
                placeholders = ",".join(["?"] * len(value))
                where_clauses.append(
                    f"{FIELD_MAPPING['employees_number_range']} IN ({placeholders})"
                )
                params.extend(value)
            else:
                where_clauses.append(
                    f"{FIELD_MAPPING['employees_number_range']} = ?"
                )
                params.append(value)

    # ==============================
    # Traitement des critères financiers
    # ==============================
    if criteria.get('financial_criteria', {}).get('present'):
        fin = criteria['financial_criteria']

        if fin.get('turnover') is not None:
            where_clauses.append(f"{FIELD_MAPPING['turnover']} >= ?")
            params.append(fin['turnover'])

        if fin.get('net_profit') is not None:
            where_clauses.append(f"{FIELD_MAPPING['net_profit']} >= ?")
            params.append(fin['net_profit'])

        if fin.get('profitability') is not None:
            where_clauses.append(f"{FIELD_MAPPING['profitability']} >= ?")
            params.append(fin['profitability'])

    # ==============================
    # Traitement des critères légaux
    # ==============================
    if legal_criteria.get('present'):

        legal = legal_criteria


        if legal.get('legal_form'):
            values = legal['legal_form']
            if not isinstance(values, list):
                values = [values]

            labels = [
                convert_legal_form_code_to_label(v)
                for v in values
                    if convert_legal_form_code_to_label(v)
            ]

            if labels:
                placeholders = ",".join(["?"] * len(labels))
                where_clauses.append(
                    f"{FIELD_MAPPING['legal_form']} IN ({placeholders})"
                )
                params.extend(labels)

        # ⚠️ headquarters volontairement ignoré ici
        # car le choix de la table est déjà fait

        if legal.get('company_creation_date_threshold'):
            date_field = FIELD_MAPPING['company_creation_date_threshold']

            if legal.get('company_creation_date_sup'):
                where_clauses.append(f"{date_field} >= ?")
                params.append(legal['company_creation_date_threshold'])

            if legal.get('company_creation_date_inf'):
                where_clauses.append(f"{date_field} <= ?")
                params.append(legal['company_creation_date_threshold'])

        if legal.get('capital') is not None:
            capital_field = FIELD_MAPPING['capital']

            if legal.get('capital_threshold_sup'):
                where_clauses.append(f"{capital_field} >= ?")
                params.append(legal['capital'])

            if legal.get('capital_threshold_inf'):
                where_clauses.append(f"{capital_field} <= ?")
                params.append(legal['capital'])

        if legal.get('subsidiaries_number') is not None:
            where_clauses.append(
                f"{FIELD_MAPPING['subsidiaries_number']} >= ?"
            )
            params.append(legal['subsidiaries_number'])

    # ==============================
    # Construction de la requête finale
    # ==============================
    base_query = f"SELECT COUNT(*) AS count FROM {table}"

    if where_clauses:
        query = base_query + " WHERE " + " AND ".join(where_clauses)
    else:
        query = base_query

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



def count_semantic(original_request):
    return 2000
    # discard < 3 letters and figures
    # remove stop words
    # count siren from WEB
    # count siren from BODACC
    # count siren from Nom_entreprise et Nom_enseigne
    # 


def require_api_key(func):
   

    AUTH_WINDOW = 300  # 5 minutes

    @wraps(func)
    def decorated(*args, **kwargs):
        headers = {k.lower(): v for k, v in request.headers.items()}

        api_key     = headers.get("x-api-key")
        timestamp   = headers.get("x-timestamp")
        signature   = headers.get("x-signature")

        if not api_key or not timestamp or not signature:
            return jsonify({"error": "Missing authentication headers"}), 401

        API_KEYS = json.loads(os.getenv("API_KEYS", "{}"))

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

@app.route('/count_bot_v2', methods=['POST'])
@require_api_key

def count_companies():
    """
    Endpoint pour compter les entreprises correspondant aux critères de ciblage
    """
    #print("API_KEY:", os.getenv("API_KEY"))

    start_time = time.perf_counter()
    timestamp = datetime.now()
    try:
        # Validation du content-type
        if not request.is_json:
            logger.warning("Content-Type not JSON")
            return jsonify({'error': 'Content-Type must be application/json'}), 400
        
        # Récupération des données
        #print(f"request:{request}")
        criteria = request.get_json()
        logger.debug(f"Received criteria: {criteria}")

        if not criteria:
            logger.warning("Request body is empty")
            return jsonify({'error': 'Request body is empty'}), 400

        # Determine where the actual criteria dict is
        criteria_dict = criteria.get("criteria", criteria)

        # Convert employees ranges to salaries
        criteria_dict = convert_employees_range_to_salaries(criteria_dict)

        # Update original JSON if needed
        if "criteria" in criteria:
            criteria["criteria"] = criteria_dict
        else:
            criteria = criteria_dict

        # --- Total count legal ---
        query, params = build_query(criteria)
        check_sql_params(query, params)
        debug_sql = format_sql_for_debug(query, params)
        logger.debug("SQL (printable): %s", debug_sql)

        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(debug_sql)
        result = cursor.fetchone()
        total_count_legal = result['count'] if result else 0
        cursor.close()
        conn.close()
        logger.info("Total count legal: %d", total_count_legal)

        # --- Individual counts per activity_code (legal only) ---
        activity_individual_counts = {}
        activity_codes = criteria.get('activity', {}).get('activity_codes_list', [])

        if activity_codes:
            for code in activity_codes:
                criteria_single = copy.deepcopy(criteria)
                criteria_single['activity'] = {
                    'present': True,
                    'activity_codes_list': [code]
                }

                query_code, params_code = build_query(criteria_single)
                check_sql_params(query_code, params_code)
                debug_sql_code = format_sql_for_debug(query_code, params_code)
                logger.debug("SQL preview for activity_code '%s': %s", code, debug_sql_code)

                conn = get_db_connection()
                cursor = conn.cursor(dictionary=True)
                cursor.execute(debug_sql_code)
                result_code = cursor.fetchone()
                cursor.close()
                conn.close()

                activity_individual_counts[code] = {
                    "count_legal": result_code['count'] if result_code else 0
                }

        # --- Build response ---
        response = {
            'status': 'success',
            'count_legal': total_count_legal,
            'activity_individual_counts': activity_individual_counts if activity_codes else None,
            'criteria_applied': {
                'localization': criteria.get('location', {}).get('present', False),
                'activity': criteria.get('activity', {}).get('present', False),
                'company_size': criteria.get('company_size', {}).get('present', False),
                'financial_criteria': criteria.get('financial_criteria', {}).get('present', False),
                'legal_criteria': criteria.get('legal_criteria', {}).get('present', False)
            },
            'debug_sql': debug_sql
        }

        logger.info("Response: %s", response)
        return jsonify(response), 200

    except Exception as e:
        logger.exception("Internal server error")
        return jsonify({'error': 'Internal server error', 'message': str(e)}), 500

    finally:
        end_time = time.perf_counter()
        duration = end_time - start_time
        try:
            insert_api_log(timestamp, criteria, duration, response)
        except Exception as e:
            logger.error("Failed to insert API log: %s", e)



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
    app.run(host='0.0.0.0', port=5002, debug=True)
    
    # Pour la production, utilisez un serveur WSGI comme Gunicorn:
    # gunicorn -w 4 -b 0.0.0.0:5000 app:app