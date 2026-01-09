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

API_KEYS = os.getenv("API_KEYS", "").split(",")
#print(f"API_KEYS:{API_KEYS}")


app = Flask(__name__)

# Configuration de la base de données MySQL
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', 3306),
    'database': os.getenv('DB_NAME', 'webscan'),
    'user': os.getenv('DB_USER', 'webscan'),
    'password': os.getenv('DB_PASSWORD', 'Garenne92&&'),
    'charset': 'utf8mb4',
    'use_unicode': True
}

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
TABLE = "sirene1225saasv9"


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


def build_query(criteria: Dict[str, Any]) -> tuple[str, List[Any]]:
    """
    Construit la requête SQL et les paramètres à partir des critères de ciblage
    
    Returns:
        tuple: (query_string, parameters_list)
    """
    where_clauses = []
    params = []
    
   # Traitement de la localisation
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
        
    # Traitement de l'activité
    if criteria.get('activity', {}).get('present'):
        act = criteria['activity']
        
        if act.get('activity_codes_list'):
            codes = act['activity_codes_list']
            placeholders = ','.join(['?'] * len(codes))
            where_clauses.append(f"{FIELD_MAPPING['activity_codes_list']} IN ({placeholders})")
            params.extend(codes)
    
     # Traitement de la taille de l'entreprise
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
    
    # Traitement des critères financiers
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
    
    # Traitement des critères légaux
    if criteria.get('legal_criteria', {}).get('present'):
        legal = criteria['legal_criteria']
        
        if legal.get('legal_category'):
            where_clauses.append(f"{FIELD_MAPPING['legal_category']} = ?")
            params.append(legal['legal_category'])
        
        if legal.get('headquarters') is not None:
            where_clauses.append(f"{FIELD_MAPPING['headquarters']} = ?")
            params.append('oui' if legal['headquarters'] else 'non')
        
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
            where_clauses.append(f"{FIELD_MAPPING['subsidiaries_number']} >= ?")
            params.append(legal['subsidiaries_number'])
    
    # Construction de la requête finale
    base_query = f"SELECT COUNT(*) as count FROM {TABLE}"
    
    if where_clauses:
        where_clause = " WHERE " + " AND ".join(where_clauses)
        query = base_query + where_clause
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
    @wraps(func)
    def decorated(*args, **kwargs):
        # Normalize headers to lowercase for case-insensitive lookup
        headers = {k.lower(): v for k, v in request.headers.items()}

        # Try X-Api-Key first
        api_key = headers.get("x-api-key")

        # Fallback to Authorization header: Bearer <token>
        if not api_key:
            auth_header = headers.get("authorization")
            if auth_header and auth_header.lower().startswith("bearer "):
                api_key = auth_header.split(" ", 1)[1]

        # print("All headers received:", dict(request.headers))
        # print(f"API key received:{api_key}:")

        # print(f"API_KEYS:{API_KEYS}:")
        if not api_key or api_key not in API_KEYS:
            return jsonify({"error": "Unauthorized: Invalid or missing API key"}), 401

        return func(*args, **kwargs)
    return decorated

@app.route('/count_bot_v1', methods=['POST'])
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
    app.run(host='0.0.0.0', port=5001, debug=True)
    
    # Pour la production, utilisez un serveur WSGI comme Gunicorn:
    # gunicorn -w 4 -b 0.0.0.0:5000 app:app