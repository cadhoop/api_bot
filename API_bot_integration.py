from flask import Flask, request, jsonify
# from datetime import datetime
# import mysql.connector
# from mysql.connector import Error
# from typing import Dict, Any, List, Optional
# import os
# from datetime import date, datetime
# from typing import Dict, List, Tuple
# import logging
# import sys
# import time
# import json
# from functools import wraps
# import socket
# import unicodedata
# import hashlib
# import hmac
# import pandas as pd
# from decimal import Decimal
# from openpyxl.utils import get_column_letter
# import argparse
# import psutil
# import secrets
# import threading
# import paramiko
# from pathlib import Path
# from fpdf import FPDF
# from PIL import Image
# from fpdf.enums import XPos, YPos
# import traceback
# from typing import Dict, Any, List

from API_bot_parameters_integration import DB_CONFIG_ikoula3, DB_CONFIG_ekima, FIELD_MAPPING, TAB_STOPWORDS, TAB_MOTS_SUP100000_SANS_ACCENT, MOIS_ANNEE, TABLE_FAST,TABLE_ALL,TABLE_AFNIC,MIN_FULLTEXT_LENGTH,LIMIT_DISPLAY_INFO,UNITARY_PRICE_LEGAL_INFOS,MAX_DAILY_REQUESTS_NUMBER, FRENCH_ELISIONS,AUTH_WINDOW , MESSAGE_WORD_TOO_COMMON_1, MESSAGE_WORD_TOO_COMMON_2, MEMORY_THRESHOLD_PERCENT, PATH_REMOTE_OVH, LINK_REMOTE_OVH, SFTP_NAME,SFTP_PORT,SFTP_USERNAME,SFTP_PASSWORD,PATH_REMOTE_DATA_FILE,PATH_REMOTE_INVOICE_FILE,PATH_LOCAL_INVOICE_FILE,PATH_LOCAL_DATA_FILE, PATH_LOGO_MARKETHINGS_EKIMIA, PATH_LOGO_MARKETHINGS_IKOULA3

# doc import functions
import library
from library import require_api_key_v1, trafic_control, query_control, memory_guard, require_api_key_v2, count_companies_logic, get_company_info

from library import VERBOSE



app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Empêche Flask de trier les clés alphabétiquement
app.json.sort_keys = False

# nohup python3 API_bot_integration.py --verbose="no" > app.log 2>&1 &




#print(DB_CONFIG)
# Mapping des champs API vers les champs de la base de données








@app.route('/count_bot_v1', methods=['POST'])
@require_api_key_v1
@trafic_control
@query_control
@memory_guard

def get_companies_v1():
    if not request.is_json:
        return jsonify({'error': 'Content-Type must be application/json'}), 400

    if VERBOSE:
        print(request)
    criteria = request.get_json()
    criteria_dict = criteria.get("criteria", criteria)

    execution_block = criteria_dict.get("execution_mode", {})
    security_block = criteria_dict.get("security", {})
    if security_block:
        ip_address = security_block.get("ip_address")
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
            return jsonify({"error get_companies_v1": str(e)}), 400

        except Exception as e:
            logger.exception("Internal server error get_companies_v1")
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
            siren_list_legal    = result_count_companies_logic.get('results_legal', [])
            siren_list_semantic = result_count_companies_logic.get('results_semantic', [])

        else:
            siren_list_legal    = []
            siren_list_semantic = []


        # Maintenant on appelle votre fonction SQL avec cette liste d'entiers
        result_legal    = get_company_info(siren_list_legal, "origine: codes NAF")
        result_semantic = get_company_info(siren_list_semantic, "origine: sémantique")

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
        file_link = get_company_file(criteria, result_count_companies_logic,"")
        #print(f"company_info:{company_info}")

        return jsonify({
            "status": "success",
            "file_link": file_link
        }), 200
    else:
        logger.exception("Incorrect data return mode: get_companies_v1")
        return jsonify({
        'error': 'Incorrect data return mode',
        'message':  'Error server'
        }), 500
    


@app.route('/count_bot_v2', methods=['POST'])
@require_api_key_v2
@trafic_control
@query_control
@memory_guard


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
                siren_list_legal    = result_count_companies_logic.get('results_legal', [])
                siren_list_semantic = result_count_companies_logic.get('results_semantic', [])

            else:
                siren_list_legal    = []
                siren_list_semantic = []

            # Maintenant on appelle votre fonction SQL avec cette liste d'entiers
            result_legal    = get_company_info(siren_list_legal, "origine codes NAF")
            result_semantic = get_company_info(siren_list_semantic, "origine: sémantique")


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
        logger.exception("Internal server error: get_companies_v2")

        return jsonify({"error": str(e)}), 400

    except Exception as e:
        logger.exception("Internal server error: get_companies_v2")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500




# doc déclencehemtnvia le webhook
@app.route('/get_info_success_page_V1', methods=['POST'])
@require_api_key_v1

def get_info_success_page():
     # Récupération du JSON envoyé dans le corps de la requête
    data            = request.get_json()
    stripe_id       = data['stripe_id']
  
    try:
        conn            = get_db_connection()
        cursor          = conn.cursor(dictionary=True)
        # doc update payment info and customer_email
        invoice_link_file, data_link_file = get_info_success_page(stripe_id, conn)
        # doc in voice production

    except Exception as e: 
        logger.exception("Internal server error get_info_success_page")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    
    return jsonify({
        "invoice_link_file":invoice_link_file,
        "data_link_file":data_link_file,
        "success": True
    }), 200



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