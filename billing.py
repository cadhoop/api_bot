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
import secrets
import threading
from openpyxl.utils import get_column_letter
from pathlib import Path
from PIL import Image


import library
from API_bot_parameters_integration import DB_CONFIG_ikoula3, DB_CONFIG_ekima, FIELD_MAPPING, TAB_STOPWORDS, TAB_MOTS_SUP100000_SANS_ACCENT, MOIS_ANNEE, TABLE_FAST,TABLE_ALL,TABLE_AFNIC,MIN_FULLTEXT_LENGTH,LIMIT_DISPLAY_INFO,UNITARY_PRICE_LEGAL_INFOS,MAX_DAILY_REQUESTS_NUMBER, FRENCH_ELISIONS,AUTH_WINDOW , MESSAGE_WORD_TOO_COMMON_1, MESSAGE_WORD_TOO_COMMON_2, MEMORY_THRESHOLD_PERCENT, PATH_REMOTE_OVH, LINK_REMOTE_OVH, SFTP_NAME,SFTP_PORT,SFTP_USERNAME,SFTP_PASSWORD,PATH_REMOTE_DATA_FILE,PATH_REMOTE_INVOICE_FILE,PATH_LOCAL_INVOICE_FILE,PATH_LOCAL_DATA_FILE, PATH_LOGO_MARKETHINGS_EKIMIA, PATH_LOGO_MARKETHINGS_IKOULA3, VERBOSE

from library import require_api_key_v1, require_api_key_v2, get_db_connection, check_siren_in_db, count_companies_logic, get_company_file, insert_stripe_id_file_link_criteria, update_payment_info_email, get_data_file_link, wait_for_data_file, invoice_edition, update_payment_invoice, push_delivery_files

    
app = Flask(__name__)

@app.route('/check_siren_build_file_V1', methods=['POST'])
@require_api_key_v1

def check_company():
    # Récupération du JSON envoyé dans le corps de la requête
    data                = request.get_json()
    siren               = data['siren']
    criteria            = data['criteria']
    billing_post_code   = data['billing_post_code']
    billing_address     = data['billing_address']
    billing_full_name   = data['billing_full_name']
    file_price          = data['file_price']
    company             = data['company']
    billing_city        = data['billing_city']


    if not data:
        return jsonify({"error": "JSON body is missing"}), 400

    # Logique de vérification : On vérifie si le SIREN existe

    try:
        conn            = get_db_connection()
        cursor          = conn.cursor(dictionary=True)
        exists          = check_siren_in_db(siren, conn)
        if exists:
            new_stripe_id   = secrets.token_hex(10)
            code = 200

            # lancement du calcul du fichier
            result = count_companies_logic(criteria)
            # print("result")
            # print(result)
             # On nettoie l'email (remplacement des caractères spéciaux par des underscores)
            #clean_email = re.sub(r'[^a-zA-Z0-9]', '_', raw_email)
            
            # 2. Génération du Timestamp (format: 20240522_143005)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            # 3. Construction du nom de fichier dynamique
            file_name           = f"export_{timestamp}_{new_stripe_id}.xlsx"
            #print(file_name)
            local_data_file     = f"./customer_files/data/{file_name}"
            data_remote_file_path    = f"{PATH_REMOTE_DATA_FILE}/{file_name}"

            #print(f"*******************data_remote_file_path:{data_remote_file_path}")
            list_siren = result['siren_list']

            # 1. Préparer le thread
            thread_prod = threading.Thread(
                target=get_company_file, 
                args=(data_remote_file_path, local_data_file, criteria, list_siren, new_stripe_id)
            )

            # 2. Lancer le thread (ne bloque pas le script)
            thread_prod.start()

            # print("file_link")
            # print(file_link)
            # print(type(criteria))

            insert_stripe_id_file_link_criteria(new_stripe_id, file_price, billing_post_code, billing_address, billing_city, billing_full_name, local_data_file, data_remote_file_path, company, siren, criteria, conn)


        else:
            new_stripe_id = None
            logger.error(f"siren non existant: {siren}")
            code = 404

        # doc generation Id

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    # Vous pouvez ici ajouter une logique pour valider les 'criteria' 
    # envoyés dans le JSON si nécessaire.
    
    return jsonify({
        "siren": siren,
        "exists": exists,
        "stripe_id": new_stripe_id
    }), code


# doc webhook call when sucess payment
@app.route('/purchase_success_V1', methods=['POST'])
@require_api_key_v1

def purchase_success_V1():

     # Récupération du JSON envoyé dans le corps de la requête
    data            = request.get_json()
    stripe_id       = data['stripe_id']
    email_client    = data['email_client']
    card_owner      = data['card_owner']

    try:
        conn            = get_db_connection()
        cursor          = conn.cursor(dictionary=True)
        # doc update payment info and customer_email
        update_payment_info_email(stripe_id, email_client, card_owner, conn)

        local_data_file_path, data_remote_file_path = get_data_file_link(stripe_id, conn)
        data_remote_file_with_email_path       = data_remote_file_path.replace(".xlsx", f"_{email_client}.xlsx")

        # doc verification of the production od the data file
        status_file_ready  = wait_for_data_file(stripe_id, conn)

        if status_file_ready:
            # doc in voice production
            status, invoice_remote_file_path, invoice_local_file_path = invoice_edition(stripe_id, conn)
            update_payment_invoice(stripe_id, invoice_remote_file_path, conn)

        else:
             return jsonify({
                    "error":"data_file not build",
                    "success": False
                }), 404
    except Exception as e:
        logger.exception("Internal server error: purchase_success_V1")
        return jsonify({
            'error': 'Internal server error',
            'message': str(e)
        }), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    if VERBOSE == "yes":
        print("**********************")
        print(f"local_data_file_path:{local_data_file_path}")
        print(f"data_remote_file_with_email_path:{data_remote_file_with_email_path}")
        print(f"invoice_local_file_path:{invoice_local_file_path}")
        print(f"invoice_remote_file_path:{invoice_remote_file_path}")


    status_push_delivery_files = push_delivery_files(local_data_file_path, data_remote_file_with_email_path, invoice_local_file_path, invoice_remote_file_path )

    data_remote_file_with_email_link = data_remote_file_with_email_path.replace(PATH_REMOTE_OVH, LINK_REMOTE_OVH)
    #print(f"data_remote_file_with_email_link:{data_remote_file_with_email_link}")
    invoice_remote_file_link = invoice_remote_file_path.replace(PATH_REMOTE_OVH, LINK_REMOTE_OVH)
    #print(f"data_remote_file_with_email_link:{data_remote_file_with_email_link}")
 
    return jsonify({
        "data_remote_file_path":data_remote_file_with_email_link,
        "invoice_remote_file_path":invoice_remote_file_link,
        "success": True
    }), 200



if __name__ == '__main__':
    # Pour le développement
    app.run(host='0.0.0.0', port=5002, debug=True, threaded=True)