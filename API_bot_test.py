import requests
import json
from datetime import datetime
import time
import os
import argparse
import hashlib
import hmac
import sys
import operator
from typing import List, Optional


#  python3 API_bot_test.py --server="localhost"
VERSION = "V1"
VERSION = "V2"
print(f"VERSION:{VERSION}")

# Cas de test avec différentes combinaisons de critères
test_cases = [

    {
        "name": "Test 1 - Localisation par régions",
        "criteria": {
            "location": {
                "present": True,
                "region": ["Bretagne", "Occitanie"],
                "departement": None,
                "post_code": None,
                "city": None
            },
            "activity": {"present": False},
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": True}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 2 - Localisation par codes postaux",
        "criteria": {
            "location": {
                "present": True,
                "post_code": ["75001", "75002"],
                "region": None,
                "departement": None,
                "city": None
            },
            "activity": {"present": False},
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": True}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 3 - Activités NAF multiples",
        "criteria": {
            "location": {
                "present": True,
                "region": ["Bretagne", "Occitanie"],
                "departement": None,
                "post_code": None,
                "city": None
            },            
            "activity": {
                "present": True,
                "activity_codes_list": ["6201Z", "6202A", "6203Z"],
                "original_activity_request": "farines elle"
            },
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {
                "present": True,
                "headquarters": True,
                "legal_category": "Société commerciale",
                "company_creation_date_threshold": "2015-01-01",
                "company_creation_date_sup": True,
                "company_creation_date_inf": False
            }        },
        "expected": {"count_legal": {"op": ">", "value": 5000}, "count_semantic": {"op": "<=", "value": 70}}
    },

    {
        "name": "Test 4 - Taille PME",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": False},
            "company_size": {"present": True, "employees_number_range": "50 to 99 employees"},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": True}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 5 - Critères financiers",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": False},
            "company_size": {"present": False},
            "financial_criteria": {"present": True, "turnover": 5_000_000, "net_profit": 200_000, "profitability": None},
            "legal_criteria": {"present": True, "headquarters": True}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 6 - Légal: siège + date création",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": False},
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {
                "present": True,
                "headquarters": True,
                "legal_category": "Société commerciale",
                "company_creation_date_threshold": "2015-01-01",
                "company_creation_date_sup": True,
                "company_creation_date_inf": False
            }
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 7 - Légal: capital minimum",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": False},
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {
                "present": True,
                "headquarters": True,
                "capital": 100000,
                "capital_threshold_sup": True,
                "subsidiaries_number": 5
            }
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 8 - Localisation + Activité + Taille",
        "criteria": {
            "location": {"present": True, "region": ["Île-de-France", "Occitanie"]},
            "activity": {"present": True, "activity_codes_list": ["6201Z", "6202A"]},
            "company_size": {"present": True, "employees_number_range": "10 to 19 employees"},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": True}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 9 - Activité + Finance + Légal",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": True, "activity_codes_list": ["4690Z", "4617B"]},
            "company_size": {"present": False},
            "financial_criteria": {"present": True, "turnover": 1_000_000, "net_profit": 50_000, "profitability": 0.05},
            "legal_criteria": {"present": True, "headquarters": True, "legal_form": "5"}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 10 - Critères complets",
        "criteria": {
            "location": {"present": True, "departement": ["69", "75"]},
            "activity": {"present": True, "activity_codes_list": ["6202A", "6311Z", "6312Z"]},
            "company_size": {"present": True, "employees_number_range": ["20 to 49 employees", "50 to 99 employees"]},
            "financial_criteria": {"present": True, "turnover": 2_000_000, "net_profit": 100_000, "profitability": 0.05},
            "legal_criteria": {"present": True, "headquarters": True, "legal_form": "5", "capital": 10000, "capital_threshold_sup": True, "subsidiaries_number": 2}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 11 - Micro-entreprises récentes",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": False},
            "company_size": {"present": True, "employees_number_range": "0 employee"},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": True, "company_creation_date_threshold": "2023-01-01", "company_creation_date_sup": True}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 12 - Grandes entreprises avec filiales",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": False},
            "company_size": {"present": True, "employees_number_range": "500 to 999 employees"},
            "financial_criteria": {"present": True, "turnover": 50_000_000, "net_profit": 2_000_000},
            "legal_criteria": {"present": True, "headquarters": True, "subsidiaries_number": 10}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 13 - Localisation par département",
        "criteria": {
            "location": {"present": True, "region": None, "departement": ["92", "75"], "post_code": None, "city": None},
            "activity": {"present": False},
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": True}
        },
        "expected": {"count_legal": {"op": ">", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 14 - Localisation par commune",
        "criteria": {
            "location": {"present": True, "region": None, "departement": None, "post_code": None, "city": ["Clamart", "Toulouse"]},
            "activity": {"present": False},
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": False}
        },
        "expected": {"count_legal": {"op": ">", "value": 100000}, "count_semantic": {"op": "<=", "value": 2000}}
    },
      {
        "name": "Test 15 - Activités NAF multiples avec stop xwords et lemmatisation",
        "criteria": {
            "location": {
                "present": True,
                "region": None,
                "departement": None,
                "post_code": None,
                "city": None
            },            
            "activity": {
                "present": True,
                "activity_codes_list": ["6201Z", "6202A", "6203Z"],
                "original_activity_request": "farines de ble"
            },
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {
                "present": True,
                "headquarters": True,
                "legal_category": "Société commerciale",
                "company_creation_date_threshold": None,
                "company_creation_date_sup": None,
                "company_creation_date_inf": None,
            }        },
        "expected": {"count_legal": {"op": ">", "value": 5000}, "count_semantic": {"op": "<=", "value": 70}}
    },

    {
        "name": "Test 16 - Activités NAF multiples avec apostrophe",
        "criteria": {
            "location": {
                "present": True,
                "region": None,
                "departement": None,
                "post_code": None,
                "city": None
            },            
            "activity": {
                "present": True,
                "activity_codes_list": ["6619B", " 6619B"],
                "original_activity_request": "banque d'investissements"
            },
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {
                "present": True,
                "headquarters": True,
                "legal_category": "Société commerciale",
                "company_creation_date_threshold": None,
                "company_creation_date_sup": None,
                "company_creation_date_inf": None,
            }        },
        "expected": {"count_legal": {"op": ">", "value": 8000}, "count_semantic": {"op": "<=", "value": 5000}}
    },

    {
        "name": "Test 17 - Activités NAF multiples avec apostrophe",
        "criteria": {
            "location": {
                "present": True,
                "region": None,
                "departement": None,
                "post_code": None,
                "city": None
            },            
            "activity": {
                "present": True,
                "activity_codes_list": ["9319Z"],
                "original_activity_request": "e-sport"
            },
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {
                "present": True,
                "headquarters": True,
                "legal_category": "Société commerciale",
                "company_creation_date_threshold": None,
                "company_creation_date_sup": None,
                "company_creation_date_inf": None,
            }        },
        "expected": {"count_legal": {"op": ">", "value": 3000}, "count_semantic": {"op": ">", "value": 40}}
    },

     {
        "name": "Test 18 - Critères complets",
        "criteria": {
            "location": {"present": True, "region": ["Occitanie"], "departement": ["92", "75"], "post_code":  ["92140"], "city": ["Clamart", "Toulouse"]},

            "activity": {
                "present": True,
                "activity_codes_list": ["1071C"],
                "original_activity_request": "boulangerie"
            },            
            "company_size": {"present": True, "employees_number_range": ["10 to 19 employees"]},
            "financial_criteria": {
              "present": True,
              
              "turnover": 20000,
              "turnover_sup": True,
              "turnover_inf": False,
              
              "net_profit": 5000,
              "net_profit_sup": True,
              "net_profit_inf": False,
              
              "profitability": 0.01,
              "profitability_sup": True,
              "profitability_inf": False
            },            
            "legal_criteria": {"present": True, "headquarters": True, "legal_form": "5", "capital": 10000, "capital_threshold_sup": True, "subsidiaries_number": 1}
        },
        "expected": {"count_legal": {"op": ">", "value": 15}, "count_semantic": {"op": ">", "value": 10}}
    },

]



def compare(actual, operator, expected):
    if operator == "==":
        return actual == expected
    if operator == "!=":
        return actual != expected
    if operator == ">":
        return actual > expected
    if operator == ">=":
        return actual >= expected
    if operator == "<":
        return actual < expected
    if operator == "<=":
        return actual <= expected
    raise ValueError(f"Unsupported operator: {operator}")

if VERSION == "V1":
    #API_KEYS = json.loads(os.getenv("API_KEYS_V2"))
    print(os.getenv("API_KEYS"))
    #sys.exit();
    API_KEYS = json.loads(os.getenv("API_KEYS")) 

    # Get API key from environment variable
    if not API_KEYS:
        raise ValueError("API_KEYS environment variable not set!")

elif VERSION == "V2":
    print(os.getenv("API_KEYS_V2"))
    #sys.exit();
    API_KEYS = json.loads(os.getenv("API_KEYS_V2")) 

    # Get API key from environment variable
    if not API_KEYS:
        raise ValueError("API_KEYS environment variable not set!")


def parse_args():
    parser = argparse.ArgumentParser(description="Run API bot tests")
    parser.add_argument('--server', required=True, help='Server address')
    parser.add_argument('test_numbers', nargs='?', help='Test numbers to replay, e.g., 1-5-12')
    return parser.parse_args()

args = parse_args()

# Convert test_numbers to list of integers
test_numbers = None  # None = run all tests

print(args.test_numbers.lower())
if args.test_numbers:
    if args.test_numbers.lower() == "all":
        test_numbers = None
    else:
        try:
            test_numbers = [int(x) for x in args.test_numbers.split('-')]
        except ValueError:
            print('Invalid format. Use "all", a number, or numbers separated by "-" (e.g. 1-5-12)')
            exit(1)
# Use the arguments
server = args.server
print("Server:", server)
print("Test numbers to replay:", test_numbers)


def test_api(test_case):

    """
    Test the API with a specific test case including API key authentication
    """
   

    # Load API keys + secrets from env variable


    # print("API_KEYS")
    # print(API_KEYS)
    # Choose the client you want to use
    #secret = API_KEYS[api_key]


    # Timestamp
    timestamp = str(int(time.time()))

    if VERSION == "V1":

        API_KEYS    = json.loads(os.getenv("API_KEYS", "{}"))
        api_key = list(API_KEYS.keys())[0]       # take the first key (or pick specific)

        headers = {
            "X-Api-Key": api_key
        }

    elif VERSION == "V2":
        API_KEYS = json.loads(os.getenv("API_KEYS_V2", "{}"))

        # Pick the first API key in the dictionary
        api_key = list(API_KEYS.keys())[0]  # e.g., "3fa036..."
        secret = API_KEYS[api_key]          # Corresponding secret

        # Timestamp
        timestamp = str(int(time.time()))

        # HMAC signature
        signature = hmac.new(
            secret.encode(),
            f"{api_key}{timestamp}".encode(),
            hashlib.sha256
        ).hexdigest()

        # Send request
        headers = {
            "X-Api-Key": api_key,
            "X-Timestamp": timestamp,
            "X-Signature": signature
        }
    try:
        response = requests.post(
            API_URL,
            json=test_case["criteria"],
            headers=headers,
            timeout=60
        )
        return {
            "test_name": test_case["name"],
            "status_code": response.status_code,
            "request": test_case["criteria"],
            "response": response.json() if response.status_code == 200 else {"error": response.text},
            "success": response.status_code == 200
        }

    except requests.exceptions.ConnectionError:
        return {
            "test_name": test_case["name"],
            "status_code": None,
            "request": test_case["criteria"],
            "response": {"error": "Impossible de se connecter à l'API"},
            "success": False
        }

    except Exception as e:
        return {
            "test_name": test_case["name"],
            "status_code": None,
            "request": test_case["criteria"],
            "response": {"error": str(e)},
            "success": False
        }


def run_all_tests(test_numbers: Optional[List[int]] = None):
    """
    Execute all API tests or a subset of tests
    """
    print("=" * 80)
    print("TESTS API - COMPANY TARGETING")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"URL: {API_URL}")
    print("=" * 80)
    print()

    results = []
    success_count = 0
    tab_errors = []

    # Determine which tests to run
    if test_numbers:
        tests_to_run = []
        for n in test_numbers:
            if 1 <= n <= len(test_cases):
                tests_to_run.append((n, test_cases[n - 1]))
            else:
                print(f"Erreur: test_number {n} hors limites (1-{len(test_cases)})")
    else:
        tests_to_run = list(enumerate(test_cases, 1))  # all tests

    # Run tests
    for i, test_case in tests_to_run:
        print(f"Running test {i}: {test_case.get('name')}")

        start_time = time.perf_counter()
        result = test_api(test_case)
        end_time = time.perf_counter()
        duration = end_time - start_time

        results.append(result)

        response = result.get('response', {})
        success_http = result.get('success', False)

        count_legal = response.get('count_legal')
        count_semantic = response.get('count_semantic')

      # Mapping string operators to Python functions
        OPS = {
            ">": operator.gt,
            "<": operator.lt,
            ">=": operator.ge,
            "<=": operator.le,
            "==": operator.eq,
            "!=": operator.ne
        }

        # Extract expected values
        expected = test_case.get('expected', {})
        exp_legal = expected.get('count_legal', {})
        exp_semantic = expected.get('count_semantic', {})

        exp_op_legal = exp_legal.get('op')
        exp_value_legal = exp_legal.get('value')

        exp_op_semantic = exp_semantic.get('op')
        exp_value_semantic = exp_semantic.get('value')

        # ---------- BUSINESS VALIDATION ----------
        business_ok = True

        print(f"exp_legal: {exp_legal}")
        print(f"count_legal: {count_legal}")

        # Use the operator dynamically
        if exp_op_legal and exp_value_legal is not None:
            if count_legal is None or not OPS[exp_op_legal](count_legal, exp_value_legal):
                business_ok = False
                tab_errors.append(f"test_number:{test_case}; count_legal expected {exp_op_legal} {exp_value_legal}, got {count_legal}")
                tab_errors.append(test_case)

        if exp_op_semantic and exp_value_semantic is not None:
            if count_semantic is None or not OPS[exp_op_semantic](count_semantic, exp_value_semantic):
                business_ok = False
                tab_errors.append(f"test_number:{test_case}; count_semantic expected {exp_op_semantic} {exp_value_semantic}, got {count_semantic}")
                tab_errors.append(test_case)

        success = success_http and business_ok
        status = "✓ SUCCÈS" if success else "✗ ÉCHEC"

        print(
            f"{status} - "
            f"Count Legal: {count_legal}, "
            f"Count Semantic: {count_semantic} "
            f"(Durée: {duration:.3f}s)"
        )

        if tab_errors:
            print("  ❌ Validation errors:")
            for err in tab_errors:
                print(f"    - {err}")

            # ---------- Print individual counts for KO tests ----------
            activity_counts = response.get('activity_individual_counts')
            if activity_counts:
                print("  Individual activity_code counts (KO test):")
                for code, info in activity_counts.items():
                    if isinstance(info, dict):
                        print(f"    - {code}: {info.get('count_legal', 0)}")
                    else:
                        print(f"    - {code}: {info}")

        # ---------- Full JSON request ----------
        print("\n  Full request JSON:")
        print(json.dumps(test_case, indent=2, ensure_ascii=False))

        # ---------- Full JSON response ----------
        print("\n  Full response JSON:")
        print(json.dumps(response, indent=2, ensure_ascii=False))

        print("-" * 80)

        if success:
            success_count += 1

    # Résumé
    print("=" * 80)
    print("RÉSUMÉ DES TESTS")
    print("=" * 80)
    print(f"Tests réussis: {success_count}/{len(results)}")
    print(f"Tests échoués: {len(results) - success_count}/{len(results)}")
    print()

    # Sauvegarde des résultats détaillés
    output_file = f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            "timestamp": datetime.now().isoformat(),
            "api_url": API_URL,
            "total_tests": len(results),
            "successful_tests": success_count,
            "failed_tests": len(results) - success_count,
            "results": results
        }, f, indent=2, ensure_ascii=False)

    #print(f"Résultats détaillés sauvegardés dans: {output_file}")
    return results, tab_errors



def display_detailed_result(result):
    """
    Affiche un résultat détaillé au format lisible
    """
    print("\n" + "=" * 80)
    print(f"TEST: {result.get('test_name', 'N/A')}")
    print("=" * 80)
    
    # Request JSON
    print("\nREQUÊTE (JSON):")
    print(json.dumps(result.get('request', {}), indent=2, ensure_ascii=False))
    
    # Response JSON
    response = result.get('response', {})
    print("\nRÉPONSE (JSON complète):")
    print(json.dumps(response, indent=2, ensure_ascii=False))

    # Optionally, highlight main counts for quick glance
    count_legal = response.get('count_legal', 0)
    count_semantic = response.get('count_semantic', 0)
    print(f"\n  → Count Legal: {count_legal}")
    print(f"  → Count Semantic: {count_semantic}")

    # # Individual activity counts
    # activity_counts = response.get('activity_individual_counts')
    # if activity_counts:
    #     print("  → Individual activity_code counts:")
    #     for code, info in activity_counts.items():
    #         if isinstance(info, dict):
    #             print(f"    - {code}: {info.get('count', 0)}")
    #             print(f"      SQL preview: {info.get('sql_preview', '')}")
    #         else:
    #             print(f"    - {code}: {info}")

    # Criteria applied
    criteria_applied = response.get('criteria_applied', {})
    print("\n  → Criteria applied:")
    for key, val in criteria_applied.items():
        print(f"    {key}: {val}")

    # Full SQL preview
    debug_sql = response.get('debug_sql')
    if debug_sql:
        print("\n  → Full SQL preview:")
        print(debug_sql)
    
    # Status
    status = "✓ SUCCÈS" if result.get('success') else "✗ ÉCHEC"
    print("\nSTATUT:", status)
    print("=" * 80)

def main():

    # API_KEYS = os.getenv("API_KEYS")
    # if not API_KEYS:
    #     raise ValueError("API_KEYS environment variable not set!")

    parser = argparse.ArgumentParser(description="Run API tests or integration")
    parser.add_argument(
        "--server",
        type=str,
        required=True,
        help="IP or hostname of the server where the API is running"
    )
    parser.add_argument(
        'test_numbers',
        nargs='?',
        help='Test numbers to replay, e.g., 3 or 1-5-12'
    )

    args = parser.parse_args()
    server_ip = args.server

    # # Convert test_numbers to list of integers
    # test_numbers = []
    # if args.test_numbers:
    #     try:
    #         test_numbers = [int(x) for x in args.test_numbers.split('-')]
    #     except ValueError:
    #         print("Invalid format. Use numbers separated by '-' e.g., 1-5-12")
    #         exit(1)

    # Example: construct API URL dynamically
    global API_URL
    if VERSION == "V2":
        API_URL = f"http://{server_ip}:5001/count_bot_v2"
    elif VERSION == "V1":
        API_URL = f"http://{server_ip}:5001/count_bot_v1"

    print(f"Using API URL: {API_URL}")
    print(f"Test numbers to replay: {test_numbers}")

    # Run tests (pass test_numbers to filter if needed)
    results, errors = run_all_tests(test_numbers=test_numbers)

    print("\n✓ Tests terminés!")

    # Display errors in pairs
    for i in range(0, len(errors), 2):
        error_msg = errors[i]
        test_case = errors[i+1]
        
        print("------ ERROR ------")
        print(error_msg)
        print("------ TEST CASE ------")
        print(f"Name: {test_case.get('name')}")
        print(f"Criteria: {test_case.get('criteria')}")
        print(f"Expected: {test_case.get('expected')}")
        print("\n")
        

if __name__ == "__main__":
    main()
