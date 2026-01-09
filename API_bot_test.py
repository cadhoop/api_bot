import requests
import json
from datetime import datetime
import time
import os



# Configuration
API_URL = "http://localhost:5001/count_bot_v1"

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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 3 - Activités NAF multiples",
        "criteria": {
            "location": {"present": False},
            "activity": {
                "present": True,
                "activity_codes_list": ["6201Z", "6202A", "6203Z"],
                "original_activity_request": "Services informatiques"
            },
            "company_size": {"present": False},
            "financial_criteria": {"present": False},
            "legal_criteria": {"present": True, "headquarters": True}
        },
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 9 - Activité + Finance + Légal",
        "criteria": {
            "location": {"present": False},
            "activity": {"present": True, "activity_codes_list": ["4690Z", "4617B"]},
            "company_size": {"present": False},
            "financial_criteria": {"present": True, "turnover": 1_000_000, "net_profit": 50_000, "profitability": 0.05},
            "legal_criteria": {"present": True, "headquarters": True, "legal_category": "SAS"}
        },
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    },

    {
        "name": "Test 10 - Critères complets",
        "criteria": {
            "location": {"present": True, "departement": ["69", "75"]},
            "activity": {"present": True, "activity_codes_list": ["6202A", "6311Z", "6312Z"]},
            "company_size": {"present": True, "employees_number_range": ["20 to 49 employees", "50 to 99 employees"]},
            "financial_criteria": {"present": True, "turnover": 2_000_000, "net_profit": 100_000, "profitability": 0.05},
            "legal_criteria": {"present": True, "headquarters": True, "legal_category": "Societe commerciale", "capital": 10000, "capital_threshold_sup": True, "subsidiaries_number": 2}
        },
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
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
        "expected": {"count_legal": {"op": ">=", "value": 0}, "count_semantic": {"op": "<=", "value": 2000}}
    }

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


# Get API key from environment variable
API_KEY = os.getenv("API_KEY")
if not API_KEY:
    raise ValueError("API_KEY environment variable not set!")

API_URL = "http://localhost:5001/count_bot_v1"

def test_api(test_case):
    """
    Test the API with a specific test case including API key authentication
    """
    headers = {
    "Content-Type": "application/json",
    "X-API-Key": API_KEY  # Use X-API-Key instead of Authorization
}
    #print("Sending request with headers test_api:", headers)

    try:
        response = requests.post(
            API_URL,
            json=test_case["criteria"],
            headers=headers,
            timeout=30
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



def run_all_tests(test_number: int = None):
    """
    Execute all API tests (or a specific test) with API key authentication
    """
    print("=" * 80)
    print("TESTS API - COMPANY TARGETING")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"URL: {API_URL}")
    print("=" * 80)
    print()

    results = []
    success_count = 0

    # Determine which tests to run
    if test_number is not None:
        if 1 <= test_number <= len(test_cases):
            tests_to_run = [(test_number, test_cases[test_number-1])]
        else:
            print(f"Erreur: test_number {test_number} hors limites (1-{len(test_cases)})")
            return []
    else:
        tests_to_run = list(enumerate(test_cases, 1))  # all tests

    for i, test_case in tests_to_run:
        print(f"Exécution du test {i}/{len(test_cases)}: {test_case['name']}")

        start_time = time.perf_counter()
        result = test_api(test_case)
        end_time = time.perf_counter()
        duration = end_time - start_time

        results.append(result)

        response = result.get('response', {})
        success_http = result.get('success', False)

        count_legal = response.get('count_legal')
        count_semantic = response.get('count_semantic')

        expected = test_case.get('expected', {})
        exp_legal = expected.get('company_number_legal')
        exp_semantic = expected.get('company_number_semantic')

        # ---------- BUSINESS VALIDATION ----------
        business_ok = True
        errors = []

        if exp_legal is not None and count_legal != exp_legal:
            business_ok = False
            errors.append(f"count_legal expected {exp_legal}, got {count_legal}")

        if exp_semantic is not None and count_semantic != exp_semantic:
            business_ok = False
            errors.append(f"count_semantic expected {exp_semantic}, got {count_semantic}")

        success = success_http and business_ok
        status = "✓ SUCCÈS" if success else "✗ ÉCHEC"

        print(
            f"{status} - "
            f"Count Legal: {count_legal}, "
            f"Count Semantic: {count_semantic} "
            f"(Durée: {duration:.3f}s)"
        )

        if errors:
            print("  ❌ Validation errors:")
            for err in errors:
                print(f"    - {err}")

        # ---------- Individual activity counts ----------
        activity_counts = response.get('activity_individual_counts')
        if activity_counts:
            print("  Individual activity_code counts:")
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

    print(f"Résultats détaillés sauvegardés dans: {output_file}")
    return results



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



if __name__ == "__main__":


    # Get API key from environment variable
    API_KEY = os.getenv("API_KEY")

    #print("API_KEY:", os.getenv("API_KEY"))

    if not API_KEY:
        raise ValueError("API_KEY environment variable not set!")

     # Run only test number 3
    #results = run_all_tests(test_number=3)
    results = run_all_tests()

    # Display a detailed result
    for idx, result in enumerate(results, 1):
        print(f"\n\n=== Détail du test {idx} ===")
        display_detailed_result(result)
    
    print("\n✓ Tests terminés!")