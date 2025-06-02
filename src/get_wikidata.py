import requests
import pandas as pd
import time

def fetch_wikidata(tech_names, delay=0.5, top_n=1):
    """
    Fetches QID, label, and description for each technology name from Wikidata.
    Not meant for data mining — just entity discovery. From the QIDs, you can get the full data.

    Parameters:
        tech_names (list of str): List of emerging technology names.
        delay (float): Delay between API requests to avoid rate-limiting.
        top_n (int): Number of top search results to return per term (default 1).

    Returns:
        pd.DataFrame: DataFrame with columns ['name', 'qid', 'label', 'description', 'match_type']
    """
    url = "https://www.wikidata.org/w/api.php"
    headers = {"User-Agent": "EmergingTechGraph/1.0 (fpovina@outlook.com)"} # helps avoid getting blocked — can personalize it
    results = []

    for name in tech_names:
        params = {
            "action": "wbsearchentities",
            "language": "en",
            "format": "json",
            "search": name
        }

        try:
            response = requests.get(url, params=params, headers=headers)
            data = response.json()

            if "search" in data:
                for entry in data["search"][:top_n]:
                    results.append({
                        "name": name,
                        "qid": entry.get("id"),
                        "label": entry.get("label"),
                        "description": entry.get("description"),
                        "match_type": entry.get("match", {}).get("type", "")
                    })
            else:
                results.append({
                    "name": name,
                    "qid": None,
                    "label": None,
                    "description": None,
                    "match_type": "no match"
                })

            time.sleep(delay)

        except Exception as e:
            print(f"Error while processing '{name}':", str(e))
            results.append({
                "name": name,
                "qid": None,
                "label": None,
                "description": None,
                "match_type": "error"
            })

    return pd.DataFrame(results)
