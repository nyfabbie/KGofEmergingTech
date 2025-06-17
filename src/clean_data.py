"""
Basic cleaners that turn raw fetch results into
✓ one DataFrame of Technology nodes
✓ one DataFrame of Paper nodes
✓ one DataFrame of Paper-MENTIONS-Tech edges
"""

import ast
import hashlib
import pandas as pd
from rapidfuzz import fuzz, process  # Add this import at the top
import re
import json
import os
from dotenv import load_dotenv

# ---------- helpers -------------------------------------------------

def _normalise(text: str) -> str:
    return text.lower().strip()

def normalize_name(name):
    import re
    if pd.isnull(name):
        return ""
    return re.sub(r'[^a-z0-9]', '', name.lower())

def _paper_id(arxiv_url: str) -> str:
    """E.g 2406.04641v1  →  2406.04641v1   (unique + short)"""
    return arxiv_url.rsplit("/", 1)[-1]

def extract_funding_total_and_currency(row):
    """
    Extracts the most accurate funding_total and funding_currency for a Crunchbase company row.
    Priority:
    1. funds_raised (JSON, single company)
    2. financials_highlights (JSON, single company)
    3. funding_total (JSON, single company)
    4. featured_list (company-specific entry)
    5. featured_list (first entry, fallback)
    """
    import json
    # 1. funds_raised
    if "funds_raised" in row and pd.notnull(row["funds_raised"]) and row["funds_raised"] != "":
        try:
            data = json.loads(row["funds_raised"])
            if isinstance(data, dict) and "value_usd" in data:
                return data["value_usd"], data.get("currency", "USD")
        except Exception:
            pass
    # 2. financials_highlights
    if "financials_highlights" in row and pd.notnull(row["financials_highlights"]) and row["financials_highlights"] != "":
        try:
            data = json.loads(row["financials_highlights"])
            if isinstance(data, dict) and "value_usd" in data:
                return data["value_usd"], data.get("currency", "USD")
        except Exception:
            pass
    # 3. funding_total (JSON)
    if "funding_total" in row and pd.notnull(row["funding_total"]) and row["funding_total"] != "":
        try:
            data = json.loads(row["funding_total"])
            if isinstance(data, dict) and "value_usd" in data:
                return data["value_usd"], data.get("currency", "USD")
        except Exception:
            pass
    # 4. featured_list (company-specific entry)
    if "featured_list" in row and pd.notnull(row["featured_list"]) and row["featured_list"] != "":
        try:
            data = json.loads(row["featured_list"])
            if isinstance(data, list) and "name" in row and row["name"]:
                for entry in data:
                    if "org_funding_total" in entry and "title" in entry:
                        if row["name"].lower() in entry["title"].lower():
                            org_funding = entry["org_funding_total"]
                            value = org_funding.get("value_usd") if org_funding.get("currency", "USD") == "USD" else org_funding.get("value")
                            currency = org_funding.get("currency", "USD")
                            return value, currency
        except Exception:
            pass
    # 5. featured_list (first entry, fallback, but only if it's not an obvious aggregate)
    if "featured_list" in row and pd.notnull(row["featured_list"]) and row["featured_list"] != "":
        try:
            data = json.loads(row["featured_list"])
            if isinstance(data, list) and len(data) > 0 and "org_funding_total" in data[0]:
                # Only use if the title matches the company name (already checked above), otherwise skip (likely aggregate)
                return None, None
        except Exception:
            pass
    return None, None

def extract_funding_total_and_currency_from_featured_list(cell, company_name=None):
    if pd.isnull(cell) or not isinstance(cell, str) or cell.strip() == "":
        return None, None
    try:
        data = json.loads(cell)
        # Try to find an entry where the title contains the company name
        if company_name and isinstance(data, list):
            for entry in data:
                if "org_funding_total" in entry and "title" in entry:
                    if company_name.lower() in entry["title"].lower():
                        org_funding = entry["org_funding_total"]
                        value = org_funding.get("value_usd") if org_funding.get("currency", "USD") == "USD" else org_funding.get("value")
                        currency = org_funding.get("currency", "USD")
                        return value, currency
        # Otherwise, just use the first entry
        if isinstance(data, list) and len(data) > 0 and "org_funding_total" in data[0]:
            org_funding = data[0]["org_funding_total"]
            value = org_funding.get("value_usd") if org_funding.get("currency", "USD") == "USD" else org_funding.get("value")
            currency = org_funding.get("currency", "USD")
            return value, currency
        elif isinstance(data, dict) and "org_funding_total" in data:
            org_funding = data["org_funding_total"]
            value = org_funding.get("value_usd") if org_funding.get("currency", "USD") == "USD" else org_funding.get("value")
            currency = org_funding.get("currency", "USD")
            return value, currency
    except Exception:
        return None, None
    return None, None

def extract_location_from_json(cell):
    """Extracts a comma-separated location string from a JSON list of location dicts."""
    if pd.isnull(cell) or not isinstance(cell, str) or cell.strip() == "":
        return None
    try:
        data = json.loads(cell)
        if isinstance(data, list):
            names = [entry["name"] for entry in data if "name" in entry]
            return ", ".join(names)
    except Exception:
        return None
    return None

def clean_startups(startups_df, cb_info_df):
    """
    Clean and convert columns before passing to Neo4j.
    Always extracts funding_total, funding_currency, and location from Crunchbase JSON columns.
    """
    # Using extraction logic
    funding_info = cb_info_df.apply(lambda row: extract_funding_total_and_currency(row), axis=1)
    cb_info_df["funding_total"] = funding_info.apply(lambda x: x[0])
    cb_info_df["funding_currency"] = funding_info.apply(lambda x: x[1])

    # Only process if Series is not empty and has at least one non-null value and is object dtype
    if not cb_info_df['funding_total'].empty and not cb_info_df['funding_total'].dropna().empty:
        if cb_info_df['funding_total'].dtype == object:
            cb_info_df['funding_total'] = (
                cb_info_df['funding_total']
                .replace({',': '', r'\s*-\s*': ''}, regex=True)
                .replace('', pd.NA)
            )
        cb_info_df['funding_total'] = pd.to_numeric(cb_info_df['funding_total'], errors='coerce')

    # Extract location from location column if present and add as 'location_extracted'
    if "location" in cb_info_df.columns:
        cb_info_df["location_extracted"] = cb_info_df["location"].apply(extract_location_from_json)

    if "funding_total_usd" in startups_df.columns and not startups_df['funding_total_usd'].empty and not startups_df['funding_total_usd'].dropna().empty:
        if startups_df['funding_total_usd'].dtype == object:
            startups_df['funding_total_usd'] = (
                startups_df['funding_total_usd']
                .replace({',': '', r'\s*-\s*': ''}, regex=True)
                .replace('', pd.NA)
            )
        startups_df['funding_total_usd'] = pd.to_numeric(startups_df['funding_total_usd'], errors='coerce')
    return startups_df, cb_info_df


def enrich_and_merge_startups(startups_yc, startups_crunchbase, cb_info_df):
    """
    Enrich YC startups with Crunchbase data, deduplicate, and filter out those already in cb_info_df.
    Returns startups_df_filtered (ready for Neo4j loading).
    """
    # Add normalized name columns
    startups_yc["norm_name"] = startups_yc["name"].apply(normalize_name)
    startups_crunchbase["norm_name"] = startups_crunchbase["name"].apply(normalize_name)
    cb_info_df["norm_name"] = cb_info_df["name"].apply(normalize_name)

    # Merge YC-labeled startups with Crunchbase data by normalized name
    startups_df = startups_yc.merge(
        startups_crunchbase[
            ['norm_name', 'homepage_url', 'category_list', 'funding_total_usd', 'status', 'region']
        ],
        on="norm_name", how="left", suffixes=('', '_crunchbase')
    )
    # Remove all commas and convert to int/float
    startups_df['funding_total_usd'] = (
        startups_df['funding_total_usd']
        .replace({',': '', r'\s*-\s*': ''}, regex=True)  # Remove commas and lone dashes
        .replace('', pd.NA)  # Replace empty strings with NA
    )
    startups_df['funding_total_usd'] = pd.to_numeric(startups_df['funding_total_usd'], errors='coerce')

    # Ensures 'by' columns are not null when dropping duplicates
    startups_df = startups_df.sort_values(
        by=['funding_total_usd'],
        ascending=[False],
        na_position='last'
    )
    startups_df = startups_df.drop_duplicates(subset=['norm_name'], keep='first')

    # Add YC+Crunchbase merged startups only if not already present in cb_info_df
    existing_names = set(cb_info_df["norm_name"])
    startups_df_filtered = startups_df[~startups_df["norm_name"].isin(existing_names)]
    return startups_df_filtered

def deduplicate_startups(startups_df1, startups_df2, threshold=92):
    """
    Deduplicate startups across two DataFrames using normalized names and fuzzy matching.
    Returns two DataFrames with a new 'startup_id' column (canonical normalized name).
    """
    # Normalize names
    startups_df1 = startups_df1.copy()
    startups_df2 = startups_df2.copy()
    startups_df1['norm_name'] = startups_df1['name'].apply(normalize_name)
    startups_df2['norm_name'] = startups_df2['name'].apply(normalize_name)

    # Build mapping from norm_name in df2 to df1 using fuzzy matching
    id_map = {}
    used = set()
    for n1 in startups_df1['norm_name'].unique():
        # Find best match in df2
        matches = process.extract(n1, startups_df2['norm_name'].unique(), scorer=fuzz.ratio, limit=1)
        if matches and matches[0][1] >= threshold:
            n2 = matches[0][0]
            id_map[n2] = n1
            used.add(n1)
    # Assign startup_id: for df1 it's its own norm_name, for df2 it's mapped if matched, else its own
    startups_df1['startup_id'] = startups_df1['norm_name']
    startups_df2['startup_id'] = startups_df2['norm_name'].apply(lambda n: id_map[n] if n in id_map else n)
    return startups_df1, startups_df2

def clean_arxiv(raw_list: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    csv_rows = pd.read_csv("data/arxiv_papers_res.csv")
    csv_rows["paper_id"] = csv_rows["id"].map(_paper_id)
    csv_rows["published"] = pd.to_datetime(csv_rows["published"])
    csv_rows["authors"] = csv_rows["authors"].apply(
        lambda s: ast.literal_eval(s) if isinstance(s, str) else s
    )

    papers_df = (
        csv_rows[["paper_id", "id", "title", "summary", "published"]]
        .drop_duplicates("paper_id")
    )

    return papers_df

# Load .env if present
load_dotenv()

# Load canonical techs and synonyms from JSON
EMERGING_TECHS_JSON = os.getenv("EMERGING_TECHS", "data/emerging_techs.json")
TECH_SYNONYMS = {}
try:
    with open(EMERGING_TECHS_JSON, encoding='utf-8') as f:
        TECH_SYNONYMS = json.load(f)
except Exception:
    TECH_SYNONYMS = {}

def match_startups_to_techs(startups_df, techs_df, text_columns=None, threshold=85):
    """
    Fuzzy matches startups to technologies using rapidfuzz.
    Returns a DataFrame with columns: startup_name, technology, qid, score.
    text_columns: list of columns to use for text matching (default: long_description, industry, short_description, tags, name)
    """
    matches = []
    # Build a mapping from synonym to canonical tech name and QID
    synonym_to_canonical_qid = {}
    for _, tech in techs_df.iterrows():
        tech_name = tech['name']
        qid = tech.get('qid', None)
        for synonym in TECH_SYNONYMS.get(tech_name, [tech_name]):
            synonym_to_canonical_qid[synonym.lower()] = (tech_name, qid)

    # Default columns if not provided
    if text_columns is None:
        text_columns = ['long_description', 'industry', 'short_description', 'tags', 'name']

    for idx, row in startups_df.iterrows():
        # Lowercase for fuzzy matching, but keep original for short synonym regex
        text = " ".join([
            str(row.get(col, '')) for col in text_columns
        ])
        text_lower = text.lower()
        for synonym, (canonical, qid) in synonym_to_canonical_qid.items():
            clean_synonym = synonym.strip()
            # Dynamic threshold: 95 if any word in the synonym is <4 chars, else normal
            words = clean_synonym.split()
            dynamic_threshold = 95 if any(len(w) <= 4 for w in words) else threshold

            if len(clean_synonym) <= 3:
                # Only match as a whole word, case-insensitive, in the original text
                if re.search(rf"\b{re.escape(clean_synonym)}\b", text, re.IGNORECASE):
                    score = 100
                else:
                    score = 0
            else:
                score = fuzz.token_set_ratio(clean_synonym.lower(), text_lower)
            if score >= dynamic_threshold and qid is not None:
                matches.append({
                    "startup_name": row.get("name"),
                    "technology": canonical,
                    "qid": qid,
                    "score": score
                })
    matches_df = pd.DataFrame(matches)
    # Keep only the row with the highest score for each (startup_name, technology) pair
    matches_df = matches_df.sort_values("score", ascending=False).drop_duplicates(subset=["startup_name", "technology"], keep="first")
    
    return matches_df

def match_papers_to_tech(papers_raw, techs_df):
    """
    Maps each paper to the QID of its technology (using the technology column in papers_raw and the name/qid in techs_df).
    Saves a CSV with columns: id, qid.
    """
    tech_name_to_qid = dict(zip(techs_df['name'], techs_df['qid']))
    papers_raw["paper_id"] = papers_raw["id"].map(_paper_id)

    mapped = []
    for _, row in papers_raw.iterrows():
        tech_name = row.get('technology')
        qid = tech_name_to_qid.get(tech_name)
        if qid:
            mapped.append({
                'paper_id': row.get('paper_id'),
                'qid': qid
            })
    mapped_df = pd.DataFrame(mapped)        
    return mapped_df

