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
    Cleans startup data from YCombinator and Crunchbase (cb_info_df).
    Focuses on normalizing names, extracting and cleaning funding, location.
    """
    # Clean YC startups (passed as startups_df)
    if 'name' in startups_df.columns:
        startups_df['original_name_yc'] = startups_df['name']
        startups_df['name'] = startups_df['name'].astype(str).apply(_normalise)

    # Ensure 'funding_total_usd' column exists for YC startups.
    # YC data as loaded initially won't have 'all_time_funding' or 'funding_total_usd'.
    # Create 'funding_total_usd' if it doesn't exist.
    if 'funding_total_usd' not in startups_df.columns:
        print("\n--- clean_startups: YC startups - 'funding_total_usd' not found, creating empty 'funding_total_usd' column. ---")
        startups_df['funding_total_usd'] = pd.NA

    # Now, clean 'funding_total_usd'; this will work even if it was just created as all NA
    if 'funding_total_usd' in startups_df.columns: # Should always be true now
        startups_df['funding_total_usd'] = startups_df['funding_total_usd'].astype(str).replace(['', 'nan', 'None', 'NaN', 'NaT', '<NA>'], pd.NA)
        startups_df['funding_total_usd'] = pd.to_numeric(startups_df['funding_total_usd'], errors='coerce')
    
    print("\n--- clean_startups: YC startups after initial cleaning (sample) ---")

    # Clean Crunchbase Info (cb_info_df)
    if 'name' in cb_info_df.columns:
        cb_info_df['original_name_cb_info'] = cb_info_df['name']
        cb_info_df['name'] = cb_info_df['name'].astype(str).apply(_normalise)

    # Always attempt to extract funding from JSON fields for Crunchbase/Brightdata
    print("\n--- clean_startups: cb_info_df - Extracting funding from JSON fields (funds_raised, financials_highlights, funding_total, featured_list) ---")
    funding_extracted_series = cb_info_df.apply(
        lambda row: extract_funding_total_and_currency(row),
        axis=1
    )
    temp_funding_df = pd.DataFrame(funding_extracted_series.tolist(), index=cb_info_df.index, columns=['funding_amount_from_json', 'funding_currency_from_json'])
    cb_info_df['funding_total_usd_json'] = pd.to_numeric(temp_funding_df['funding_amount_from_json'], errors='coerce')
    # Add 'funding_currency_from_json' if it's not already there
    cb_info_df['funding_currency_from_json'] = temp_funding_df['funding_currency_from_json']

    # Initialize 'funding_total_usd_direct' from existing 'funding_total_usd' or set to NA
    if 'funding_total_usd' in cb_info_df.columns:
        print("\n--- clean_startups: cb_info_df - Processing existing 'funding_total_usd' column. ---")
        cb_info_df['funding_total_usd_direct'] = pd.to_numeric(
            cb_info_df['funding_total_usd'].astype(str).replace(['', 'nan', 'None', 'NaN', 'NaT', '<NA>', ' - '], pd.NA),
            errors='coerce'
        )
    else:
        print("\n--- clean_startups: cb_info_df - No direct 'funding_total_usd' column found, initializing helper with pd.NA. ---")
        cb_info_df['funding_total_usd_direct'] = pd.NA

    # Combine direct and JSON-extracted funding, then assign to 'funding_total_usd'
    cb_info_df['funding_total_usd'] = cb_info_df['funding_total_usd_direct'].combine_first(cb_info_df['funding_total_usd_json'])

    # Clean up helper columns
    cb_info_df.drop(columns=['funding_total_usd_direct', 'funding_total_usd_json'], inplace=True, errors='ignore')
    # Drop older helper column if it exists from previous runs/logic
    if 'funding_total_extracted_value' in cb_info_df.columns:
        cb_info_df.drop(columns=['funding_total_extracted_value'], inplace=True, errors='ignore')

    # Ensure 'funding_total_usd' exists even if both sources were absent
    if 'funding_total_usd' not in cb_info_df.columns:
        print("\n--- clean_startups: cb_info_df - 'funding_total_usd' was not created from sources, initializing with pd.NA. ---")
        cb_info_df['funding_total_usd'] = pd.NA
    
    print("\n--- clean_startups: cb_info_df after funding extraction/creation (sample with funding) ---")
    # Prepare columns for printing, ensuring they exist
    cols_to_print_cb = ['name']
    # Print all possible funding JSON columns for debugging
    for col in ['funds_raised', 'financials_highlights', 'funding_total', 'featured_list']:
        if col in cb_info_df.columns:
            cols_to_print_cb.append(col)
    if 'funding_total_usd' in cb_info_df.columns:
        cols_to_print_cb.append('funding_total_usd')
    if 'funding_currency_from_json' in cb_info_df.columns:
        cols_to_print_cb.append('funding_currency_from_json')
    existing_cols_to_print_cb = [col for col in cols_to_print_cb if col in cb_info_df.columns]
    if not cb_info_df.empty and 'funding_total_usd' in cb_info_df.columns:
        print(cb_info_df[cb_info_df['funding_total_usd'].notna()][existing_cols_to_print_cb].head())
    elif not cb_info_df.empty:
        print(cb_info_df[existing_cols_to_print_cb].head())
    else:
        print("cb_info_df is empty or funding_total_usd column is missing for sample printing.")

    # Extract location from 'location' (often JSON-like)
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

def merge_and_clean_startups(startups_yc, cb_info_df):
    """
    Merges YC and Crunchbase startup dataframes, prioritizing Crunchbase data
    for common startups (based on startup_id), and cleans funding columns.
    """
    print(f"\n--- merge_and_clean_startups: startups_yc before concat (sample with startup_id and funding) ---")
    print(startups_yc[['startup_id', 'name', 'funding_total_usd']].head())
    print(f"\n--- merge_and_clean_startups: cb_info_df before concat (sample with startup_id and funding) ---")
    print(cb_info_df[['startup_id', 'name', 'funding_total_usd']].head())

    # Concatenate, cb_info_df first to be kept during drop_duplicates
    all_startups_df = pd.concat([cb_info_df, startups_yc], ignore_index=True)
    print(f"\n--- merge_and_clean_startups: all_startups_df after concat, before drop_duplicates (sample) ---")
    print(all_startups_df[['startup_id', 'name', 'funding_total_usd']].head())
    print(f"Shape after concat: {all_startups_df.shape}")
    print(f"Duplicates in startup_id before drop: {all_startups_df.duplicated(subset=['startup_id']).sum()}")

    # Deduplicate based on startup_id, keeping the first occurrence (cb_info_df prioritized)
    all_startups_df = all_startups_df.drop_duplicates(subset=["startup_id"], keep="first")
    print(f"\n--- merge_and_clean_startups: all_startups_df after drop_duplicates (sample) ---")
    print(all_startups_df[['startup_id', 'name', 'funding_total_usd']].head())
    print(f"Shape after drop_duplicates: {all_startups_df.shape}")

    # Define potential funding columns in order of preference
    funding_cols = ['funding_total_usd', 'funding_total', 'funding'] 

    # Ensure all potential funding columns exist, if not, create them with pd.NA
    for col in funding_cols:
        if col not in all_startups_df.columns:
            all_startups_df[col] = pd.NA

    # Clean and convert funding columns to numeric
    for col in funding_cols:
        # Convert to string, replace common non-numeric/null representations
        all_startups_df[col] = all_startups_df[col].astype(str).replace(['', 'nan', 'None', 'NaN', 'NaT', '<NA>', 'unknown', '-'], pd.NA)
        # Attempt to convert to numeric, coercing errors
        all_startups_df[col] = pd.to_numeric(all_startups_df[col], errors='coerce')
    
    print(f"\n--- merge_and_clean_startups: all_startups_df after initial numeric conversion of funding columns (sample) ---")
    print(all_startups_df[['startup_id', 'name'] + funding_cols].head())

    # Unify funding columns into 'funding_total_usd'
    # Start with the first column as the base
    unified_funding = all_startups_df[funding_cols[0]].copy()
    # Iteratively combine with other columns
    for col in funding_cols[1:]:
        unified_funding = unified_funding.combine_first(all_startups_df[col])
    
    all_startups_df['funding_total_usd'] = unified_funding
    print(f"\n--- merge_and_clean_startups: all_startups_df after unifying funding into 'funding_total_usd' (sample) ---")
    print(all_startups_df[['startup_id', 'name', 'funding_total_usd'] + funding_cols].head())
    print(f"Non-null funding_total_usd count: {all_startups_df['funding_total_usd'].notna().sum()}")
    print(f"Sum of funding_total_usd (where notna): {all_startups_df['funding_total_usd'].sum()}")

    # Fill NaN in 'funding_total_usd' with 0, as Neo4j might not handle NaN well for numeric types
    all_startups_df['funding_total_usd'] = all_startups_df['funding_total_usd'].fillna(0)
    print(f"\n--- merge_and_clean_startups: all_startups_df after filling NaN in funding_total_usd with 0 (sample) ---")
    print(all_startups_df[['startup_id', 'name', 'funding_total_usd']].head())
    print(f"Sum of funding_total_usd after fillna(0): {all_startups_df['funding_total_usd'].sum()}")

    return all_startups_df

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


# Create a mapping from job title to skills for quick lookup.
# This now correctly handles the 'job_skills' column which contains a list of skills.
def parse_skills_list(skills_str):
    if not isinstance(skills_str, str) or not skills_str.startswith('['):
        return []
    try:
        return ast.literal_eval(skills_str)
    except (ValueError, SyntaxError):
        return []

def extract_skills_from_roles(linkedin_staff_df, kaggle_jobs_df):
    """
    Matches LinkedIn roles to Kaggle job titles to infer skills for each startup.

    Args:
        linkedin_staff_df (pd.DataFrame): DataFrame with columns ['start_up', 'current_position', 'skills'].
        kaggle_jobs_df (pd.DataFrame): DataFrame with columns ['job_title', 'job_skills'].

    Returns:
        pd.DataFrame: A DataFrame with columns ['start_up', 'skill'].
    """
    all_skills = []

    # 1. Extract skills already present in the linkedin_staff_df
    for _, row in linkedin_staff_df.iterrows():
        skills_val = row['skills']
        if pd.notna(skills_val):
            # If it's a string, check if it's not empty or '[]'
            if isinstance(skills_val, str):
                if skills_val.strip() and skills_val.strip() != '[]':
                    try:
                        skills_list = ast.literal_eval(skills_val)
                        for skill_dict in skills_list:
                            all_skills.append({'start_up': row['start_up'], 'skill': skill_dict['name']})
                    except (ValueError, SyntaxError):
                        pass
            # If it's a list or array, check if it has elements
            elif isinstance(skills_val, (list, tuple)):
                if len(skills_val) > 0:
                    for skill_dict in skills_val:
                        if isinstance(skill_dict, dict) and 'name' in skill_dict:
                            all_skills.append({'start_up': row['start_up'], 'skill': skill_dict['name']})

    # 2. Fuzzy match roles to get more skills
    
    # Check for the correct skills column. If it's not there, we can't infer skills.
    if 'job_skills' not in kaggle_jobs_df.columns:
        print("Warning: 'job_skills' column not found in Kaggle data. Skipping role-based skill inference.")
        if not all_skills:
            return pd.DataFrame(columns=['start_up', 'skill'])
        else:
            # Return only the skills found directly in the LinkedIn data
            skills_df = pd.DataFrame(all_skills)
            skills_df.drop_duplicates(inplace=True)
            return skills_df

    # Create a unique list of job titles from Kaggle for the fuzzy matching choices
    unique_kaggle_titles = kaggle_jobs_df['job_title'].dropna().unique()


    kaggle_jobs_df = kaggle_jobs_df.dropna(subset=['job_title', 'job_skills']).copy()
    kaggle_jobs_df['parsed_skills'] = kaggle_jobs_df['job_skills'].apply(parse_skills_list)

    # Group by job title and aggregate the lists of skills into a single list of unique skills
    title_to_skills_map = kaggle_jobs_df.groupby('job_title')['parsed_skills'].agg(
        lambda lists: sorted(list(set(skill for sublist in lists for skill in sublist)))
    ).to_dict()

    # Get unique roles from LinkedIn staff to avoid re-matching the same role
    unique_linkedin_roles = linkedin_staff_df['current_position'].dropna().unique()

    # Create a mapping from linkedin role to the best matching kaggle title
    role_to_kaggle_map = {}
    for role in unique_linkedin_roles:
        # Using process.extractOne to find the best match above a certain threshold
        match = process.extractOne(role, unique_kaggle_titles, scorer=fuzz.WRatio, score_cutoff=85)
        if match:
            # match is a tuple: (matched_title, score, index)
            role_to_kaggle_map[role] = match[0]

    # Use the mapping to get skills for each startup employee
    for _, row in linkedin_staff_df.iterrows():
        if pd.notna(row['current_position']):
            matched_title = role_to_kaggle_map.get(row['current_position'])
            if matched_title:
                inferred_skills = title_to_skills_map.get(matched_title, [])
                for skill in inferred_skills:
                    all_skills.append({'start_up': row['start_up'], 'skill': skill})

    if not all_skills:
        return pd.DataFrame(columns=['start_up', 'skill'])

    # 3. Create the final DataFrame and clean up
    skills_df = pd.DataFrame(all_skills)
    skills_df.drop_duplicates(inplace=True)
    skills_df['skill'] = skills_df['skill'].str.strip()
    
    return skills_df

