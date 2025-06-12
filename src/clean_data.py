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

# ---------- helpers -------------------------------------------------

def _normalise(text: str) -> str:
    return text.lower().strip()

def _paper_id(arxiv_url: str) -> str:
    """E.g 2406.04641v1  →  2406.04641v1   (unique + short)"""
    return arxiv_url.rsplit("/", 1)[-1]

# ---------- public API ---------------------------------------------



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

# These synonyms and related terms will help fuzzy matching catch more real-world variations and abbreviations for each emerging technology.
TECH_SYNONYMS = {
    "artificial intelligence": [
        "artificial intelligence", "AI", ".ai", "ai", "machine intelligence", "machine learning", "deep learning", "neural network"
    ],
    "3D printing": [
        "3D printing", "additive manufacturing", "rapid prototyping", "3d printer", "digital fabrication"
    ],
    "augmented reality": [
        "augmented reality", "AR", "mixed reality", "spatial computing"
    ],
    "blockchain": [
        "blockchain", "distributed ledger", "DLT", "crypto", "cryptocurrency", "smart ledger"
    ],
    "cancer vaccine": [
        "cancer vaccine", "oncology vaccine", "therapeutic vaccine", "immunotherapy"
    ],
    "cultured meat": [
        "cultured meat", "lab-grown meat", "cell-based meat", "clean meat", "in vitro meat"
    ],
    "gene therapy": [
        "gene therapy", "genetic therapy", "gene editing", "CRISPR", "genome editing"
    ],
    "neurotechnology": [
        "neurotechnology", "brain-computer interface", "BCI", "neural interface", "neurotech"
    ],
    "reusable launch vehicle": [
        "reusable launch vehicle", "RLV", "reusable rocket", "reusable spacecraft"
    ],
    "robotics": [
        "robotics", "robot", "automation", "autonomous system", "robotic process automation", "RPA"
    ],
    "smart contracts": [
        "smart contracts", "self-executing contract", "blockchain contract", "automated contract"
    ],
    "stem-cell therapy": [
        "stem-cell therapy", "stem cell treatment", "regenerative medicine", "cell therapy"
    ],
}

def match_startups_to_techs(startups_df, techs_df, threshold=80):
    """
    Fuzzy matches startups to technologies using rapidfuzz.
    Returns a DataFrame with columns: startup_name, technology, qid, score.
    If save_csv_path is provided, saves the matches to that CSV.
    Keeps only the highest score for each (startup_name, technology) pair.
    """
    matches = []
    # Build a mapping from synonym to canonical tech name and QID
    synonym_to_canonical_qid = {}
    for _, tech in techs_df.iterrows():
        tech_name = tech['name']
        qid = tech.get('qid', None)
        for synonym in TECH_SYNONYMS.get(tech_name.lower(), [tech_name]):
            synonym_to_canonical_qid[synonym.lower()] = (tech_name, qid)

    for idx, row in startups_df.iterrows():
        text = " ".join([
            str(row.get('long_description', '')),
            str(row.get('industry', '')),
            str(row.get('short_description', '')),
            str(row.get('tags', '')),
            str(row.get('name', ''))
        ]).lower()
        for synonym, (canonical, qid) in synonym_to_canonical_qid.items():
            if len(synonym) <= 3:  # e.g., "AI", "AR"
                # Only match as a whole word
                if re.search(rf"\b{re.escape(synonym)}\b", text):
                    score = 100
                else:
                    score = 0
            else:
                score = fuzz.token_set_ratio(synonym, text)
            if score >= threshold and qid is not None:
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

