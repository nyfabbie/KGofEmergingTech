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

def clean_wikidata(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = (
        df_raw
        .dropna(subset=["qid"])              # keep only matched ones
        .assign(name=lambda d: d["label"].str.strip(),
                tech_key=lambda d: d["label"].map(_normalise))
        [["qid", "name", "description", "tech_key"]]
        .drop_duplicates("qid")
    )
    return df


def clean_arxiv(raw_list: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    # """raw_list is the `papers` returned by fetch_arxiv()"""
    # rows = []
    # for d in raw_list:
    #     if not d["response"]:
    #         continue
    #     csv_rows = pd.read_csv(
    #         pd.compat.StringIO(d["response"]),  # already parsed by parse_et?
    #     )
    # If used parse_et to append to CSV, simply load that CSV here
    csv_rows = pd.read_csv("data/arxiv_papers_res.csv")

    csv_rows["tech_key"] = csv_rows["technology"].map(_normalise)
    csv_rows["paper_id"] = csv_rows["id"].map(_paper_id)
    csv_rows["published"] = pd.to_datetime(csv_rows["published"])
    csv_rows["authors"] = csv_rows["authors"].apply(
        lambda s: ast.literal_eval(s) if isinstance(s, str) else s
    )

    papers_df = (
        csv_rows[["paper_id", "id", "title", "summary",
                  "published"]]
        .drop_duplicates("paper_id")
    )
    edges_df = csv_rows[["paper_id", "tech_key"]]
    return papers_df, edges_df


def clean_and_merge(papers_raw, techs_raw):
    tech_df  = clean_wikidata(pd.DataFrame(techs_raw))
    paper_df, edge_df = clean_arxiv(papers_raw)

    # keep only edges whose tech_key exists in tech_df
    edge_df = edge_df.merge(tech_df[["tech_key", "qid"]], on="tech_key")

    return tech_df, paper_df, edge_df

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

def match_startups_to_techs(startups_df, tech_names, threshold=80, save_csv_path=None):
    """
    Fuzzy matches startups to technologies using rapidfuzz.
    Returns a DataFrame with columns: startup_name, technology, score.
    If save_csv_path is provided, saves the matches to that CSV.
    Keeps only the highest score for each (startup_name, technology) pair.
    """
    matches = []
    # Build a mapping from synonym to canonical tech name
    synonym_to_canonical = {}
    for tech in tech_names:
        for synonym in TECH_SYNONYMS.get(tech.lower(), [tech]):
            synonym_to_canonical[synonym.lower()] = tech

    for idx, row in startups_df.iterrows():
        text = " ".join([
            str(row.get('long_description', '')),
            str(row.get('industry', '')),
            str(row.get('short_description', '')),
            str(row.get('tags', '')),
            str(row.get('name', ''))
        ]).lower()
        for synonym, canonical in synonym_to_canonical.items():
            if len(synonym) <= 3:  # e.g., "AI", "AR"
                # Only match as a whole word
                if re.search(rf"\b{re.escape(synonym)}\b", text):
                    score = 100
                else:
                    score = 0
            else:
                score = fuzz.token_set_ratio(synonym, text)
            if score >= threshold:
                matches.append({
                    "startup_name": row.get("name"),
                    "technology": canonical,
                    "score": score
                })
    matches_df = pd.DataFrame(matches)
    # Keep only the row with the highest score for each (startup_name, technology) pair
    matches_df = matches_df.sort_values("score", ascending=False).drop_duplicates(subset=["startup_name", "technology"], keep="first")
    if save_csv_path:
        matches_df.to_csv(save_csv_path, index=False)
    return matches_df
