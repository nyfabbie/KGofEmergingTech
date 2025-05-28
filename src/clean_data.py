"""
Basic cleaners that turn raw fetch results into
✓ one DataFrame of Technology nodes
✓ one DataFrame of Paper nodes
✓ one DataFrame of Paper-MENTIONS-Tech edges
"""

import ast
import hashlib
import pandas as pd

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
