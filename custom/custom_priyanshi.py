import json
import math
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from diskcache import Cache
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from tqdm import tqdm
from urllib3.util.retry import Retry

from jsontocsv import json_dir_to_csv

from segments import mrp_dict

# -----------------------------
# User config
# -----------------------------
CAMPAIGN_NAME = "Armaan_Februaryv2"
MAX_OFFSET = 10000

# note:search_type can have values "Only Jt" or "all"
# Only Jt will apply job title filters only to the position title field , 
# while "all" will apply to title, description, headline, and about fields
# only jt", "only_jt"

# NOTE: Update these paths to point to your actual input CSV files
INPUT_CSVs = [
              r"C:\Users\karta\Desktop\pintel\aviato\input\Armaan - February - Input1.2.csv",
              r"C:\Users\karta\Desktop\pintel\aviato\input\Armaan - February - input_c2.2.csv"
            ]


# SEGMENTS: dict[str, dict[str, str]] = mrp_dict
# keywords :
# WalkMe, DAP, Digital Adoption Platform, SAP, Pendo, SAP Enable Now, Salesforce, Dynamics 365, SAP S/4HANA, SAP Ariba, Workday, SuccessFactors, MS Dynamics 365, Microsoft Dynamics 365, Pendo.io, Apty, Userlane, Oracle Guided Learning, SAP HANA

# Seniority :
# 

# Location :
# 
SEGMENTS: dict[str, dict[str, str]] = mrp_dict
# {
#     "c1":{
#         "search_type":"only_jt",
#         "loc":"Albania, Andorra, Austria, Belarus, Belgium, Bosnia and Herzegovina, Bulgaria, Croatia, Cyprus, Czech Republic, Denmark, Estonia, Finland, France, Germany, Greece, Hungary, Iceland, Ireland, Italy, Latvia, Liechtenstein, Lithuania, Luxembourg, Malta, Moldova, Monaco, Montenegro, Netherlands, North Macedonia, Norway, Poland, Portugal, Romania, Russia, San Marino, Serbia, Slovakia, Slovenia, Spain, Sweden, Switzerland, Turkey, Ukraine, United Kingdom, Vatican City, Kosovo",
#         "sen":"Head, VP, Director, AVP, SVP, Vice President, Senior Vice President, Associate Vice President, Assistant Vice President, Executive Vice President, Senior Director, National Director, Executive Director, Global Director, Head of, Global Head, Manager, senior manager, sr. manager",
#         "jt":" WalkMe, DAP, Digital Adoption Platform, SAP, Pendo, SAP Enable Now, Salesforce, Dynamics 365, SAP S/4HANA, SAP Ariba, Workday, SuccessFactors, MS Dynamics 365, Microsoft Dynamics 365, Pendo.io, Apty, Userlane, Oracle Guided Learning, SAP HANA"
#     }
# }
# -----------------------------
# Runtime config
# -----------------------------
PERSON_SEARCH_URL = "https://data.api.aviato.co/person/search"
COMPANY_SEARCH_URL = "https://data.api.aviato.co/company/search"
API_TOKEN_ENV = "ava"
CACHE_DIR = "./company_cache"
ERROR_LOG = "errors.log"
INFO_LOG = "info.log"
RATE = 60
PER_SECONDS = 60
PAGE_LIMIT = 250
LOG_DIR: Path | None = None

# print(os.getenv("ava"))
# return
import re
import unicodedata


@dataclass
class SegmentTerms:
    jobs: list[str]
    seniority: list[str]
    countries: list[str]
    search_type: str = "all"


class RateLimiter:
    def __init__(self, rate: int, per_seconds: int) -> None:
        self.rate = rate
        self.per_seconds = per_seconds
        self.tokens = float(rate)
        self.last_check = time.time()
        self.lock = threading.Lock()

    def wait_for_slot(self) -> None:
        with self.lock:
            now = time.time()
            elapsed = now - self.last_check
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))

            if self.tokens < 1:
                sleep_for = (1 - self.tokens) * (self.per_seconds / self.rate)
                time.sleep(sleep_for)
                self.tokens = 0
            else:
                self.tokens -= 1

            self.last_check = now


class AviatoClient:
    def __init__(self, token: str) -> None:
        self.rate_limiter = RateLimiter(RATE, PER_SECONDS)
        self.cache = Cache(CACHE_DIR)

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            }
        )

        retry_strategy = Retry(
            total=5,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=50, pool_maxsize=50)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
    
    
    
    def search_company_id(self, linkedin_slug: str) -> list[Any] | None:

        def normalize_linkedin_slug(linkedin_slug: str) -> str:
            if not linkedin_slug:
                return linkedin_slug

            slug = unicodedata.normalize("NFKC", linkedin_slug)

            # Remove ALL zero-width + control characters
            slug = "".join(
                ch for ch in slug
                if unicodedata.category(ch)[0] != "C"
            )

            return slug.strip()
            
        slug = linkedin_slug.strip().lower()
        slug=normalize_linkedin_slug(slug)

        cached = self.cache.get(slug)
        if cached:
            return cached

        dsl = {
            "dsl": {
                "offset": 0,
                "limit": 250,
                "filters": [
                    {
                        "AND": [
                            {
                                "linkedinID": {
                                    "operation": "eq",
                                    "value": slug,
                                }
                            }
                        ]
                    }
                ],
            }
        }

        try:
            self.rate_limiter.wait_for_slot()
            response = self.session.post(COMPANY_SEARCH_URL, json=dsl, timeout=30)

            if response.status_code == 204:
                return None

            response.raise_for_status()
            items = response.json().get("items", [])

            if not items:
                log_error(f"No company id found for slug={linkedin_slug} {items}")
                return None

            # We already filtered by linkedinID in DSL; trust returned items and collect ids.
            company_ids = [item.get("id") for item in items if item.get("id") is not None]

            if not company_ids:
                log_error(f"No usable ids found in company search response for slug={linkedin_slug}")
                return None

            company_ids = list(dict.fromkeys(company_ids))
            self.cache.set(slug, company_ids, expire=86400)
            return company_ids

        except Exception as exc:
            log_error(f"Company lookup failed slug={linkedin_slug} err={exc}")
            return None


    def search_people_page(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        self.rate_limiter.wait_for_slot()
        response = self.session.post(PERSON_SEARCH_URL, json=payload, timeout=30)
        response.raise_for_status()
        return response.json().get("items", [])


def log_error(message: str) -> None:
    log_path = (LOG_DIR / ERROR_LOG) if LOG_DIR else Path(ERROR_LOG)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {message}\n")


def log_info(message: str) -> None:
    log_path = (LOG_DIR / INFO_LOG) if LOG_DIR else Path(INFO_LOG)
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {message}\n")


def split_csv_values(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def to_segment_terms(segment: dict[str, str]) -> SegmentTerms:
    raw_search_type = segment.get("search_type", "All").strip().lower()
    normalized_search_type = "only_jt" if raw_search_type in {"only jt", "only_jt"} else "all"
    return SegmentTerms(
        jobs=split_csv_values(segment.get("jt", "")),
        seniority=split_csv_values(segment.get("sen", "")),
        countries=split_csv_values(segment.get("loc", "")),
        search_type=normalized_search_type,
    )


def fts_terms_clause(field: str, values: list[str]) -> dict[str, Any]:
    clauses: list[dict[str, Any]] = []
    for raw in values:
        words = raw.split()
        if len(words) == 1:
            clauses.append({field: {"operation": "fts", "value": raw}})
        else:
            clauses.append(
                {
                    "AND": [
                        {field: {"operation": "fts", "value": word}}
                        for word in words
                    ]
                }
            )
    return {"OR": clauses}


def append_fts_clause(filters: list[dict[str, Any]], field: str, values: list[str]) -> None:
    if values:
        filters.append(fts_terms_clause(field, values))


def load_company_inputs(csv_path: str) -> tuple[list[str], list[str]]:
    input_path = Path(csv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    input_df = pd.read_csv(input_path)
    required_cols = ["slug", "linkedin_id"]
    missing_cols = [col for col in required_cols if col not in input_df.columns]
    if missing_cols:
        print(f"Missing required columns in {input_path}: {missing_cols}")
        # raise ValueError(f"Missing required columns in {input_path}: {missing_cols}")
    company_slugs,excluded_ids=[],[]
    if "slug" in input_df.columns:
        company_slugs =(
            input_df["slug"]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .unique()
            .tolist()
        )
        
    if "linkedin_id" in input_df.columns:
        excluded_ids = (
            input_df["linkedin_id"]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .unique()
            .tolist()
        )
    return company_slugs, excluded_ids


def textcontains_location_clause(field: str, values: list[str]) -> dict[str, Any]:
    """Handle location with exact matching using 'eq' operation"""
    clauses = []
    
    for location in values:
        clauses.append({
            field: {
                "operation": "textcontains",
                "value": location
            }
        })
    
    return {"OR": clauses}

def location_textcontains(field: str, values: list[str])-> dict[str, Any]:
    clauses: list[dict[str, Any]] = []
    for raw in values:
            clauses.append({field: {"operation": "textcontains", "value": raw}})
       
    return {"OR": clauses}
    
    # pass


def build_person_search_dsl(
    *,
    offset: int,
    limit: int,
    company_id: list[str],
    segment_terms: SegmentTerms,
    excluded_linkedin_ids: list[str],
) -> dict[str, Any]:

    filters: list[dict[str, Any]] = []

    if excluded_linkedin_ids:
        filters.append(
            {
                "linkedinID": {
                    "operation": "notin",
                    "value": excluded_linkedin_ids,
                }
            }
        )

    if segment_terms.countries:
        filters.append(
             {
            "country": {"operation": "in", "value": segment_terms.countries}
        }
                        # location_textcontains("location",segment_terms.countries)

        )

    # 🔹 Active experience constraint
    exp_con = [
        {"experienceList.companyID": {"operation": "in", "value": company_id}},
        {"experienceList.endDate": {"operation": "eq", "value": None}},
        {"experienceList.positionList['endDate']": {"operation": "eq", "value": None}},
    ]

    # 🔹 Only job title search
    if segment_terms.search_type == "only_jt" and segment_terms.jobs:
        filters.append(
            {
                "AND": [
                    fts_terms_clause(
                        "experienceList.positionList['title']", segment_terms.jobs
                    ),
                    *exp_con,
                ]
            }
        )

    # 🔹 Full search
    if segment_terms.search_type == "all" and segment_terms.jobs:
        filters.append(
            {
                "AND": [
                    {
                        "OR": [
                            # fts_terms_clause("about", segment_terms.jobs),
                            # fts_terms_clause(
                            #     "experienceList.positionList['title']",
                            #     segment_terms.jobs,
                            # ),
                            fts_terms_clause(
                                "experienceList.positionList['description']",
                                segment_terms.jobs,
                            ),
                            fts_terms_clause("headline", segment_terms.jobs),
                        ]
                    },

                    *exp_con,
                ]
            }
        )

    # 🔹 Seniority
    if segment_terms.seniority:
        filters.append(
            {
                "AND": [
                    fts_terms_clause(
                        "experienceList.positionList['title']",
                        segment_terms.seniority,
                    ),
                    *exp_con,
                ]
            }
        )

    return {
        "dsl": {
            "offset": offset,
            "limit": limit,
            "filters": [{"AND": filters}],
        }
    }


def clean_nan_inf(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: clean_nan_inf(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_nan_inf(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def fetch_all_people(
    *,
    client: AviatoClient,
    company_id: list[str],
    segment_terms: SegmentTerms,
    excluded_ids: list[str],
    limit: int,
    debug_prefix: Path,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    offset = 0

    while True:
        if offset >= MAX_OFFSET:
            log_error(
                f"Offset ceiling ({MAX_OFFSET}) reached for company_id={company_id}. "
                f"Results may be truncated at {len(results)} people."
            )
            break

        payload = build_person_search_dsl(
            offset=offset,
            limit=limit,
            company_id=company_id,
            segment_terms=segment_terms,
            excluded_linkedin_ids=excluded_ids,
        )

        with open(f"{debug_prefix}_offset_{offset}.json", "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        try:
            page_items = client.search_people_page(payload)
        except Exception as exc:
            log_error(f"People fetch failed company_id={company_id} offset={offset} err={exc}")
            break

        if not page_items:
            break

        results.extend(clean_nan_inf(page_items))

        if len(page_items) < limit:
            break

        offset += limit

    log_info(f"Fetched {len(results)} people for company_id={company_id}")
    return results


def unique_preserve_order(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for item in values:
        if item not in seen:
            seen.add(item)
            output.append(item)
    return output


# ---------------------------------------------------------------------------
# run2() helpers – row-based CSV parsing
# ---------------------------------------------------------------------------

# Column name aliases (case-insensitive). Maps canonical key -> list of accepted
# column name variants.
_COL_ALIASES: dict[str, list[str]] = {
    "slug":        ["slug", "company_slug", "company slug", "linkedin_slug",
                    "linkedin url", "linkedin_url", "linkedin", "company url",
                    "company_url", "url"],
    "region":      ["region", "loc", "location", "country", "countries"],
    "jt":          ["jt", "job_title", "job title", "jobtitle", "keywords", "keyword"],
    "sen":         ["sen", "seniority", "seniority_level", "level"],
    "search_type": ["search_type", "search type", "searchtype", "type"],
}


def find_first_column(df: pd.DataFrame, canonical_key: str) -> str | None:
    """Return the first DataFrame column that matches any alias for *canonical_key*.

    The match is case-insensitive and strips surrounding whitespace.
    Returns None if no matching column is found.
    """
    aliases = [a.lower().strip() for a in _COL_ALIASES.get(canonical_key, [])]
    for col in df.columns:
        if col.lower().strip() in aliases:
            return col
    return None


def load_row_search_inputs(csv_path: str) -> list[dict[str, Any]]:
    """Load the input CSV and return one dict per data row.

    Each dict has the structure::

        {
            "slug": str,
            "region": str,       # raw comma-separated string (may be empty)
            "jt": str,           # raw comma-separated string (may be empty)
            "sen": str,          # raw comma-separated string (may be empty)
            "search_type": str,  # "all" or "only_jt"
            "row_index": int,    # 0-based row position in the CSV
        }

    Rows with a blank/missing slug are skipped with a warning.
    """
    input_path = Path(csv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    df = pd.read_csv(input_path)

    # Resolve column names
    slug_col        = find_first_column(df, "slug")
    region_col      = find_first_column(df, "region")
    jt_col          = find_first_column(df, "jt")
    sen_col         = find_first_column(df, "sen")
    search_type_col = find_first_column(df, "search_type")

    print(f"[run2] Column mapping: slug={slug_col!r}  region={region_col!r}  "
          f"jt={jt_col!r}  sen={sen_col!r}  search_type={search_type_col!r}")

    if slug_col is None:
        raise ValueError(
            f"Input CSV '{csv_path}' has no recognisable slug column. "
            f"Expected one of: {_COL_ALIASES['slug']}. "
            f"Actual columns found: {list(df.columns)}"
        )

    rows: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        slug = str(row[slug_col]).strip() if slug_col else ""
        if not slug or slug.lower() == "nan":
            print(f"  [run2] Skipping row {idx}: blank slug")
            continue

        def _get(col: str | None) -> str:
            if col is None:
                return ""
            val = row.get(col, "")
            if pd.isna(val):
                return ""
            return str(val).strip()

        raw_search_type = _get(search_type_col).lower()
        normalized_search_type = (
            "only_jt" if raw_search_type in {"only jt", "only_jt"} else "all"
        )

        rows.append(
            {
                "slug":        slug,
                "region":      _get(region_col),
                "jt":          _get(jt_col),
                "sen":         _get(sen_col),
                "search_type": normalized_search_type,
                "row_index":   int(idx),  # type: ignore[arg-type]
            }
        )

    return rows


# ---------------------------------------------------------------------------
# run2() – one query per CSV row with row-specific filters
# ---------------------------------------------------------------------------

def run2() -> None:
    """Process each CSV row as an independent query.

    Row schema (case-insensitive column names, aliases accepted):
        slug        – company LinkedIn slug  (required)
        Region      – comma-separated country/region list
        JT          – comma-separated job-title keywords
        sen         – comma-separated seniority keywords
        search_type – "only_jt" or "all" (default: "all")

    Outputs per row:
        <campaign>_<date>/<row_index>_<slug>/debug/  – raw DSL payloads
        <campaign>_<date>/<row_index>_<slug>/json/   – people JSON
        <campaign>_<date>/<row_index>_<slug>/<campaign>_row<row_index>.csv

    A master CSV is written to:
        <campaign>_<date>/<campaign>_<date>_master.csv
    """
    global LOG_DIR
    load_dotenv()
    token = os.getenv(API_TOKEN_ENV)
    if not token:
        raise RuntimeError(f"Missing API token in env var '{API_TOKEN_ENV}'")

    client = AviatoClient(token)
    i=0
    # Load all rows from the input CSV
    for INPUT_CSV  in INPUT_CSVs:
        
        campaign = f"{CAMPAIGN_NAME}_{i}"        
        i+=1
        row_inputs = load_row_search_inputs(INPUT_CSV)
        if not row_inputs:
            print("No valid rows found in input CSV. Exiting run2().")
            return

        print(f"[run2] Loaded {len(row_inputs)} rows from {INPUT_CSV}")

        run_date = datetime.now().strftime("%Y%m%d")
        # CAMPAIGN_NAME=CAMPAIGN_NAME+f"_{i+1}"

        root_dir = Path(f"{campaign}_{run_date}")
        root_dir.mkdir(parents=True, exist_ok=True)
        LOG_DIR = root_dir
        (LOG_DIR / ERROR_LOG).touch(exist_ok=True)
        (LOG_DIR / INFO_LOG).touch(exist_ok=True)
        print(f"Info log : {LOG_DIR / INFO_LOG}")
        print(f"Error log: {LOG_DIR / ERROR_LOG}")
        log_info(f"run2 started campaign={campaign} date={run_date} rows={len(row_inputs)} {",".join(pd.read_csv(INPUT_CSV)["slug"])}")

        all_frames: list[pd.DataFrame] = []
        # exclude_ids: list[str] = unique_preserve_order(
        #     pd.read_csv(r"C:\Users\karta\Downloads\Untitled spreadsheet Extra.csv")["id"]
        #     .dropna()
        #     .astype(str)
        #     .str.strip()
        #     .loc[lambda s: s != ""]
        #     .tolist()
        # )
        exclude_ids=[]

        for row in tqdm(row_inputs, desc="Processing rows"):
            slug= row["slug"]
            log_info(slug)
            row_idx     = row["row_index"]
            search_type = "all"
            # search_type = "only_jt"

            # Build SegmentTerms from this row's columns
            segment_terms = SegmentTerms(
                jobs        = split_csv_values(row["jt"]),
                seniority   = split_csv_values(row["sen"]),
                countries   = split_csv_values(row["region"]),
                search_type = search_type,
            )

            # Per-row output directory: <root>/<row_index>_<slug>/
            safe_slug  = re.sub(r"[^\w\-]", "_", slug)
            row_label  = f"row{row_idx:04d}_{safe_slug}"
            row_dir    = root_dir / row_label
            debug_dir  = row_dir / "debug"
            json_dir   = row_dir / "json"
            debug_dir.mkdir(parents=True, exist_ok=True)
            json_dir.mkdir(parents=True, exist_ok=True)

            print(f"\n[run2] Row {row_idx}: slug={slug!r}  jt={row['jt']!r}  "
                f"region={row['region']!r}  sen={row['sen']!r}  "
                f"search_type={search_type!r}")

            out_file = json_dir / f"{safe_slug}_{row_label}.json"
            if out_file.exists():
                log_info(f"Skipping existing output file: {out_file}")
            else:
                company_id = client.search_company_id(slug)
                if company_id is None:
                    log_error(f"Skipping row {row_idx} slug={slug}; company id not found")
                    continue

                people = fetch_all_people(
                    client        = client,
                    company_id    = company_id,
                    segment_terms = segment_terms,
                    excluded_ids  = exclude_ids,
                    limit         = PAGE_LIMIT,
                    debug_prefix  = debug_dir / safe_slug,
                )

                with open(out_file, "w", encoding="utf-8") as f:
                    json.dump(people, f, indent=2)
                log_info(f"Saved people json: {out_file}")

            # Convert this row's JSON directory to CSV
            try:
                output_csv = row_dir / f"{campaign}_{row_label}.csv"
                json_dir_to_csv(
                    input_dir     = str(json_dir),
                    output_csv    = str(output_csv),
                    endswith_text = f"_{row_label}.json",
                )

                row_df = pd.read_csv(output_csv)
                row_df["row_index"] = row_idx
                row_df["slug"]      = slug
                all_frames.append(row_df)

                # Accumulate exclusions so later rows don't return duplicate people
                if "linkedin_id" in row_df.columns:
                    new_ids = row_df["linkedin_id"].dropna().astype(str).unique().tolist()
                    exclude_ids.extend(new_ids)
                    exclude_ids = unique_preserve_order(exclude_ids)

                print(f"  Row {row_idx} complete. CSV: {output_csv}  "
                    f"(total excluded IDs: {len(exclude_ids)})")
            except Exception as exc:
                log_error(f"CSV conversion failed row={row_idx} slug={slug} err={exc}")

        # Master CSV
        if all_frames:
            master_df  = pd.concat(all_frames, ignore_index=True)
            master_csv = root_dir / f"{campaign}_{run_date}_master.csv"
            master_df.to_csv(master_csv, index=False)
            print(f"\n[run2] Master sheet saved to: {master_csv}")
        else:
            print("\n[run2] No row data found; master sheet was not created.")


# ---------------------------------------------------------------------------
# Original run() – preserved intact
# ---------------------------------------------------------------------------

# def run() -> None:
#     global LOG_DIR
#     load_dotenv()
#     token = os.getenv(API_TOKEN_ENV)
#     print(token)
#     if not token:
#         raise RuntimeError(f"Missing API token in env var '{API_TOKEN_ENV}'")
#     client = AviatoClient(token)
#     company_slugs, excluded_linkedin_ids = load_company_inputs(INPUT_CSV)
#     excluded_linkedin_ids=[]
#     # log_info(company_slugs)
#     # company_slugs=["amazon"]
#     exclude_ids: list[str] = excluded_linkedin_ids.copy()
#     run_date = datetime.now().strftime("%Y%m%d")
#     root_dir = Path(f"{CAMPAIGN_NAME}_{run_date}")
#     root_dir.mkdir(parents=True, exist_ok=True)
#     LOG_DIR = root_dir
#     (LOG_DIR / ERROR_LOG).touch(exist_ok=True)
#     (LOG_DIR / INFO_LOG).touch(exist_ok=True)
#     print(f"Info log: {LOG_DIR / INFO_LOG}")
#     print(f"Error log: {LOG_DIR / ERROR_LOG}")
#     log_info(f"Run started campaign={CAMPAIGN_NAME} date={run_date}")

#     companies = unique_preserve_order(company_slugs)
#     segment_frames: list[pd.DataFrame] = []

#     for segment_key, segment_config in tqdm(SEGMENTS.items(), desc="Segments"):
#         segment_terms = to_segment_terms(segment_config)

#         segment_dir = root_dir / segment_key
#         debug_dir = segment_dir / "debug"
#         json_dir = segment_dir / "json"
#         debug_dir.mkdir(parents=True, exist_ok=True)
#         json_dir.mkdir(parents=True, exist_ok=True)

#         print(f"\\nProcessing segment: {segment_key}")
#         print(f"Output directory: {segment_dir}")

#         for company_slug in tqdm(companies, desc=f"Processing {segment_key}", leave=False):
#             company_slug=company_slug.strip()
#             out_file = json_dir / f"{company_slug}_{segment_key}.json"
#             if out_file.exists():
#                 log_info(f"Skipping existing output file: {out_file}")
#                 continue

#             company_id = client.search_company_id(company_slug)
#             if company_id is None:
#                 log_error(f"Skipping slug={company_slug}; company id not found")
#                 continue

#             people = fetch_all_people(
#                 client=client,
#                 company_id=company_id,
#                 segment_terms=segment_terms,
#                 excluded_ids=exclude_ids,
#                 limit=PAGE_LIMIT,
#                 debug_prefix=debug_dir / company_slug,
#             )

#             with open(out_file, "w", encoding="utf-8") as f:
#                 json.dump(people, f, indent=2)
#             log_info(f"Saved people json: {out_file}")

#         try:
#             output_csv = segment_dir / f"{CAMPAIGN_NAME}_{segment_key}.csv"
#             json_dir_to_csv(
#                 input_dir=str(json_dir),
#                 output_csv=str(output_csv),
#                 endswith_text=f"_{segment_key}.json",
#             )

#             segment_df = pd.read_csv(output_csv)
#             segment_df["key"] = segment_key
#             segment_frames.append(segment_df)

#             exclude_ids.extend(segment_df["linkedin_id"].dropna().unique().tolist())
#             exclude_ids = unique_preserve_order([str(x) for x in exclude_ids if str(x).strip()])

#             print(f"Segment {segment_key} complete. CSV saved to: {output_csv}")
#             print(f"Total excluded IDs: {len(exclude_ids)}")
#         except Exception as exc:
#             log_error(f"CSV conversion failed key={segment_key} err={exc}")

#     if segment_frames:
#         master_df = pd.concat(segment_frames, ignore_index=True)
#         master_csv = root_dir / f"{CAMPAIGN_NAME}_{run_date}_master.csv"
#         master_df.to_csv(master_csv, index=False)
#         print(f"Master sheet saved to: {master_csv}")
#     else:
#         print("No segment data found; master sheet was not created.")


if __name__ == "__main__":
    run2()   # ← change to run2() to use row-based mode