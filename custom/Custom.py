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

import re
import unicodedata

# -----------------------------
# User config
# -----------------------------
CAMPAIGN_NAME = "aashi_6v2+yrs"
MAX_OFFSET = 10000

# note:search_type can have values "only_jt" or "all"
# only_jt will apply job title filters only to the position title field,
# while "all" will apply to title, description, headline, and about fields
INPUT_CSV = r"C:\Users\karta\Desktop\pintel\aviato\input\Mansimar_Ayush Prospecting - Sheet1.csv"

SEGMENTS: dict[str, dict[str, str]] = mrp_dict

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

        def normalize_linkedin_slug(slug: str) -> str:
            if not slug:
                return slug
            slug = unicodedata.normalize("NFKC", slug)
            slug = "".join(ch for ch in slug if unicodedata.category(ch)[0] != "C")
            return slug.strip()

        slug = normalize_linkedin_slug(linkedin_slug.strip().lower())

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

    company_slugs, excluded_ids = [], []

    if "slug" in input_df.columns:
        company_slugs = (
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


def find_first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    normalized = {str(col).strip().lower(): str(col) for col in df.columns}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def slug_to_key(slug: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", slug.strip().lower())
    return cleaned.strip("-") or "unknown-slug"


def load_row_search_inputs(csv_path: str) -> tuple[list[dict[str, Any]], list[str]]:
    input_path = Path(csv_path)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    input_df = pd.read_csv(input_path)

    slug_col = find_first_column(input_df, ["slug"])
    region_col = find_first_column(input_df, ["region", "loc", "country", "countries"])
    jt_col = find_first_column(input_df, ["keywords"])
    # jt_col=input_df["Keywords"]
    sen_col = find_first_column(input_df, ["sen", "seniority"])
    search_type_col = find_first_column(input_df, ["search_type", "searchtype", "type"])
    excluded_col = find_first_column(input_df, ["linkedin_id", "linkedinid"])

    if not slug_col:
        raise ValueError("Input CSV must contain a 'slug' column for run2().")

    rows: list[dict[str, Any]] = []
    for idx, row in input_df.iterrows():
        slug_raw = row.get(slug_col)
        slug = "" if pd.isna(slug_raw) else str(slug_raw).strip()
        if not slug:
            continue

        region_raw = row.get(region_col) if region_col else ""
        jt_raw = row.get(jt_col) if jt_col else ""
        sen_raw = row.get(sen_col) if sen_col else ""
        search_type_raw = row.get(search_type_col) if search_type_col else "all"
        raw_search_type = "" if pd.isna(search_type_raw) else str(search_type_raw).strip().lower()
        normalized_search_type = "only_jt" if raw_search_type in {"only jt", "only_jt"} else "all"

        segment_terms = SegmentTerms(
            jobs=split_csv_values("" if pd.isna(jt_raw) else str(jt_raw)),
            seniority=split_csv_values("" if pd.isna(sen_raw) else str(sen_raw)),
            countries=split_csv_values("" if pd.isna(region_raw) else str(region_raw)),
            search_type=normalized_search_type,
        )

        rows.append(
            {
                "row_number": idx + 1,
                "slug": slug,
                "segment_terms": segment_terms,
            }
        )

    excluded_ids: list[str] = []
    if excluded_col:
        excluded_ids = (
            input_df[excluded_col]
            .dropna()
            .astype(str)
            .str.strip()
            .loc[lambda s: s != ""]
            .unique()
            .tolist()
        )

    return rows, excluded_ids


def build_person_search_dsl_custom(
    *,
    offset: int,
    limit: int,
    company_id: list[str],
    segment_terms: SegmentTerms,
    excluded_linkedin_ids: list[str],
) -> dict[str, Any]:

    filters: list[dict[str, Any]] = []

    # ✅ Exclude LinkedIn IDs
    if excluded_linkedin_ids:
        filters.append(
            {
                "linkedinID": {
                    "operation": "notin",
                    "value": excluded_linkedin_ids,
                }
            }
        )

    # ✅ Country filter
    if segment_terms.countries:
        filters.append(
            {
                "country": {
                    "operation": "in",
                    "value": segment_terms.countries,
                }
            }
        )

    # ✅ Start date filter (exactly like your JSON)
    filters.append(
        {
            "experienceList.startDate": {
                "operation": "lte",
                "value": "2019-02-01T00:00:00.000Z",
            }
        }
    )

    # ✅ Inner AND block (matches your JSON structure)
    inner_and: list[dict[str, Any]] = [
        {
            "experienceList.companyID": {
                "operation": "in",
                "value": company_id,
            }
        },
        {
            "experienceList.endDate": {
                "operation": "eq",
                "value": None,
            }
        },
        {
            "experienceList.positionList['endDate']": {
                "operation": "eq",
                "value": None,
            }
        },
        {
            "experienceList.positionList['department']": {
                "operation": "eq",
                "value": "ENGINEERING",
            }
        },
    ]

    # ✅ OR block using YOUR fts_terms_clause
    if segment_terms.jobs:
        inner_and.append(
            {
                "OR": [
                    fts_terms_clause("experienceList.positionList['title']", segment_terms.jobs),
                    fts_terms_clause("experienceList.positionList['description']", segment_terms.jobs),
                    # fts_terms_clause("headline", segment_terms.jobs),
                    # fts_terms_clause("about", segment_terms.jobs),
                ]
            }
        )

    filters.append({"AND": inner_and})

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

        payload = build_person_search_dsl_custom(
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


def run() -> None:
    global LOG_DIR
    load_dotenv()
    token = os.getenv(API_TOKEN_ENV)
    if not token:
        raise RuntimeError(f"Missing API token in env var '{API_TOKEN_ENV}'")

    client = AviatoClient(token)
    company_slugs, excluded_linkedin_ids = load_company_inputs(INPUT_CSV)

    exclude_ids: list[str] = excluded_linkedin_ids.copy()
    run_date = datetime.now().strftime("%Y%m%d")
    root_dir = Path(f"{CAMPAIGN_NAME}_{run_date}")
    root_dir.mkdir(parents=True, exist_ok=True)
    LOG_DIR = root_dir
    (LOG_DIR / ERROR_LOG).touch(exist_ok=True)
    (LOG_DIR / INFO_LOG).touch(exist_ok=True)
    print(f"Info log: {LOG_DIR / INFO_LOG}")
    print(f"Error log: {LOG_DIR / ERROR_LOG}")
    log_info(f"Run started campaign={CAMPAIGN_NAME} date={run_date}")

    companies = unique_preserve_order(company_slugs)
    segment_frames: list[pd.DataFrame] = []

    for segment_key, segment_config in tqdm(SEGMENTS.items(), desc="Segments"):
        segment_terms = to_segment_terms(segment_config)

        segment_dir = root_dir / segment_key
        debug_dir = segment_dir / "debug"
        json_dir = segment_dir / "json"
        debug_dir.mkdir(parents=True, exist_ok=True)
        json_dir.mkdir(parents=True, exist_ok=True)

        print(f"\nProcessing segment: {segment_key}")
        print(f"Output directory: {segment_dir}")

        for company_slug in tqdm(companies, desc=f"Processing {segment_key}", leave=False):
            company_slug = company_slug.strip()
            out_file = json_dir / f"{company_slug}_{segment_key}.json"
            if out_file.exists():
                log_info(f"Skipping existing output file: {out_file}")
                continue

            company_id = client.search_company_id(company_slug)
            if company_id is None:
                log_error(f"Skipping slug={company_slug}; company id not found")
                continue

            people = fetch_all_people(
                client=client,
                company_id=company_id,
                segment_terms=segment_terms,
                excluded_ids=exclude_ids,
                limit=PAGE_LIMIT,
                debug_prefix=debug_dir / company_slug,
            )

            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(people, f, indent=2)
            log_info(f"Saved people json: {out_file}")

        try:
            output_csv = segment_dir / f"{CAMPAIGN_NAME}_{segment_key}.csv"
            json_dir_to_csv(
                input_dir=str(json_dir),
                output_csv=str(output_csv),
                endswith_text=f"_{segment_key}.json",
            )

            segment_df = pd.read_csv(output_csv)
            segment_df["key"] = segment_key
            segment_frames.append(segment_df)

            exclude_ids.extend(segment_df["linkedin_id"].dropna().unique().tolist())
            exclude_ids = unique_preserve_order([str(x) for x in exclude_ids if str(x).strip()])

            print(f"Segment {segment_key} complete. CSV saved to: {output_csv}")
            print(f"Total excluded IDs: {len(exclude_ids)}")
        except Exception as exc:
            log_error(f"CSV conversion failed key={segment_key} err={exc}")

    if segment_frames:
        master_df = pd.concat(segment_frames, ignore_index=True)
        master_csv = root_dir / f"{CAMPAIGN_NAME}_{run_date}_master.csv"
        master_df.to_csv(master_csv, index=False)
        print(f"Master sheet saved to: {master_csv}")
    else:
        print("No segment data found; master sheet was not created.")



if __name__ == "__main__":
    run()
