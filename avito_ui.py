import streamlit as st
import pandas as pd
import json
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
import io
import zipfile
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata

# =============================================================================
# ✏️  PLUG IN YOUR SEGMENT DICTIONARIES HERE
#     Each entry in SEGMENT_LIBRARIES is:
#       "Display Name": { "segment_key": { "jt": "...", "sen": "...", "loc": "...", "search_type": "..." }, ... }
# =============================================================================

from segments import mrp_dict ,arp_dict

# Add more dicts below — just follow the same pattern:
# from my_other_file import another_dict

SEGMENT_LIBRARIES: dict[str, dict] = {
    "MRP": mrp_dict,
    "ARPIT": arp_dict,
    # "Custom Pharma": another_dict,   ← uncomment & add more as needed
}

# =============================================================================
# Core API logic
# =============================================================================
PERSON_SEARCH_URL = "https://data.api.aviato.co/person/search"
COMPANY_SEARCH_URL = "https://data.api.aviato.co/company/search"
RATE = 60
PER_SECONDS = 60
PAGE_LIMIT = 250
MAX_OFFSET = 10000

FIELD_MAP = {
    "title":       "experienceList.positionList['title']",
    "description": "experienceList.positionList['description']",
    "headline":    "headline",
    "about":       "about",
}
SEARCH_FIELD_OPTIONS = {
    "Job Title (positionList.title)":          "title",
    "Job Description (positionList.description)": "description",
    "Headline":                                "headline",
    "About":                                   "about",
}
SEARCH_FIELD_LABELS = {v: k for k, v in SEARCH_FIELD_OPTIONS.items()}


@dataclass
class SegmentTerms:
    jobs: list
    seniority: list
    countries: list
    search_type: str = "all"
    search_fields: list = None

    def __post_init__(self):
        if self.search_fields is None:
            self.search_fields = ["title", "description", "headline"]


class RateLimiter:
    def __init__(self, rate, per_seconds, hourly_limit=900):
        self.rate = rate
        self.per_seconds = per_seconds
        self.hourly_limit = hourly_limit
        self.tokens = float(rate)
        self.last_check = time.time()
        self.lock = threading.Lock()
        self.request_timestamps = []

    def wait_for_slot(self):
        with self.lock:
            now = time.time()
            window = 3600
            self.request_timestamps = [t for t in self.request_timestamps if now - t < window]

            if len(self.request_timestamps) >= self.hourly_limit:
                oldest = self.request_timestamps[0]
                sleep_for = window - (now - oldest) + 1
                st.toast(f"⏳ Hourly limit reached, sleeping {sleep_for:.0f}s...", icon="⏳")
                time.sleep(sleep_for)
                now = time.time()
                self.request_timestamps = [t for t in self.request_timestamps if now - t < window]

            elapsed = now - self.last_check
            self.tokens = min(self.rate, self.tokens + elapsed * (self.rate / self.per_seconds))
            if self.tokens < 1:
                time.sleep((1 - self.tokens) * (self.per_seconds / self.rate))
                self.tokens = 0
            else:
                self.tokens -= 1
            self.last_check = time.time()
            self.request_timestamps.append(time.time())


class AviatoClient:
    def __init__(self, token, company_hourly_limit=500, person_hourly_limit=900):
        self.company_limiter = RateLimiter(RATE, PER_SECONDS, hourly_limit=company_hourly_limit)
        self.person_limiter  = RateLimiter(RATE, PER_SECONDS, hourly_limit=person_hourly_limit)
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        retry = Retry(total=5, backoff_factor=2, status_forcelist=[500, 502, 503, 504], allowed_methods=["POST"])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def search_company_id(self, linkedin_slug):
        slug = unicodedata.normalize("NFKC", linkedin_slug.strip().lower())
        slug = "".join(ch for ch in slug if unicodedata.category(ch)[0] != "C").strip()
        dsl = {"dsl": {"offset": 0, "limit": 250, "filters": [{"AND": [{"linkedinID": {"operation": "eq", "value": slug}}]}]}}
        try:
            self.company_limiter.wait_for_slot()
            resp = self.session.post(COMPANY_SEARCH_URL, json=dsl, timeout=30)
            if resp.status_code == 204:
                return None
            resp.raise_for_status()
            items = resp.json().get("items", [])
            ids = list(dict.fromkeys([i.get("id") for i in items if i.get("id")]))
            return ids or None
        except Exception:
            return None

    def search_people_page(self, payload):
        self.person_limiter.wait_for_slot()
        resp = self.session.post(PERSON_SEARCH_URL, json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json().get("items", [])

def split_csv_values(value):
    return [p.strip() for p in str(value).split(",") if p.strip()]


def keyword_or_phrase_search(field, values):
    clauses = []
    for raw in values:
        words = raw.split()
        if len(words) == 1:
            clauses.append({field: {"operation": "fts", "value": raw}})
        else:
            clauses.append({field: {"operation": "textcontains", "value": raw}})
    return {"OR": clauses}


def location_textcontains(field, values):
    return {"OR": [{field: {"operation": "textcontains", "value": v}} for v in values]}


def build_person_search_dsl(*, offset, limit, company_id, segment_terms, excluded_linkedin_ids):
    filters = []
    if excluded_linkedin_ids:
        filters.append(
            {"linkedinID": {"operation": "notin", "value": excluded_linkedin_ids}})
    if segment_terms.countries:
        filters.append(
            {"country": {"operation": "in", "value": segment_terms.countries}})

    exp = [
        {"experienceList.companyID": {"operation": "in", "value": company_id}},
        {"experienceList.endDate": {"operation": "eq", "value": None}},
        {"experienceList.positionList['endDate']": {"operation": "eq", "value": None}},
    ]
    text_constraints = []

    if segment_terms.jobs:
        if segment_terms.search_type == "only_jt":
            text_constraints.append({"AND": [keyword_or_phrase_search(FIELD_MAP["title"], segment_terms.jobs), *exp]})
        else:
            or_clauses = [keyword_or_phrase_search(FIELD_MAP[f], segment_terms.jobs) for f in segment_terms.search_fields if f in FIELD_MAP]
            text_constraints.append({"AND": [{"OR": or_clauses}, *exp]})

    if segment_terms.seniority:
        text_constraints.append({"AND": [keyword_or_phrase_search(FIELD_MAP["title"], segment_terms.seniority), *exp]})

    if text_constraints:
        filters.append({"AND": text_constraints})

    return {"dsl": {"offset": offset, "limit": limit, "filters": [{"AND": filters}] if filters else []}}


def clean_nan_inf(value):
    if isinstance(value, dict):
        return {k: clean_nan_inf(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clean_nan_inf(v) for v in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


ZERO_WIDTH_RE = re.compile(r'[\u200B-\u200D\uFEFF]')

def clean_str(value):
    """Remove invisible Unicode chars and trim whitespace."""
    if isinstance(value, str):
        return ZERO_WIDTH_RE.sub('', value).strip()
    return value


def flatten_person(person: dict) -> dict:
    """
    Extract and flatten a person record into CSV-ready fields,
    mirroring the json_dir_to_csv field mapping.
    """
    urls = person.get("URLs", {}) or {}
    linkedin = clean_str(urls.get("linkedin", ""))
    twitter  = clean_str(urls.get("twitter", ""))

    # current experience: first entry where endDate is None
    exp_list = person.get("experienceList", []) or []
    current_exp = next(
        (e for e in exp_list if e.get("endDate") is None),
        exp_list[0] if exp_list else {}
    )

    # current position: first position in current exp where endDate is None
    positions = current_exp.get("positionList", []) or []
    current_pos = next(
        (p for p in positions if p.get("endDate") is None),
        positions[0] if positions else {}
    )

    return {
        "person_name":      clean_str(person.get("fullName", "")),
        "headline":         clean_str(person.get("headline", "")),
        "location":         clean_str(person.get("location", "")),
        "linkedin":         linkedin,
        "linkedin_id":      linkedin.split("/")[-1] if linkedin else clean_str(str(person.get("linkedinID", ""))),
        "twitter":          twitter,
        "current_company":  clean_str(current_exp.get("companyName", "")),
        "current_title":    clean_str(current_pos.get("title", "")),
        "current_start":    clean_str(str(current_pos.get("startDate", "") or "")),
        "_segment":         person.get("_segment", ""),
        "_library":         person.get("_library", ""),
        "_company_slug":    person.get("_company_slug", ""),
    }


# =============================================================================
# Helper: render an editable segment panel
# Returns updated segment config dict.
# =============================================================================
def render_segment_panel(seg_key: str, state: dict, uid: str, badge_html: str) -> dict:
    """Renders edit fields for one segment. Returns the (possibly updated) config."""
    hc1, hc2 = st.columns([6, 1])
    with hc1:
        st.markdown(f'<span style="font-size:1rem;font-weight:700;color:#111827">{seg_key}</span>{badge_html}',
                    unsafe_allow_html=True)
    with hc2:
        if st.button("↺ Reset", key=f"reset_{uid}", help="Restore original values"):
            return None  # caller handles reset

    ec1, ec2 = st.columns([1, 3])
    with ec1:
        new_key = st.text_input("Segment key", value=state.get("key", seg_key), key=f"key_{uid}")
    with ec2:
        mode_opts = ["Only Job Title field", "Multiple fields (select below)"]
        cur_mode = "Only Job Title field" if state.get("search_type") == "only_jt" else "Multiple fields (select below)"
        new_mode = st.radio("Search mode", mode_opts, index=mode_opts.index(cur_mode), key=f"mode_{uid}", horizontal=True)

    new_jt = st.text_area("Job Title Keywords", value=state.get("jt", ""), height=100, key=f"jt_{uid}")

    fc1, fc2 = st.columns(2)
    with fc1:
        new_sen = st.text_area("Seniority Keywords", value=state.get("sen", ""), height=75, key=f"sen_{uid}")
    with fc2:
        new_loc = st.text_area("Locations / Countries", value=state.get("loc", ""), height=75, key=f"loc_{uid}")

    new_fields = ["title"]
    if new_mode == "Multiple fields (select below)":
        cur_fields = state.get("search_fields", ["title", "description", "headline"])
        cur_labels = [SEARCH_FIELD_LABELS[f] for f in cur_fields if f in SEARCH_FIELD_LABELS]
        sel = st.multiselect("Search fields for job keywords", list(SEARCH_FIELD_OPTIONS.keys()),
                             default=cur_labels, key=f"fields_{uid}")
        new_fields = [SEARCH_FIELD_OPTIONS[s] for s in sel] if sel else ["title"]

    return {
        "key": new_key, "jt": new_jt, "sen": new_sen, "loc": new_loc,
        "search_type": "only_jt" if new_mode == "Only Job Title field" else "all",
        "search_fields": new_fields,
    }


# =============================================================================
# Streamlit UI
# =============================================================================
st.set_page_config(page_title="Aviato People Search", layout="wide", page_icon="🔍")

st.markdown("""
<style>
    .block-container { padding-top: 2rem; }
    .badge {
        display: inline-block; font-size: 0.68rem; font-weight: 700;
        padding: 1px 8px; border-radius: 999px; margin-left: 6px; vertical-align: middle;
    }
</style>
""", unsafe_allow_html=True)

BADGE_COLORS = [
    ("badge", "#dbeafe", "#1d4ed8"),
    ("badge", "#d1fae5", "#065f46"),
    ("badge", "#fef9c3", "#92400e"),
    ("badge", "#fce7f3", "#9d174d"),
    ("badge", "#ede9fe", "#4c1d95"),
    ("badge", "#ffedd5", "#9a3412"),
]

def badge(text: str, idx: int) -> str:
    _, bg, fg = BADGE_COLORS[idx % len(BADGE_COLORS)]
    return f'<span class="badge" style="background:{bg};color:{fg}">{text}</span>'


st.title("🔍 Aviato People Search")
st.caption("Search LinkedIn profiles across companies using predefined segment libraries or custom segments.")

# ── Step 1: API Config ─────────────────────────────────────────────────────────
with st.expander("🔑 Step 1: API Configuration", expanded=True):
    api_token = st.text_input("Aviato API Token", type="password", placeholder="Enter your Bearer token...")
    campaign_name = st.text_input("Campaign Name", value="campaign_" + datetime.now().strftime("%Y%m%d"))

# ── Step 2: Upload CSV ─────────────────────────────────────────────────────────
with st.expander("📁 Step 2: Upload Company CSV", expanded=True):
    uploaded_file = st.file_uploader("Upload CSV with company slugs", type=["csv"])
    slug_col = None
    excluded_col = None

    if uploaded_file:
        df_preview = pd.read_csv(uploaded_file)
        uploaded_file.seek(0)
        st.write(f"**{len(df_preview)} rows** — Columns: `{list(df_preview.columns)}`")
        st.dataframe(df_preview.head(5), use_container_width=True)
        cols = list(df_preview.columns)
        c1, c2 = st.columns(2)
        with c1:
            default_slug = next((c for c in cols if "slug" in c.lower()), cols[0])
            slug_col = st.selectbox("Column with **company slugs**", cols, index=cols.index(default_slug))
        with c2:
            none_opt = ["(none)"] + cols
            default_excl = next((c for c in cols if "linkedin_id" in c.lower()), "(none)")
            excl_raw = st.selectbox("Column with **LinkedIn IDs to exclude** (optional)", none_opt,
                                    index=none_opt.index(default_excl) if default_excl in none_opt else 0)
            excluded_col = None if excl_raw == "(none)" else excl_raw

# ── Step 3: Choose which libraries to use ─────────────────────────────────────
with st.expander("📚 Step 3: Choose Segment Libraries", expanded=True):
    lib_names = list(SEGMENT_LIBRARIES.keys())

    if len(lib_names) == 1:
        st.info(f"Only one library available: **{lib_names[0]}**. Add more dicts to `SEGMENT_LIBRARIES` in the code.")
        selected_libs = lib_names
    else:
        st.caption("Select which segment libraries to load. Each library's segments will be shown below for editing.")
        selected_libs = st.multiselect(
            "Active libraries",
            lib_names,
            default=lib_names,
            format_func=lambda name: f"{name}  ({len(SEGMENT_LIBRARIES[name])} segments)",
        )

    # Show a quick summary table of what's in each selected library
    if selected_libs:
        rows = []
        for lib_name in selected_libs:
            for seg_key, seg_cfg in SEGMENT_LIBRARIES[lib_name].items():
                kw_count = len(split_csv_values(seg_cfg.get("jt", "")))
                rows.append({
                    "Library": lib_name,
                    "Segment": seg_key,
                    "# JT Keywords": kw_count,
                    "Seniority": seg_cfg.get("sen", "—")[:60] + ("…" if len(seg_cfg.get("sen", "")) > 60 else ""),
                    "Location": seg_cfg.get("loc", "—"),
                    "Search Type": seg_cfg.get("search_type", "all"),
                })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

# ── Step 4: Edit segments from selected libraries ─────────────────────────────
with st.expander("✏️ Step 4: Edit Segments", expanded=True):

    # Session state init
    if "lib_states" not in st.session_state:
        st.session_state.lib_states = {}
    if "lib_enabled" not in st.session_state:
        st.session_state.lib_enabled = {}

    for lib_idx, lib_name in enumerate(selected_libs):
        source_dict = SEGMENT_LIBRARIES[lib_name]

        # Init state for this library if needed
        if lib_name not in st.session_state.lib_states:
            st.session_state.lib_states[lib_name] = {
                k: dict(v, search_fields=["title", "description", "headline"])
                for k, v in source_dict.items()
            }
        if lib_name not in st.session_state.lib_enabled:
            st.session_state.lib_enabled[lib_name] = {k: True for k in source_dict}

        lib_badge = badge(lib_name, lib_idx)
        st.markdown(f"### {lib_badge}", unsafe_allow_html=True)

        # "Enable all / Disable all" shortcuts
        ea1, ea2, ea3 = st.columns([2, 2, 6])
        with ea1:
            if st.button(f"✅ Enable all", key=f"enable_all_{lib_name}"):
                for k in source_dict:
                    st.session_state.lib_enabled[lib_name][k] = True
                st.rerun()
        with ea2:
            if st.button(f"❌ Disable all", key=f"disable_all_{lib_name}"):
                for k in source_dict:
                    st.session_state.lib_enabled[lib_name][k] = False
                st.rerun()

        for seg_key, original_cfg in source_dict.items():
            state = st.session_state.lib_states[lib_name].get(seg_key, dict(original_cfg))
            uid = f"{lib_name}__{seg_key}"

            hc1, hc2, hc3 = st.columns([4, 1.2, 1.2])
            with hc1:
                seg_badge = badge(f"{lib_name} › {seg_key}", lib_idx)
                st.markdown(f'<span style="font-size:1rem;font-weight:700;color:#111827">{seg_key}</span>{seg_badge}',
                            unsafe_allow_html=True)
            with hc2:
                enabled = st.checkbox("Enable", value=st.session_state.lib_enabled[lib_name].get(seg_key, True),
                                      key=f"enable_{uid}")
                st.session_state.lib_enabled[lib_name][seg_key] = enabled
            with hc3:
                if st.button("↺ Reset", key=f"reset_{uid}", help="Restore original values"):
                    st.session_state.lib_states[lib_name][seg_key] = dict(
                        original_cfg, search_fields=["title", "description", "headline"]
                    )
                    st.rerun()

            if enabled:
                ec1, ec2 = st.columns([1, 3])
                with ec1:
                    new_key = st.text_input("Segment key", value=state.get("key", seg_key), key=f"key_{uid}")
                with ec2:
                    mode_opts = ["Only Job Title field", "Multiple fields (select below)"]
                    cur_mode = "Only Job Title field" if state.get("search_type") == "only_jt" else "Multiple fields (select below)"
                    new_mode = st.radio("Search mode", mode_opts, index=mode_opts.index(cur_mode),
                                        key=f"mode_{uid}", horizontal=True)

                new_jt = st.text_area("Job Title Keywords", value=state.get("jt", ""), height=100, key=f"jt_{uid}")
                fc1, fc2 = st.columns(2)
                with fc1:
                    new_sen = st.text_area("Seniority Keywords", value=state.get("sen", ""), height=75, key=f"sen_{uid}")
                with fc2:
                    new_loc = st.text_area("Locations / Countries", value=state.get("loc", ""), height=75, key=f"loc_{uid}")

                new_fields = ["title"]
                if new_mode == "Multiple fields (select below)":
                    cur_fields = state.get("search_fields", ["title", "description", "headline"])
                    cur_labels = [SEARCH_FIELD_LABELS[f] for f in cur_fields if f in SEARCH_FIELD_LABELS]
                    sel = st.multiselect("Search fields", list(SEARCH_FIELD_OPTIONS.keys()),
                                         default=cur_labels, key=f"fields_{uid}")
                    new_fields = [SEARCH_FIELD_OPTIONS[s] for s in sel] if sel else ["title"]

                st.session_state.lib_states[lib_name][seg_key] = {
                    "key": new_key, "jt": new_jt, "sen": new_sen, "loc": new_loc,
                    "search_type": "only_jt" if new_mode == "Only Job Title field" else "all",
                    "search_fields": new_fields,
                }

            st.divider()

        if lib_idx < len(selected_libs) - 1:
            st.markdown("---")

# ── Step 5: Custom Segments ────────────────────────────────────────────────────
with st.expander("➕ Step 5: Additional Custom Segments (optional)", expanded=False):
    num_custom = st.number_input("How many custom segments?", min_value=0, max_value=20, value=0, step=1)
    custom_segments = []

    for i in range(int(num_custom)):
        custom_badge = badge("CUSTOM", len(SEGMENT_LIBRARIES))
        st.markdown(f'<span style="font-size:1rem;font-weight:700;color:#111827">Custom Segment {i+1}</span>{custom_badge}',
                    unsafe_allow_html=True)
        cc1, cc2 = st.columns([1, 3])
        with cc1:
            ckey = st.text_input("Segment key", value=f"custom_{i+1}", key=f"cust_key_{i}")
        with cc2:
            cmode = st.radio("Search mode", ["Only Job Title field", "Multiple fields (select below)"],
                             key=f"cust_mode_{i}", horizontal=True)
        cjt = st.text_area("Job Title Keywords", key=f"cust_jt_{i}", height=80, placeholder="e.g. CEO, Chief Executive Officer")
        ca, cb = st.columns(2)
        with ca:
            csen = st.text_area("Seniority Keywords (optional)", key=f"cust_sen_{i}", height=70)
        with cb:
            cloc = st.text_area("Locations / Countries (optional)", key=f"cust_loc_{i}", height=70)

        cfields = ["title"]
        if cmode == "Multiple fields (select below)":
            csel = st.multiselect("Search fields", list(SEARCH_FIELD_OPTIONS.keys()),
                                  default=["Job Title (positionList.title)", "Job Description (positionList.description)", "Headline"],
                                  key=f"cust_fields_{i}")
            cfields = [SEARCH_FIELD_OPTIONS[s] for s in csel] if csel else ["title"]

        custom_segments.append({"key": ckey, "jt": cjt, "sen": csen, "loc": cloc,
                                 "search_type": "only_jt" if cmode == "Only Job Title field" else "all",
                                 "search_fields": cfields})
        st.divider()

# ── Step 6: DSL Preview ────────────────────────────────────────────────────────
with st.expander("🔎 Step 6: Preview DSL payload (optional)", expanded=False):
    preview_options = []
    for lib_name in selected_libs:
        for seg_key in SEGMENT_LIBRARIES.get(lib_name, {}):
            if st.session_state.lib_enabled.get(lib_name, {}).get(seg_key):
                preview_options.append(f"{lib_name} › {seg_key}")
    preview_options += [f"custom › {s['key']}" for s in custom_segments]

    if preview_options:
        preview_choice = st.selectbox("Select segment to preview", preview_options)
        if st.button("Generate Sample DSL"):
            seg_data = None
            if " › " in preview_choice:
                parts = preview_choice.split(" › ", 1)
                lib, sk = parts[0], parts[1]
                if lib == "custom":
                    seg_data = next((s for s in custom_segments if s["key"] == sk), None)
                else:
                    seg_data = st.session_state.lib_states.get(lib, {}).get(sk)
            if seg_data:
                st_obj = SegmentTerms(
                    jobs=split_csv_values(seg_data.get("jt", "")),
                    seniority=split_csv_values(seg_data.get("sen", "")),
                    countries=split_csv_values(seg_data.get("loc", "")),
                    search_type=seg_data.get("search_type", "all"),
                    search_fields=seg_data.get("search_fields", ["title"]),
                )
                st.json(build_person_search_dsl(offset=0, limit=250, company_id=["SAMPLE_ID"],
                                                segment_terms=st_obj, excluded_linkedin_ids=[]))
    else:
        st.info("Enable at least one segment to preview.")

# ── Run ────────────────────────────────────────────────────────────────────────
st.markdown("---")

# Build final run list
all_segments_to_run = []
for lib_name in selected_libs:
    for seg_key, original_cfg in SEGMENT_LIBRARIES.get(lib_name, {}).items():
        if st.session_state.lib_enabled.get(lib_name, {}).get(seg_key):
            cfg = st.session_state.lib_states.get(lib_name, {}).get(seg_key, dict(original_cfg))
            all_segments_to_run.append((f"{lib_name}:{seg_key}", cfg))
for s in custom_segments:
    all_segments_to_run.append((f"custom:{s['key']}", s))

total_seg_count = len(all_segments_to_run)

if total_seg_count:
    summary_parts = []
    for lib_name in selected_libs:
        active_in_lib = [k for k in SEGMENT_LIBRARIES.get(lib_name, {})
                         if st.session_state.lib_enabled.get(lib_name, {}).get(k)]
        if active_in_lib:
            summary_parts.append(f"**{lib_name}**: {', '.join(active_in_lib)}")
    if custom_segments:
        summary_parts.append(f"**Custom**: {', '.join(s['key'] for s in custom_segments)}")
    st.info(f"🗂 **{total_seg_count} segment(s) queued**\n\n" + "\n\n".join(summary_parts))
else:
    st.warning("No segments enabled. Enable at least one segment or add a custom segment.")

rc, _ = st.columns([1, 3])
with rc:
    run_button = st.button("🚀 Run Search", type="primary",
                           disabled=not (api_token and uploaded_file and slug_col and total_seg_count > 0))

if run_button:
    uploaded_file.seek(0)
    input_df = pd.read_csv(uploaded_file)
    company_slugs = (input_df[slug_col].dropna().astype(str).str.strip()
                     .loc[lambda s: s != ""].unique().tolist())
    excluded_ids = []
    if excluded_col:
        excluded_ids = (input_df[excluded_col].dropna().astype(str).str.strip()
                        .loc[lambda s: s != ""].unique().tolist())

    client = AviatoClient(api_token)
    all_results = []
    exclude_running = list(excluded_ids)

    progress_bar = st.progress(0)
    status_text = st.empty()

    # ── Live log panel ──────────────────────────────────────────────────────
    st.markdown("**📋 Live Run Log**")
    log_container = st.container()
    log_lines: list[str] = []          # accumulate all log lines
    log_display = log_container.empty() # single element we overwrite each tick

    def ts() -> str:
        return datetime.now().strftime("%H:%M:%S")

    def push_log(line: str) -> None:
        log_lines.append(line)
        # keep the most recent 200 lines so the box doesn't grow forever
        visible = log_lines[-200:]
        log_display.markdown(
            '<div style="'
            'background:#0f172a;color:#e2e8f0;font-family:monospace;font-size:0.78rem;'
            'padding:10px 14px;border-radius:6px;max-height:340px;overflow-y:auto;'
            'white-space:pre-wrap;line-height:1.55;">'
            + "\n".join(visible)
            + "</div>",
            unsafe_allow_html=True,
        )

    push_log(f"[{ts()}]  🚀  Run started — {len(all_segments_to_run)} segment(s) × {len(company_slugs)} company slug(s)")
    # ───────────────────────────────────────────────────────────────────────

    total_steps = len(all_segments_to_run) * len(company_slugs)
    step = 0
    grand_total_found = 0

    for run_id, seg_cfg in all_segments_to_run:
        effective_key = seg_cfg.get("key", run_id.split(":")[-1])
        seg_terms = SegmentTerms(
            jobs=split_csv_values(seg_cfg.get("jt", "")),
            seniority=split_csv_values(seg_cfg.get("sen", "")),
            countries=split_csv_values(seg_cfg.get("loc", "")),
            search_type=seg_cfg.get("search_type", "all"),
            search_fields=seg_cfg.get("search_fields", ["title", "description", "headline"]),
        )
        seg_results = []
        seg_total = 0
        push_log(f"\n[{ts()}]  ▶  Segment [{effective_key}]  |  fields: {seg_terms.search_fields}  |  mode: {seg_terms.search_type}")

        for slug in company_slugs:
            step += 1
            progress_bar.progress(step / max(total_steps, 1))
            status_text.info(f"[{effective_key}] Company: **{slug}** ({step}/{total_steps})")
            push_log(f"[{ts()}]    🔎  {slug} …")

            company_ids = client.search_company_id(slug)
            if not company_ids:
                push_log(f"[{ts()}]    ⚠️  {slug} — company ID not found, skipping")
                continue

            push_log(f"[{ts()}]    ✔  {slug} → company ID(s): {company_ids[:3]}{'…' if len(company_ids) > 3 else ''}")

            offset = 0
            slug_count = 0
            pages = 0
            while offset < MAX_OFFSET:
                payload = build_person_search_dsl(offset=offset, limit=PAGE_LIMIT, company_id=company_ids,
                                                   segment_terms=seg_terms, excluded_linkedin_ids=exclude_running)
                try:
                    items = client.search_people_page(payload)
                except Exception as e:
                    push_log(f"[{ts()}]    ❌  {slug} — API error: {e}")
                    status_text.error(f"Error for {slug}: {e}")
                    break
                if not items:
                    break
                pages += 1
                slug_count += len(items)
                cleaned = [clean_nan_inf(p) for p in items]
                for p in cleaned:
                    p["_segment"] = effective_key
                    p["_library"] = run_id.split(":")[0]
                    p["_company_slug"] = slug
                seg_results.extend(cleaned)
                if len(items)+2 < PAGE_LIMIT:
                    break
                offset += PAGE_LIMIT

            paged_note = f"  ({pages} page{'s' if pages != 1 else ''})" if pages > 1 else ""
            if slug_count > 0:
                push_log(f"[{ts()}]    👤  {slug} → {slug_count} people found{paged_note}")
            else:
                push_log(f"[{ts()}]    —   {slug} → 0 people found")

            seg_total += slug_count

        seg_ids = [p.get("linkedinID") or p.get("linkedin_id") or p.get("id") for p in seg_results if p]
        exclude_running.extend([str(x) for x in seg_ids if x])
        all_results.extend(seg_results)
        grand_total_found += seg_total

        push_log(f"[{ts()}]  ✅  Segment [{effective_key}] done — {seg_total} people total  |  running total: {grand_total_found}")

    progress_bar.progress(1.0)
    status_text.success(f"✅ Done! Found **{grand_total_found}** people across all segments.")
    push_log(f"\n[{ts()}]  🏁  Run complete — {grand_total_found} people found across {len(all_segments_to_run)} segment(s)")

    if all_results:
        result_df = pd.DataFrame([flatten_person(p) for p in all_results])
        raw_count = len(result_df)

        # Deduplicate by linkedin_id (keep first = first segment hit)
        has_id = result_df["linkedin_id"].str.strip().ne("")
        df_with_id    = result_df[has_id].drop_duplicates(subset=["linkedin_id"], keep="first")
        df_without_id = result_df[~has_id]
        result_df = pd.concat([df_with_id, df_without_id], ignore_index=True)

        deduped_count = len(result_df)
        removed = raw_count - deduped_count
        push_log(f"[{ts()}]  🧹  Dedup: {raw_count} raw rows → {deduped_count} unique (removed {removed} duplicate linkedin_id{'s' if removed != 1 else ''})")

        mc1, mc2, mc3 = st.columns(3)
        mc1.metric("Total rows (raw)", f"{raw_count:,}")
        mc2.metric("After dedup", f"{deduped_count:,}")
        mc3.metric("Duplicates removed", f"{removed:,}")

        st.subheader("Preview (first 50 rows)")
        st.dataframe(result_df.head(50), use_container_width=True)

        dl1, dl2 = st.columns(2)
        with dl1:
            csv_buf = io.StringIO()
            result_df.to_csv(csv_buf, index=False)
            st.download_button("⬇️ Download Master CSV (deduped)", data=csv_buf.getvalue().encode("utf-8-sig"),
                               file_name=f"{campaign_name}_results.csv", mime="text/csv")
        with dl2:
            zip_buf = io.BytesIO()
            with zipfile.ZipFile(zip_buf, "w") as zf:
                for sk in result_df["_segment"].unique():
                    zf.writestr(f"{campaign_name}_{sk}.csv",
                                result_df[result_df["_segment"] == sk].to_csv(index=False))
            st.download_button("⬇️ Download Per-Segment ZIP", data=zip_buf.getvalue(),
                               file_name=f"{campaign_name}_segments.zip", mime="application/zip")
    else:
        st.warning("No results found.")