# Aviato Search Utilities

Utilities for running Aviato people/company searches, managing segment dictionaries, and exporting search results to CSV.

## Files in this repo

- `avaitov4.py`: Main script for running company + people search flows and writing outputs.
- `segments.py`: Large segment dictionary (`mrp_dict`) used by search workflows.
- `jsontocsv.py`: Converts JSON result files into a flattened CSV.
- `avito_ui.py`: Streamlit UI for running searches interactively.
- `docs.md`: Aviato DSL reference notes.

## Requirements

- Python 3.10+
- Aviato API token

Install dependencies:

```bash
pip install pandas requests diskcache python-dotenv tqdm streamlit
```

## Environment setup

Create a `.env` file in the project root:

```env
ava=YOUR_AVIATO_API_TOKEN
```

`ava` is read by the scripts as the bearer token.

## Run (script mode)

```bash
python avaitov4.py
```

Before running:

- Update `INPUT_CSV` path in `avaitov4.py` if needed.
- Confirm `SEGMENTS` / `mrp_dict` entries match your campaign.

## Run (UI mode)

```bash
streamlit run avito_ui.py
```

This launches a local UI to select segments, run searches, and export results.

## JSON to CSV conversion

`jsontocsv.py` exposes `json_dir_to_csv(input_dir, output_csv, endswith_text="s4.json")` for flattening result JSON files into a CSV with:

- `person_name`
- `location`
- `linkedin`
- `twitter`
- `company_name`
- `linkedin_id`

## Notes

- Generated/cache folders like `output/` and `company_cache/` are ignored via `.gitignore`.
- Keep API tokens and private input data out of git.
