import json
import csv
import os
import re

# Regex for zero-width & BOM characters
ZERO_WIDTH_RE = re.compile(r'[\u200B-\u200D\uFEFF]')

def clean(value):
    """Remove invisible Unicode chars and trim whitespace"""
    if isinstance(value, str):
        return ZERO_WIDTH_RE.sub('', value).strip()
    return value


def json_dir_to_csv(
    input_dir,
    output_csv,
    endswith_text="s4.json",
    verbose=True
):
    """
    Read JSON files from a directory and write extracted data to a CSV.

    Args:
        input_dir (str): Directory containing JSON files
        output_csv (str): Output CSV file path
        endswith_text (str): Filename suffix to filter JSON files
        verbose (bool): Print progress messages

    Returns:
        int: Number of rows written to CSV
    """
    rows = []

    for filename in os.listdir(input_dir):
        if not filename.endswith(endswith_text):
            if verbose:
                print(f"Skipping unrelated file: {filename}")
            continue

        company_name = clean(filename.replace(endswith_text, ""))
        file_path = os.path.join(input_dir, filename)

        with open(file_path, "r", encoding="utf-8") as f:
            try:
                people = json.load(f)
            except json.JSONDecodeError:
                if verbose:
                    print(f"Skipping invalid JSON: {filename}")
                continue

        for person in people:
            urls = person.get("URLs", {}) or {}
            linkedin = clean(urls.get("linkedin", ""))
            twitter = clean(urls.get("twitter", ""))

            rows.append({
                "person_name": clean(person.get("fullName", "")),
                "location": clean(person.get("location", "")),
                "linkedin": linkedin,
                "twitter": twitter,
                "company_name": company_name,
                "linkedin_id": linkedin.split("/")[-1] if linkedin else "",
            })

    # Write CSV (Excel-safe)
    with open(output_csv, "w", newline="", encoding="utf-8-sig") as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=[
                "person_name",
                "location",
                "linkedin",
                "twitter",
                "company_name",
                "linkedin_id"
            ]
        )
        writer.writeheader()
        writer.writerows(rows)

    if verbose:
        print(f"✅ Created {output_csv} with {len(rows)} rows")

    return len(rows)
