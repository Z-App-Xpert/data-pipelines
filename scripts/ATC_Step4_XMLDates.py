import csv
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

# =========================
# FILE PATHS
# =========================
STEP3_INPUT = Path("output/ATC_CIPHER_UPLOAD_2026.csv")
LATEST_XML = Path("latestHumanlist.xml")
OUTPUT_FILE = Path("output/step3_output_with_fallback_dates.csv")

# =========================
# COLUMN NAMES IN STEP 3
# Change these if needed
# =========================
ATC_CODE_COL = "ATC_Code"
DATE_START_COL = "date_start"
ATC_NAME_COL = "ATC_Name" # optional, only used if present

# =========================
# HELPERS
# =========================
def parse_date(text: str):
    """Try to parse known date formats into a Python date."""
    if not text:
        return None

    text = text.strip()
    if not text:
        return None

    formats = [
    "%d/%m/%Y",
    "%Y-%m-%d",
    "%m/%d/%Y"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue

    return None


def format_date_yyyy_mm_dd(d):
    return d.isoformat() if d else ""


def normalize_code(code: str):
    return (code or "").strip().upper()


def clean_text(val: str):
    return (val or "").strip()


# =========================
# XML PARSING
# =========================
def build_atc_to_earliest_authorized_date(xml_path: Path):
    """
    Returns:
    dict[str, date]
    Maps each ATC code found in the XML to the earliest AuthorisedDate
    seen across all products.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()

    earliest_by_atc = {}

    for product in root.findall(".//Product"):
        auth_text = product.findtext("AuthorisedDate")
        auth_date = parse_date(auth_text)
        if not auth_date:
            continue

        atc_nodes = product.findall(".//ATCs/ATC")
        for atc_node in atc_nodes:
            atc_code = normalize_code(atc_node.text)
            if not atc_code:
                continue

        existing = earliest_by_atc.get(atc_code)
        if existing is None or auth_date < existing:
            earliest_by_atc[atc_code] = auth_date

    return earliest_by_atc


# =========================
# STEP 3 ENRICHMENT
# =========================
def enrich_step3_with_fallback_dates(
    step3_input: Path,
    output_file: Path,
    xml_lookup: dict
):
    with open(step3_input, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    # Add audit columns if they do not exist
    new_columns = [
        "xml_fallback_date_start",
        "date_start_final",
        "date_start_source",
        "review_note"
    ]
    for col in new_columns:
        if col not in fieldnames:
            fieldnames.append(col)

    updated_count = 0
    unresolved_count = 0

    for row in rows:
        atc_code = normalize_code(row.get(ATC_CODE_COL, ""))
        existing_start = clean_text(row.get(DATE_START_COL, ""))

        fallback_date = xml_lookup.get(atc_code)
        row["xml_fallback_date_start"] = format_date_yyyy_mm_dd(fallback_date)

        if existing_start:
            row["date_start_final"] = existing_start
            row["date_start_source"] = "existing_step3"
            row["review_note"] = ""
        elif fallback_date:
            row["date_start_final"] = format_date_yyyy_mm_dd(fallback_date)
            row["date_start_source"] = "hpra_authorised_date_fallback"
            row["review_note"] = "Filled from HPRA XML earliest observed AuthorisedDate"
            updated_count += 1
        else:
            row["date_start_final"] = ""
            row["date_start_source"] = "unresolved"
            row["review_note"] = "No Step 3 date_start and no XML fallback found"
            unresolved_count += 1

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done.")
    print(f"Output file: {output_file}")
    print(f"Rows updated from XML fallback: {updated_count}")
    print(f"Rows still unresolved: {unresolved_count}")

# =========================
# MAIN
# =========================
def main():
    print("Building ATC -> earliest AuthorisedDate lookup from XML...")
    xml_lookup = build_atc_to_earliest_authorized_date(LATEST_XML)
    print(f"Found fallback dates for {len(xml_lookup)} ATC codes in XML.")

    print("Enriching Step 3 output...")
    enrich_step3_with_fallback_dates(
        step3_input=STEP3_INPUT,
        output_file=OUTPUT_FILE,
        xml_lookup=xml_lookup
)

if __name__ == "__main__":
    main()
