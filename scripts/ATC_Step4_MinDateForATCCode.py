import os
from datetime import datetime
import pandas as pd
import xml.etree.ElementTree as ET

from streamlit import code

def clean_tag(elem):
    return elem.tag.split("}")[-1].strip()

base_dir = os.path.dirname(__file__)
output_dir = os.path.join(base_dir, "..", "..", "output")

xml_file = os.path.join(output_dir, "latestHumanlist.xml")

print("XML path", xml_file)
print("Exists:", os.path.exists(xml_file))

tree = ET.parse(xml_file)
root = tree.getroot()

#atc_dates = {}

# for product in root.iter():
#     if clean_tag(product) != "Product":
#         continue

#     auth_date_text = None
#     atc_codes = []

#     for child in product.iter():
#         tag = clean_tag(child)

#         if tag == "AuthorisedDate":
#             auth_date_text = child.text

#         elif tag == "ATC" and child.text:
#             atc_codes.append(child.text.strip())

#     if not auth_date_text:
#         continue

#     try:
#         auth_date = datetime.strptime(auth_date_text.strip(), "%d/%m/%Y")
#     except ValueError:
#         continue

#     for atc_code in atc_codes:
#         if atc_code not in atc_dates:
#          atc_dates[atc_code] = auth_date
#         else:
#             atc_dates[atc_code] = min(atc_dates[atc_code], auth_date)

# print("ATC codes loaded from XML:", len(atc_dates))
# print("A01AD02 date:", atc_dates.get("A01AD02"))


# -----------------------------
# Paths

# -----------------------------

today = datetime.today().strftime("%Y-%m-%d")

base_dir = os.path.dirname(__file__)
output_dir = os.path.join(base_dir, "..","..", "output")
dated_output_dir = os.path.join(output_dir, today)

os.makedirs(dated_output_dir, exist_ok=True)

step3_file = os.path.join(output_dir, "ATC_CIPHER_UPLOAD_2026.csv")
xml_file = os.path.join(output_dir, "latestHumanlist.xml")

step4_output_file = os.path.join(
    dated_output_dir,
    f"ATC_CIPHER_UPLOAD_STEP4_{today}.csv"
)


# -----------------------------
# Read Step 3 CSV
# -----------------------------

df = pd.read_csv(step3_file)

if "ATC_Code" not in df.columns:
    raise ValueError("Step 3 file must contain an ATC_Code column.")


# -----------------------------
# Read XML and build ATC date lookup
# -----------------------------

tree = ET.parse(xml_file)
root = tree.getroot()

print("\nFirst 30 XML tags:")
for i, elem in enumerate(root.iter()):
    print(i, elem.tag, "=", elem.text[:50].strip() if elem.text else "")
    if i >= 30:
        break


print("Root tag:", root.tag)
# print("Products found:", len(root.findall(".//Product")))
# print("ATC nodes found:", len(root.findall(".//ATC")))

atc_dates = {}

for product in root.iter():
    if clean_tag(product) != "Product":
     continue

    auth_date_text = None
    atc_codes = []

    for child in product.iter():
        tag = clean_tag(child)

        if tag == "AuthorisedDate":
            auth_date_text = child.text

        elif tag == "ATC" and child.text:
            atc_codes.append(child.text.strip().upper())

    if not auth_date_text:
        continue

    try:
        auth_date = datetime.strptime(auth_date_text.strip(), "%d/%m/%Y")
    except ValueError:
        continue

    for atc_code in atc_codes:
        atc_dates[atc_code] = min(
            atc_dates.get(atc_code, auth_date),
            auth_date
    )

print("ATC codes loaded from XML:", len(atc_dates))
print("A01AD02 date:", atc_dates.get("A01AD02"))

# -----------------------------
# Add date_start to Step 4
# -----------------------------

# -----------------------------
# Find earliest XML date for ATC code
# Exact match first, then child-code prefix match
# -----------------------------

def find_earliest_xml_date(atc_code):
    code = str(atc_code).strip().upper()

    # 1. Exact match
    if code in atc_dates:
         atc_dates[code]

    # 2. Child match (XML has longer codes)
    child_dates = [
        date
        for xml_code, date in atc_dates.items()
        if xml_code.startswith(code)
    ]
    if child_dates:
        return min(child_dates)

    # 3. Parent match (XML has shorter codes)
    for i in range(len(code) - 1, 0, -1):
        parent = code[:i]
        if parent in atc_dates:
            return atc_dates[parent]

    return None

df["date_start"] = df["ATC_Code"].apply(find_earliest_xml_date)

df["date_start"] = pd.to_datetime(df["date_start"]).dt.strftime("%Y-%m-%d")

# -----------------------------
# Save Step 4 output
# -----------------------------

df.to_csv(step4_output_file, index=False)

print("Products found namespace-safe:", sum(1 for e in root.iter() if clean_tag(e) == "Product"))
print("ATC found namespace-safe:", sum(1 for e in root.iter() if clean_tag(e) == "ATC"))
print("AuthorisedDate found namespace-safe:", sum(1 for e in root.iter() if clean_tag(e) == "AuthorisedDate"))

print(f"Step 4 complete.")
print(f"Saved: {step4_output_file}")
print(f"ATC codes in Step 3: {len(df)}")
print(f"ATC codes matched to XML dates: {df['date_start'].notna().sum()}")
print(f"ATC codes without XML dates: {df['date_start'].isna().sum()}")
