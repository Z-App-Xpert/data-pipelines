import pandas as pd
import requests
import re
from bs4 import BeautifulSoup
import os
from datetime import date

URL = "https://atcddd.fhi.no/atc_ddd_alterations__cumulative/atc_alterations/"
OUT_DIR = "output"
YEAR = date.today().year
OUT_FILE = os.path.join(OUT_DIR, f"ATC_Alterations_2005_{YEAR}.csv")

os.makedirs(OUT_DIR, exist_ok=True)

resp = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
resp.raise_for_status()

soup = BeautifulSoup(resp.content, "html.parser")
table = soup.find("table")
if not table:
    raise RuntimeError(f"No table found at {URL}")

rows = table.find_all("tr")[1:]  # skip header row

data = []
skipped = 0

for i, row in enumerate(rows, start=1):
    cols = row.find_all("td")

    # Skip non-data rows (spacers, headers)
    if len(cols) < 4:
        skipped += 1
        continue

    prev_code = cols[0].get_text(strip=True)
    substance = cols[1].get_text(strip=True)
    new_code  = cols[2].get_text(strip=True)
    year      = cols[3].get_text(strip=True)

    # Some rows may be blank—skip those safely
    if not new_code or not year:
        skipped += 1
        continue

    def clean_atc_code(x):
        return re.sub(r"\s*\d+\)?$", "", str(x)).strip().upper()

    data.append({
        "previous_atc_code": clean_atc_code(prev_code),
        "substance_name": substance.strip(),
        "new_atc_code": clean_atc_code(new_code),
        "year_changed": year.strip()
    })


df = pd.DataFrame(data)
df.to_csv(OUT_FILE, index=False, encoding="utf-8")

print(f"✅ Step 2 complete: wrote {len(df)} rows to {OUT_FILE} (skipped {skipped} non-data rows)")
