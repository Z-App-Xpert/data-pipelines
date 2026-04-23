import os
import re
import sys
from io import StringIO
from time import sleep

import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup


# Increase recursion limit to handle deep recursion
sys.setrecursionlimit(100000)


def ensure_directory(dir_path: str) -> str:
    """Ensure output directory exists."""
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)
    return dir_path


# Set up output directory
OUT_DIR = ensure_directory("output")

# Define the ATC roots
ATC_ROOTS = ['A', 'B', 'C', 'D', 'G', 'H', 'J', 'L', 'M', 'N', 'P', 'R', 'S', 'V']
#ATC_ROOTS = ['A']   #TEST

# Validate ATC code structure
ATC_CODE_PATTERN = re.compile(
r'(^[A-Z]$)|(^[A-Z][a-zA-Z0-9]{1,25}$)|(^[A-Z][a-zA-Z0-9]{1,25}[A-Z]$)|(^[A-Z][a-zA-Z0-9]{1,25}[A-Z][a-zA-Z0-9]{1,25}$)'
)


def is_valid_atc_code(code: str) -> bool:
    """Validate an ATC code."""
    return bool(ATC_CODE_PATTERN.match(str(code).strip()))


def scrape_who_atc(root_atc_code: str, f_out) -> None:
    """
    Scrape ATC data recursively and write to file.

    This function scrapes and writes all data available from the WHO ATC/DDD
    index for the given ATC code and its subcodes.
    """
    if not is_valid_atc_code(root_atc_code):
        return

    web_address = f"https://www.whocc.no/atc_ddd_index/?code={root_atc_code}&showdescription=no"
    print(f"Scraping {web_address}")

    try:
        response = requests.get(web_address, timeout=60)
    except requests.RequestException as e:
        print(f"Request failed for {web_address}")
        return
    
    if response.status_code != 200:
            print(f"Error fetching {web_address} | status_code={response.status_code}")
            return

    soup = BeautifulSoup(response.content, "html.parser")
    atc_code_length = len(root_atc_code)

    # Process hierarchy levels (A, A01, A01A, etc.)
    if atc_code_length < 5:
        # Add the root node if needed
        if atc_code_length == 1:
            root_atc_code_name_elements = soup.select("#content a")
            if len(root_atc_code_name_elements) >= 3:
                root_atc_code_name = root_atc_code_name_elements[2].get_text(strip=True)
            else:
                root_atc_code_name = ""

            f_out.write(f"{root_atc_code}\t{root_atc_code_name}\t\t\t\t\n")

        # Process higher-level codes
        content_p = soup.select_one("#content > p:nth-of-type(2)")
        if content_p is None:
            print(f"No hierarchy paragraph found {root_atc_code}")
            return

        scraped_strings = content_p.get_text().split("\n")
        scraped_strings = [s.strip() for s in scraped_strings if s.strip()]
        if not scraped_strings:
            print(f"no hierarchy rows found for {root_atc_code}")
            return

        for scraped_string in scraped_strings:
            match = re.match(r'^(\S+)\s+(.+)$', scraped_string)
            if not match:
                continue

            atc_code = match.group(1).strip()
            atc_name = match.group(2).strip()

            if not re.match(r'^[A-Z](?:\d{2})?(?:[A-Z]{1,2})?$', atc_code):
                print(f"Skipping non-ATC token: {atc_code} | line: {scraped_string}")
                continue

   
            # Write hierarchy row
            f_out.write(f"{atc_code}\t{atc_name}\t\t\t\t\n")

            # Recurse into subcodes
            scrape_who_atc(atc_code, f_out)

    #Process detailed codes (full ATC + DD table)
    else:
        table = soup.select_one("ul > table")
        if table is None:
            print(f"No detailed table found for {root_atc_code}")
            return

        df_list = pd.read_html(StringIO(str(table)), header=0)
        if len(df_list) == 0:
            return

        df = df_list[0]
        df = df.rename(
            columns={
            "ATC code": "atc_code",
            "Name": "atc_name",
            "DDD": "ddd",
            "U": "uom",
            "Adm.R": "adm_r",
            "Note": "note",
            }
        )

        df = df.replace("", np.nan)

        # Fill down ATC code and name when WHO table leaves them blank
        if "atc_code" in df.columns:
            df["atc_code"] = df["atc_code"].ffill()
        if "atc_name" in df.columns:
            df["atc_name"] = df["atc_name"].ffill()

        # Ensure all expected columns exist
        for col in ["atc_code", "atc_name", "ddd", "uom", "adm_r", "note"]:
            if col not in df.columns:
                df[col] = np.nan

        # Write rows without header
        df[["atc_code", "atc_name", "ddd", "uom", "adm_r", "note"]].to_csv(
            f_out,
            sep="\t",
            index=False,
            header=False,
            lineterminator="\n",
        )


def main() -> None:
    out_file_name = os.path.join(
    OUT_DIR,
    f"WHO_ATC-DDD_{pd.Timestamp.now().strftime('%Y-%m-%d')}.csv"
    )
    print(f"Writing results to {out_file_name}")

    if os.path.exists(out_file_name):
        print("Warning: file already exists. It will be overwritten.")

    with open(out_file_name, "w", encoding="utf-8") as f_out:
        # Write header
        f_out.write("atc_code\tatc_name\tddd\tuom\tadm_r\tnote\n")

        # Request all root codes and recurse downward
        for atc_root in ATC_ROOTS:
            scrape_who_atc(atc_root, f_out)
            f_out.flush()

    print("Script execution completed.")


if __name__ == "__main__":
    main()

