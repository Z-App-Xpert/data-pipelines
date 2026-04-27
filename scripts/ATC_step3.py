import os
import glob
from pathlib import Path
import pandas as pd
import re


OUTPUT_DIR = "output"


def find_latest_file(pattern: str) -> str:
    matches = glob.glob(os.path.join(OUTPUT_DIR, pattern))
    if not matches:
        raise FileNotFoundError(f"No files found matching pattern: {pattern}")
    matches.sort(key=os.path.getmtime, reverse=True)
    return matches[0]

def build_display_name(row: pd.Series) -> str:
    atc_name = str(row.get("atc_name", "")).strip()
    ddd = "" if pd.isna(row.get("ddd")) else str(row.get("ddd")).strip()
    uom = "" if pd.isna(row.get("uom")) else str(row.get("uom")).strip()
    adm_r = "" if pd.isna(row.get("adm_r")) else str(row.get("adm_r")).strip()

    parts = [p for p in [ddd, uom, adm_r] if p]
    if parts:
        return f"{atc_name} ({' '.join(parts)})"
    return atc_name

def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Find the latest outputs from Step 1 and Step 2
    snapshot_file = find_latest_file("WHO_ATC-DDD_2026-04-27.csv")
    alterations_file = find_latest_file("ATC_Alterations_2005_2026.csv")

    print(f"Using snapshot file: {snapshot_file}")
    print(f"Using alterations file: {alterations_file}")

    # Load Step 1 snapshot
    df_atc = pd.read_csv(snapshot_file, sep='\t',dtype=str,engine='python')
    df_atc.columns = [c.strip() for c in df_atc.columns]
    df_atc["atc_code"] = df_atc["atc_code"].fillna("").astype(str).str.strip().str.rstrip(")")
    expected_atc_cols = {"atc_code", "atc_name", "ddd", "uom", "adm_r", "note"}
    missing_atc = expected_atc_cols - set(df_atc.columns)
    if missing_atc:
        raise ValueError(f"Snapshot file missing columns: {missing_atc}")

    for col in ["atc_code", "atc_name", "ddd", "uom", "adm_r", "note"]:
        df_atc[col] = df_atc[col].astype(str).replace("nan", "").str.strip()

    # Load Step 2 alterations
    df_alt = pd.read_csv(alterations_file, dtype=str)
    df_alt.columns = [c.strip() for c in df_alt.columns]
    df_alt["previous_atc_code"] = df_alt["previous_atc_code"].fillna("").astype(str).str.strip().str.rstrip(")")
    df_alt["new_atc_code"] = df_alt["new_atc_code"].fillna("").astype(str).str.strip().str.rstrip(")")

    expected_alt_cols = {"previous_atc_code", "substance_name", "new_atc_code", "year_changed"}
    missing_alt = expected_alt_cols - set(df_alt.columns)
    if missing_alt:
        raise ValueError(f"Alterations file missing columns: {missing_alt}")

    for col in ["previous_atc_code", "substance_name", "new_atc_code", "year_changed"]:
        df_alt[col] = df_alt[col].astype(str).replace("nan", "").str.strip()

    # Infer snapshot year from filename
    snapshot_name = Path(snapshot_file).name

    match = re.search(r"(20\d{2})", snapshot_name)
    if match:
        snapshot_year = match.group(1)
    else:
        raise ValueError(f"Could not infer snapshot year from filename: {snapshot_name}")

    print("Snapshot year:", snapshot_year)


    # Derive date_start:
    # earliest year_changed where code appears as new_atc_code
    df_alt_year = df_alt.copy()
    df_alt_year["year_changed_num"] = pd.to_numeric(df_alt_year["year_changed"], errors="coerce")

    date_start_map = (
        df_alt_year.dropna(subset=["year_changed_num"])
        .groupby("new_atc_code")["year_changed_num"]
        .min()
        .astype(int)
        .astype(str)
        .to_dict()
    )

    # Derive date_end:
    # latest year_changed where code appears as previous_atc_code
    date_end_map = (
        df_alt_year.dropna(subset=["year_changed_num"])
        .groupby("previous_atc_code")["year_changed_num"]
        .max()
        .astype(int)
        .astype(str)
        .to_dict()
    )

    # Build current rows from Step 1
    df_current = df_atc.copy()
    df_current["ATC_Code"] = df_current["atc_code"]
    df_current["ATC_Name"] = df_current.apply(build_display_name, axis=1)
    df_current["date_start"] = df_current["atc_code"].map(date_start_map).fillna(snapshot_year)
    df_current["date_end"] = pd.NA

    df_current_final = df_current[["ATC_Code", "ATC_Name", "date_start", "date_end"]].copy()

    # Build historical rows from Step 2
    # No "replaced by ..." text, per CIPHER feedback
    df_history = df_alt.copy()
    df_history["ATC_Code"] = df_history["previous_atc_code"]
    df_history["ATC_Name"] = df_history["substance_name"]
    df_history["date_start"] = pd.NA
    df_history["date_end"] = df_history["year_changed"]

    df_history_final = df_history[["ATC_Code", "ATC_Name", "date_start", "date_end"]].copy()

    # Combine
    df_final = pd.concat([df_history_final, df_current_final], ignore_index=True)

    # Clean whitespace and drop exact duplicates
    for col in ["ATC_Code", "ATC_Name", "date_start", "date_end"]:
        df_final[col] = df_final[col].astype("string").str.strip()

    df_final = df_final.drop_duplicates()

    # Save final output
    out_file = os.path.join(OUTPUT_DIR, f"ATC_CIPHER_UPLOAD_{snapshot_year}.csv")
    df_final.to_csv(out_file, index=False, encoding="utf-8")

    print(f"Step 3 complete. Wrote {len(df_final):,} rows to {out_file}")


if __name__ == "__main__":
    main()
