import os
import pandas as pd


def initialize_pipeline():
    print("Initializing ATC Step 1 pipeline...")


def create_output_directory():
    output_dir = "../outputs"
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory ready: {output_dir}")
    return output_dir


def create_empty_dataframe():
    columns = [
        "atc_code",
        "atc_name",
        "ddd",
        "unit",
        "route",
        "notes"
    ]
    df = pd.DataFrame(columns=columns)
    print("Empty ATC DataFrame created")
    return df


def save_dataframe(df, output_dir):
    file_path = os.path.join(output_dir, "step1_atc_snapshot.csv")
    df.to_csv(file_path, index=False)
    print(f"File saved: {file_path}")


if __name__ == "__main__":
    initialize_pipeline()
    output_dir = create_output_directory()
    df = create_empty_dataframe()
    save_dataframe(df, output_dir)