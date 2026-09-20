from pathlib import Path

import pandas as pd

DATA_DIR = Path("data/stream/sample")

EXPECTED_FILE_COUNT = 30
EXPECTED_TOTAL_ROWS = 30220


REQUIRED_COLUMNS = [
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "status",
    "borough",
    "incident_zip",
    "latitude",
    "longitude",
    "resolution_description",
]


CRITICAL_COLUMNS = [
    "unique_key",
    "created_date",
    "agency",
    "complaint_type",
    "status",
    "borough",
]


def main():

    print("=" * 80)
    print("NYC 311 STREAMING SAMPLE VALIDATION")
    print("=" * 80)

    # ========================================
    # 1. FILE COUNT CHECK
    # ========================================

    files = sorted(DATA_DIR.glob("nyc311_2026_04_*.jsonl"))

    print("\n[1] FILE COUNT CHECK")

    print(f"Expected files : " f"{EXPECTED_FILE_COUNT}")

    print(f"Actual files   : " f"{len(files)}")

    if len(files) != EXPECTED_FILE_COUNT:
        raise ValueError("Streaming sample file count mismatch.")

    print("Status         : SUCCESS")

    # ========================================
    # READ ALL FILES
    # ========================================

    dataframes = []

    for file_path in files:

        df = pd.read_json(file_path, lines=True)

        dataframes.append(df)

    df = pd.concat(dataframes, ignore_index=True)

    # ========================================
    # 2. ROW COUNT CHECK
    # ========================================

    print("\n[2] ROW COUNT CHECK")

    print(f"Expected rows : " f"{EXPECTED_TOTAL_ROWS:,}")

    print(f"Actual rows   : " f"{len(df):,}")

    if len(df) != EXPECTED_TOTAL_ROWS:
        raise ValueError("Streaming sample row count mismatch.")

    print("Status        : SUCCESS")

    # ========================================
    # 3. REQUIRED COLUMN CHECK
    # ========================================

    print("\n[3] REQUIRED COLUMN CHECK")

    missing_columns = [
        column for column in REQUIRED_COLUMNS if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(f"Missing columns : {missing_columns}")

    print(f"Required columns : " f"{len(REQUIRED_COLUMNS)}")

    print("Status           : SUCCESS")

    # ========================================
    # 4. DATE RANGE CHECK
    # ========================================

    print("\n[4] DATE RANGE CHECK")

    created_date = pd.to_datetime(df["created_date"], errors="coerce")

    min_date = created_date.min()
    max_date = created_date.max()

    print(f"Minimum created_date : " f"{min_date}")

    print(f"Maximum created_date : " f"{max_date}")

    if min_date < pd.Timestamp("2026-04-01") or max_date >= pd.Timestamp("2026-05-01"):
        raise ValueError("Invalid streaming date range.")

    print("Status               : SUCCESS")

    # ========================================
    # 5. DUPLICATE UNIQUE KEY CHECK
    # ========================================

    print("\n[5] DUPLICATE UNIQUE KEY CHECK")

    duplicate_count = df["unique_key"].duplicated().sum()

    print(f"Duplicate unique_key : " f"{duplicate_count:,}")

    if duplicate_count > 0:
        raise ValueError("Duplicate unique_key found.")

    print("Status               : SUCCESS")

    # ========================================
    # 6. CRITICAL NULL CHECK
    # ========================================

    print("\n[6] CRITICAL NULL CHECK")

    critical_null = df[CRITICAL_COLUMNS].isnull().sum()

    print(critical_null.to_string())

    if critical_null.sum() > 0:
        raise ValueError("Critical NULL value found.")

    print("Status : SUCCESS")

    # ========================================
    # FINAL STATUS
    # ========================================

    print("\n" + "=" * 80)
    print("STREAMING SAMPLE VALIDATION COMPLETED")
    print("=" * 80)

    print(f"Total files : " f"{len(files)}")

    print(f"Total rows  : " f"{len(df):,}")

    print("\nStatus : SUCCESS")


if __name__ == "__main__":
    main()
