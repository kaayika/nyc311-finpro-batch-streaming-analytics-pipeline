import os

import pandas as pd
import pyarrow.parquet as pq

DATA_DIR = "data/batch/monthly"


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


EXPECTED_DATA = {
    "2026_01": {
        "rows": 348511,
        "start": "2026-01-01",
        "end": "2026-02-01",
    },
    "2026_02": {
        "rows": 334690,
        "start": "2026-02-01",
        "end": "2026-03-01",
    },
    "2026_03": {
        "rows": 342388,
        "start": "2026-03-01",
        "end": "2026-04-01",
    },
}


def validate_file(period_name, expected_rows, start_date, end_date):

    file_path = os.path.join(DATA_DIR, f"nyc311_{period_name}.parquet")

    print("\n" + "=" * 80)
    print(f"VALIDATING : {period_name}")
    print("=" * 80)

    # ========================================
    # 1. FILE CHECK
    # ========================================

    print("\n[1] FILE CHECK")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found : {file_path}")

    print("Status : SUCCESS")
    print(f"File   : {file_path}")

    # ========================================
    # 2. ROW COUNT CHECK
    # ========================================

    print("\n[2] ROW COUNT CHECK")

    parquet_file = pq.ParquetFile(file_path)

    actual_rows = parquet_file.metadata.num_rows

    print(f"Expected rows : {expected_rows:,}")
    print(f"Actual rows   : {actual_rows:,}")

    if actual_rows != expected_rows:
        raise ValueError(f"Row count mismatch for {period_name}")

    print("Status        : SUCCESS")

    # ========================================
    # 3. REQUIRED COLUMN CHECK
    # ========================================

    print("\n[3] REQUIRED COLUMN CHECK")

    available_columns = parquet_file.schema_arrow.names

    missing_columns = [
        column for column in REQUIRED_COLUMNS if column not in available_columns
    ]

    if missing_columns:
        raise ValueError(f"Missing columns : {missing_columns}")

    print(f"Required columns : " f"{len(REQUIRED_COLUMNS)}")

    print("Status           : SUCCESS")

    # ========================================
    # 4. DATE RANGE CHECK
    # ========================================

    print("\n[4] DATE RANGE CHECK")

    date_df = pd.read_parquet(file_path, columns=["created_date"])

    min_date = date_df["created_date"].min()
    max_date = date_df["created_date"].max()

    start_timestamp = pd.Timestamp(start_date)
    end_timestamp = pd.Timestamp(end_date)

    print(f"Minimum created_date : " f"{min_date}")

    print(f"Maximum created_date : " f"{max_date}")

    if min_date < start_timestamp or max_date >= end_timestamp:
        raise ValueError(f"Invalid date range for {period_name}")

    print("Status               : SUCCESS")

    # ========================================
    # 5. DUPLICATE UNIQUE KEY CHECK
    # ========================================

    print("\n[5] DUPLICATE UNIQUE KEY CHECK")

    key_df = pd.read_parquet(file_path, columns=["unique_key"])

    duplicate_count = key_df["unique_key"].duplicated().sum()

    print(f"Duplicate unique_key : " f"{duplicate_count:,}")

    if duplicate_count > 0:
        raise ValueError(f"Duplicate unique_key found in {period_name}")

    print("Status               : SUCCESS")

    return actual_rows


def main():

    try:

        print("=" * 80)
        print("NYC 311 BATCH EXTRACTION VALIDATION")
        print("=" * 80)

        grand_total = 0

        for period_name, config in EXPECTED_DATA.items():

            month_rows = validate_file(
                period_name, config["rows"], config["start"], config["end"]
            )

            grand_total += month_rows

        # ========================================
        # FINAL RESULT
        # ========================================

        expected_total = sum(config["rows"] for config in EXPECTED_DATA.values())

        print("\n" + "=" * 80)
        print("BATCH VALIDATION COMPLETED")
        print("=" * 80)

        print(f"Expected Total : " f"{expected_total:,} rows")

        print(f"Actual Total   : " f"{grand_total:,} rows")

        if grand_total != expected_total:
            raise ValueError("Grand total row count mismatch.")

        print("\nStatus : SUCCESS")

    except Exception as error:

        print("\nBATCH VALIDATION FAILED")
        print(f"Error : {error}")
        raise


if __name__ == "__main__":
    main()
