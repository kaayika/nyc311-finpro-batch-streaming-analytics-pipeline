from pathlib import Path

import pandas as pd

INPUT_DIR = Path("data/batch/monthly")
OUTPUT_DIR = Path("data/batch/daily")

MONTHLY_FILES = [
    INPUT_DIR / "nyc311_2026_01.parquet",
    INPUT_DIR / "nyc311_2026_02.parquet",
    INPUT_DIR / "nyc311_2026_03.parquet",
]


def main():

    print("=" * 80)
    print("NYC 311 MONTHLY TO DAILY PARQUET")
    print("=" * 80)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_input_rows = 0
    total_output_rows = 0
    total_daily_files = 0

    for file_path in MONTHLY_FILES:

        if not file_path.exists():
            raise FileNotFoundError(f"File not found : {file_path}")

        print(f"\nReading : {file_path}")

        df = pd.read_parquet(file_path)

        total_input_rows += len(df)

        df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

        invalid_created_date = df["created_date"].isna().sum()

        if invalid_created_date > 0:
            raise ValueError(
                f"{file_path.name} contains "
                f"{invalid_created_date} invalid created_date"
            )

        df["_created_date_only"] = df["created_date"].dt.date

        for date_value, daily_df in df.groupby("_created_date_only", sort=True):

            daily_df = daily_df.drop(columns=["_created_date_only"])

            month_string = date_value.strftime("%Y_%m")

            date_string = date_value.strftime("%Y_%m_%d")

            month_output_dir = OUTPUT_DIR / month_string

            month_output_dir.mkdir(parents=True, exist_ok=True)

            output_file = month_output_dir / f"nyc311_{date_string}.parquet"

            daily_df.to_parquet(output_file, index=False)

            total_output_rows += len(daily_df)
            total_daily_files += 1

            print(f"{date_value} : " f"{len(daily_df):,} rows")

    print("\n" + "=" * 80)
    print("DAILY PARQUET SPLIT COMPLETED")
    print("=" * 80)

    print(f"Input rows  : " f"{total_input_rows:,}")

    print(f"Output rows : " f"{total_output_rows:,}")

    print(f"Daily files : " f"{total_daily_files}")

    if total_input_rows != total_output_rows:
        raise ValueError("Row count mismatch after daily split.")

    print("\nStatus : SUCCESS")


if __name__ == "__main__":
    main()
