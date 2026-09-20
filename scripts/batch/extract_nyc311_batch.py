import os
import time

import pandas as pd
import requests
import pyarrow as pa
import pyarrow.parquet as pq

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

OUTPUT_DIR = "data/batch/monthly"

PAGE_SIZE = 25000


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


BATCH_PERIODS = [
    ("2026_01", "2026-01-01", "2026-02-01"),
    ("2026_02", "2026-02-01", "2026-03-01"),
    ("2026_03", "2026-03-01", "2026-04-01"),
]


def request_api(params, timeout=180, max_retries=5):

    for attempt in range(1, max_retries + 1):

        try:

            response = requests.get(API_URL, params=params, timeout=timeout)

            response.raise_for_status()

            return response.json()

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as error:

            print(f"Request failed " f"(attempt {attempt}/{max_retries})")

            print(f"Error : {error}")

            if attempt == max_retries:
                raise

            wait_time = attempt * 5

            print(f"Retrying in " f"{wait_time} seconds...")

            time.sleep(wait_time)


def convert_data_types(df):

    df = df.reindex(columns=REQUIRED_COLUMNS)

    string_columns = [
        "unique_key",
        "agency",
        "agency_name",
        "complaint_type",
        "descriptor",
        "status",
        "borough",
        "incident_zip",
        "resolution_description",
    ]

    for column in string_columns:
        df[column] = df[column].astype("string")

    df["created_date"] = pd.to_datetime(df["created_date"], errors="coerce")

    df["closed_date"] = pd.to_datetime(df["closed_date"], errors="coerce")

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")

    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    return df


def extract_month(period_name, start_date, end_date):

    print("\n" + "=" * 80)
    print(f"EXTRACTING : {period_name}")
    print("=" * 80)

    output_file = os.path.join(OUTPUT_DIR, f"nyc311_{period_name}.parquet")

    offset = 0
    total_rows = 0
    writer = None

    try:

        while True:

            params = {
                "$select": ",".join(REQUIRED_COLUMNS),
                "$where": (
                    f"created_date >= '{start_date}T00:00:00.000' "
                    f"AND created_date < '{end_date}T00:00:00.000'"
                ),
                "$order": "created_date, unique_key",
                "$limit": PAGE_SIZE,
                "$offset": offset,
            }

            print(f"Fetching rows " f"{offset:,} - " f"{offset + PAGE_SIZE - 1:,}")

            data = request_api(params)

            if not data:
                break

            df = pd.DataFrame(data)

            df = convert_data_types(df)

            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:

                writer = pq.ParquetWriter(
                    output_file, table.schema, compression="snappy"
                )

            writer.write_table(table)

            rows_received = len(df)

            total_rows += rows_received
            offset += rows_received

            print(f"Rows received : " f"{rows_received:,}")

            print(f"Total written : " f"{total_rows:,}")

            if rows_received < PAGE_SIZE:
                break

            time.sleep(0.1)

    finally:

        if writer is not None:
            writer.close()

    print(f"\nCompleted : " f"{period_name}")

    print(f"Total rows : " f"{total_rows:,}")

    print(f"File       : " f"{output_file}")

    return total_rows


def main():

    try:

        print("=" * 80)
        print("NYC 311 BATCH DATA EXTRACTION")
        print("=" * 80)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        grand_total = 0

        for period_name, start_date, end_date in BATCH_PERIODS:

            month_total = extract_month(period_name, start_date, end_date)

            grand_total += month_total

        print("\n" + "=" * 80)
        print("BATCH EXTRACTION COMPLETED")
        print("=" * 80)

        print(f"Grand Total : " f"{grand_total:,} rows")

    except requests.exceptions.RequestException as error:

        print("\nAPI REQUEST FAILED")
        print(f"Error : {error}")
        raise

    except Exception as error:

        print("\nBATCH EXTRACTION FAILED")
        print(f"Error : {error}")
        raise


if __name__ == "__main__":
    main()
