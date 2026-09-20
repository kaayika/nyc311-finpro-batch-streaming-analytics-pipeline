from datetime import datetime, timedelta
from pathlib import Path
import time

import pandas as pd
import requests


API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

OUTPUT_DIR = Path("data/stream/sample")

PAGE_SIZE = 25000
SAMPLE_FRACTION = 0.10
RANDOM_STATE = 42


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


def request_api(params, timeout=180, max_retries=5):

    for attempt in range(1, max_retries + 1):

        try:

            response = requests.get(
                API_URL,
                params=params,
                timeout=timeout,
            )

            response.raise_for_status()

            return response.json()

        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
        ) as error:

            print(
                f"Request failed "
                f"(attempt {attempt}/{max_retries})"
            )

            print(f"Error : {error}")

            if attempt == max_retries:
                raise

            wait_time = attempt * 5

            print(
                f"Retrying in "
                f"{wait_time} seconds..."
            )

            time.sleep(wait_time)


def get_daily_data(start_date, end_date):

    offset = 0
    daily_rows = []

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

        data = request_api(params)

        if not data:
            break

        daily_rows.extend(data)

        rows_received = len(data)

        offset += rows_received

        if rows_received < PAGE_SIZE:
            break

        time.sleep(0.1)

    return pd.DataFrame(daily_rows)


def main():

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    current_date = datetime(2026, 4, 1)
    end_period = datetime(2026, 5, 1)

    total_source_rows = 0
    total_sample_rows = 0

    print("=" * 80)
    print("NYC 311 STREAMING SAMPLE PREPARATION")
    print("=" * 80)

    while current_date < end_period:

        next_date = current_date + timedelta(days=1)

        date_string = current_date.strftime("%Y-%m-%d")
        file_date = current_date.strftime("%Y_%m_%d")

        print("\n" + "-" * 80)
        print(f"Date : {date_string}")
        print("-" * 80)

        df = get_daily_data(
            current_date.strftime("%Y-%m-%d"),
            next_date.strftime("%Y-%m-%d"),
        )

        if df.empty:

            print("No data found.")

            current_date = next_date
            continue

        df = df.reindex(
            columns=REQUIRED_COLUMNS
        )

        source_rows = len(df)

        sample_size = round(
            source_rows * SAMPLE_FRACTION
        )

        sample_df = df.sample(
            n=sample_size,
            random_state=RANDOM_STATE,
        )

        sample_df = sample_df.sort_values(
            by=[
                "created_date",
                "unique_key",
            ]
        )

        output_file = (
            OUTPUT_DIR
            / f"nyc311_{file_date}.jsonl"
        )

        sample_df.to_json(
            output_file,
            orient="records",
            lines=True,
            force_ascii=False,
        )

        total_source_rows += source_rows
        total_sample_rows += len(sample_df)

        print(
            f"Source rows : "
            f"{source_rows:,}"
        )

        print(
            f"Sample rows : "
            f"{len(sample_df):,}"
        )

        print(
            f"Sample rate : "
            f"{len(sample_df) / source_rows * 100:.2f}%"
        )

        print(
            f"Output      : "
            f"{output_file}"
        )

        current_date = next_date

    print("\n" + "=" * 80)
    print("STREAMING SAMPLE PREPARATION COMPLETED")
    print("=" * 80)

    print(
        f"Total source rows : "
        f"{total_source_rows:,}"
    )

    print(
        f"Total sample rows : "
        f"{total_sample_rows:,}"
    )

    print(
        f"Overall rate      : "
        f"{total_sample_rows / total_source_rows * 100:.2f}%"
    )


if __name__ == "__main__":
    main()
