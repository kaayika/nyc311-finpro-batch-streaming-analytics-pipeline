import requests
import pandas as pd

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"


BATCH_PERIODS = [
    ("January 2026", "2026-01-01", "2026-02-01"),
    ("February 2026", "2026-02-01", "2026-03-01"),
    ("March 2026", "2026-03-01", "2026-04-01"),
]


STREAM_START = "2026-04-01"
STREAM_END = "2026-05-01"


def request_api(params, timeout=60):
    response = requests.get(API_URL, params=params, timeout=timeout)

    response.raise_for_status()

    return response.json()


def validate_api():
    print("=" * 80)
    print("NYC 311 API VALIDATION")
    print("=" * 80)

    params = {
        "$where": (
            "created_date >= '2026-01-01T00:00:00.000' "
            "AND created_date < '2026-02-01T00:00:00.000'"
        ),
        "$limit": 100,
        "$order": "created_date, unique_key",
    }

    data = request_api(params)

    if not data:
        raise ValueError("API returned no data.")

    df = pd.DataFrame(data)

    print("\n[1] API Connection")
    print("Status : SUCCESS")

    print("\n[2] JSON Response")
    print("Status : SUCCESS")
    print(f"Rows received : {len(df):,}")

    print("\n[3] Dataset Columns")
    print(f"Total columns : {len(df.columns)}")

    for column in df.columns:
        print(f"- {column}")

    print("\n[4] Sample Data")
    print(df.head(3).to_string(index=False))

    return df


def count_data(start_date, end_date):
    params = {
        "$select": "count(*) as total",
        "$where": (
            f"created_date >= '{start_date}T00:00:00.000' "
            f"AND created_date < '{end_date}T00:00:00.000'"
        ),
    }

    data = request_api(params, timeout=300)

    return int(data[0]["total"])


def main():
    try:

        # ========================================
        # API VALIDATION
        # ========================================

        validate_api()

        print("\n" + "=" * 80)
        print("DATA VOLUME CHECK")
        print("=" * 80)

        # ========================================
        # BATCH DATA COUNT
        # ========================================

        batch_count = 0

        print("\nBatch Period")

        for month_name, start_date, end_date in BATCH_PERIODS:

            month_count = count_data(start_date, end_date)

            batch_count += month_count

            print(f"{month_name:<15} : " f"{month_count:,} rows")

        print(f"\nTotal Batch     : " f"{batch_count:,} rows")

        # ========================================
        # STREAMING DATA COUNT
        # ========================================

        stream_count = count_data(STREAM_START, STREAM_END)

        # Estimate only for initial volume check.
        # Actual streaming implementation will sample 10% from each day.
        estimated_stream_sample = int(stream_count * 0.10)

        print("\nStreaming Source Period")
        print("Period : April 2026")
        print(f"Rows   : {stream_count:,}")

        print("\nStreaming Sample")
        print("Usage  : 10%")
        print(f"Estimated Rows : " f"{estimated_stream_sample:,}")

        # ========================================
        # FINAL STATUS
        # ========================================

        print("\n" + "=" * 80)
        print("VALIDATION COMPLETED SUCCESSFULLY")
        print("=" * 80)

    except requests.exceptions.RequestException as error:

        print("\nAPI REQUEST FAILED")
        print(f"Error : {error}")

    except Exception as error:

        print("\nVALIDATION FAILED")
        print(f"Error : {error}")


if __name__ == "__main__":
    main()
