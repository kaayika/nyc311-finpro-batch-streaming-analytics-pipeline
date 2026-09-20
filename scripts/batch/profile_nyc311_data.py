import requests
import pandas as pd

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"


PROFILE_PERIODS = [
    ("January 2026", "2026-01-01", "2026-02-01"),
    ("February 2026", "2026-02-01", "2026-03-01"),
    ("March 2026", "2026-03-01", "2026-04-01"),
    ("April 2026", "2026-04-01", "2026-05-01"),
]


SAMPLE_PER_MONTH = 2000


def request_api(params, timeout=120):
    response = requests.get(API_URL, params=params, timeout=timeout)

    response.raise_for_status()

    return response.json()


def get_month_sample(month_name, start_date, end_date):
    params = {
        "$where": (
            f"created_date >= '{start_date}T00:00:00.000' "
            f"AND created_date < '{end_date}T00:00:00.000'"
        ),
        "$limit": SAMPLE_PER_MONTH,
        "$order": "created_date, unique_key",
    }

    data = request_api(params)

    if not data:
        print(f"{month_name} : No data")
        return pd.DataFrame()

    df = pd.DataFrame(data)

    # Add source month for profiling comparison
    df["profile_month"] = month_name

    print(f"{month_name:<15} : " f"{len(df):,} rows")

    return df


def main():
    try:
        print("=" * 80)
        print("NYC 311 INITIAL DATA PROFILING")
        print("=" * 80)

        # ========================================
        # GET SAMPLE FROM EACH MONTH
        # ========================================

        monthly_samples = []

        print("\n[1] MONTHLY SAMPLE")

        for month_name, start_date, end_date in PROFILE_PERIODS:

            month_df = get_month_sample(month_name, start_date, end_date)

            if not month_df.empty:
                monthly_samples.append(month_df)

        if not monthly_samples:
            raise ValueError("No data found for profiling.")

        # Combine all monthly samples
        df = pd.concat(monthly_samples, ignore_index=True)

        # ========================================
        # 2. ROW COUNT
        # ========================================

        print("\n[2] ROW COUNT")

        print(f"Total sample rows : {len(df):,}")
        print(f"Total columns     : {len(df.columns)}")

        # ========================================
        # 3. COLUMN NAMES
        # ========================================

        print("\n[3] COLUMN NAMES")

        for column in df.columns:
            print(f"- {column}")

        # ========================================
        # 4. DATA TYPES
        # ========================================

        print("\n[4] DATA TYPES")

        print(df.dtypes.to_string())

        # ========================================
        # 5. NULL VALUES
        # ========================================

        print("\n[5] NULL VALUES")

        null_summary = pd.DataFrame(
            {
                "null_count": df.isnull().sum(),
                "null_percentage": (df.isnull().mean() * 100).round(2),
            }
        )

        null_summary = null_summary.sort_values(by="null_percentage", ascending=False)

        print(null_summary.to_string())

        # ========================================
        # 6. DUPLICATE CHECK
        # ========================================

        print("\n[6] DUPLICATE CHECK")

        if "unique_key" in df.columns:

            duplicate_unique_key = df["unique_key"].duplicated().sum()

            print(f"Duplicate unique_key : " f"{duplicate_unique_key:,}")

        # ========================================
        # 7. SAMPLE VALUES
        # ========================================

        print("\n[7] SAMPLE VALUES")

        sample_columns = [
            "profile_month",
            "unique_key",
            "created_date",
            "closed_date",
            "agency",
            "agency_name",
            "complaint_type",
            "descriptor",
            "status",
            "borough",
        ]

        available_columns = [
            column for column in sample_columns if column in df.columns
        ]

        print(df[available_columns].head(15).to_string(index=False))

        # ========================================
        # 8. DATE RANGE CHECK
        # ========================================

        print("\n[8] CREATED DATE RANGE")

        if "created_date" in df.columns:

            created_date = pd.to_datetime(df["created_date"], errors="coerce")

            print(f"Minimum created_date : " f"{created_date.min()}")

            print(f"Maximum created_date : " f"{created_date.max()}")

        # ========================================
        # 9. MONTHLY NULL SUMMARY
        # ========================================

        print("\n[9] MONTHLY BASIC SUMMARY")

        important_columns = [
            "unique_key",
            "created_date",
            "closed_date",
            "agency",
            "complaint_type",
            "status",
            "borough",
        ]

        for month_name in df["profile_month"].unique():

            month_df = df[df["profile_month"] == month_name]

            print("\n" + "-" * 60)
            print(month_name)
            print("-" * 60)

            print(f"Rows : {len(month_df):,}")

            for column in important_columns:

                if column in month_df.columns:

                    null_count = month_df[column].isnull().sum()

                    null_percentage = month_df[column].isnull().mean() * 100

                    print(
                        f"{column:<20} "
                        f"Null: {null_count:>5,} "
                        f"({null_percentage:.2f}%)"
                    )

        # ========================================
        # FINAL STATUS
        # ========================================

        print("\n" + "=" * 80)
        print("INITIAL DATA PROFILING COMPLETED")
        print("=" * 80)

    except requests.exceptions.RequestException as error:

        print("\nAPI REQUEST FAILED")
        print(f"Error : {error}")

    except Exception as error:

        print("\nPROFILING FAILED")
        print(f"Error : {error}")


if __name__ == "__main__":
    main()
