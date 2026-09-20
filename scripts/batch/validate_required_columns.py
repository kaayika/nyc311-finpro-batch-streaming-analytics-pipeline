import requests
import pandas as pd

API_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"


# ========================================
# REQUIRED COLUMNS
# ========================================

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


# ========================================
# TARGET DATA TYPES
# ========================================
#
# Target schema represents the expected
# data types after cleaning & transformation.
#
# Raw / staging data types may differ
# depending on the Parquet representation.
# ========================================

TARGET_SCHEMA = {
    "unique_key": "STRING",
    "created_date": "TIMESTAMP",
    "closed_date": "TIMESTAMP",
    "agency": "STRING",
    "agency_name": "STRING",
    "complaint_type": "STRING",
    "descriptor": "STRING",
    "status": "STRING",
    "borough": "STRING",
    "incident_zip": "STRING",
    "latitude": "FLOAT64",
    "longitude": "FLOAT64",
    "resolution_description": "STRING",
}


def request_api(params, timeout=120):

    response = requests.get(API_URL, params=params, timeout=timeout)

    response.raise_for_status()

    return response.json()


def format_value(value):

    if pd.isna(value):
        return "NULL"

    value = str(value)

    # Prevent very long text from flooding the terminal
    if len(value) > 500:
        return value[:500] + "..."

    return value


def main():

    try:

        print("=" * 80)
        print("NYC 311 REQUIRED COLUMNS VALIDATION")
        print("=" * 80)

        # ========================================
        # 1. REQUIRED COLUMNS
        # ========================================

        print("\n[1] REQUIRED COLUMNS")

        for column in REQUIRED_COLUMNS:
            print(f"- {column}")

        print(f"\nTotal required columns : " f"{len(REQUIRED_COLUMNS)}")

        # ========================================
        # 2. VALIDATE COLUMNS AGAINST API
        # ========================================

        params = {
            "$select": ",".join(REQUIRED_COLUMNS),
            "$where": (
                "created_date >= '2026-01-01T00:00:00.000' "
                "AND created_date < '2026-05-01T00:00:00.000'"
            ),
            "$limit": 100,
            "$order": "created_date DESC, unique_key",
        }

        data = request_api(params)

        if not data:
            raise ValueError("API returned no data.")

        df = pd.DataFrame(data)

        print("\n[2] COLUMN VALIDATION")
        print("Status : SUCCESS")
        print("All selected column names are accepted " "by the NYC 311 API.")

        # ========================================
        # 3. TARGET DATA TYPE
        # ========================================

        print("\n[3] TARGET DATA TYPE")
        print("(Expected after cleaning & transformation)\n")

        for column, data_type in TARGET_SCHEMA.items():

            print(f"{column:<30} : " f"{data_type}")

        # ========================================
        # 4. SAMPLE DATA
        # ========================================

        print("\n[4] SAMPLE DATA")

        # Ensure all required columns are available
        # in the DataFrame even when some fields
        # are missing from the API response.
        df = df.reindex(columns=REQUIRED_COLUMNS)

        # Choose the row with the most available values
        sample_index = df[REQUIRED_COLUMNS].notna().sum(axis=1).idxmax()

        sample_row = df.loc[sample_index]

        for column in REQUIRED_COLUMNS:

            value = sample_row.get(column, None)

            print(f"{column:<30} : " f"{format_value(value)}")

        # ========================================
        # FINAL STATUS
        # ========================================

        print("\n" + "=" * 80)
        print("REQUIRED COLUMNS VALIDATION COMPLETED")
        print("=" * 80)

    except requests.exceptions.RequestException as error:

        print("\nAPI REQUEST FAILED")
        print(f"Error : {error}")

    except Exception as error:

        print("\nVALIDATION FAILED")
        print(f"Error : {error}")


if __name__ == "__main__":
    main()
