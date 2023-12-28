from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from supabase import create_client, Client
from datetime import datetime
from dateutil import parser
import re
import unicodedata

# Google Sheets and Supabase setup (as you already have)
SERVICE_ACCOUNT_FILE = "keygoogle/email-parsing-406909-231994f0fedc.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEET_ID = "1O7A06oyw5uW-ysw7R04FzohQ2506-G6iSEQpR4p-dFo"
RANGE_NAME = "ACCT-Order!A1:AI"
SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def parse_date(date_str):
    if (
        not date_str
        or date_str.strip() == ""
        or date_str.lower() == "n/a"
        or date_str == "-"
    ):
        return None
    try:
        return parser.parse(date_str, fuzzy=True).date().isoformat()
    except ValueError:
        return None


def safe_int_conversion(value):
    try:
        if value and value.strip() != "":
            return int(value)
        else:
            return None
    except ValueError:
        return None


def format_column(column_name, value):
    if value == "-" or value == "":
        return None

    date_columns = [
        "export_date",
        "earliest_estimated_arrival_date",
        "latest_estimated_arrival_date",
        "ngay_het_han",
    ]
    int_columns = [
        "final_qty",
        "estimate_arrival_qty",
        "difference",
        "expiration_return",
        "qty_receive_1",
        "qty_receive_2",
        "qty_receive_3",
        "qty_receive_4",
        "qty_receive_5",
    ] + [f"tracking_number_{i}" for i in range(1, 11)]

    if column_name in date_columns:
        return parse_date(value)

    if column_name in int_columns:
        return safe_int_conversion(value)

    return value


def format_header(header):
    # Convert to lowercase
    header = header.lower()
    # Replace spaces with underscores
    header = header.replace(" ", "_")
    # Remove Vietnamese characters by decomposing and keeping only ASCII
    header = (
        unicodedata.normalize("NFKD", header).encode("ASCII", "ignore").decode("ASCII")
    )
    return header


def main():
    # Fetch data from Google Sheets
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()
    result = (
        sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
    )
    values = result.get("values", [])

    if not values:
        print("No data found.")
        return

    # Extract the header row
    headers = values[0]
    headers = [format_header(h) for h in values[0]]

    # Delete all existing data in the table
    delete_response = (
        supabase.table("data_warehouse_raw").delete().gte("code", 0).execute()
    )  # or any other always-true condition

    # Check for an error in the delete response
    if hasattr(delete_response, "error") and delete_response.error is not None:
        print(f"Error deleting existing data: {delete_response.error}")
        return

    # Prepare and push data to Supabase
    for row in values[1:]:  # Skip the header row
        data = {
            headers[i]: format_column(headers[i], row[i]) if i < len(row) else None
            for i in range(len(headers))
        }
        response = supabase.table("data_warehouse_raw").insert(data).execute()

        # Check for an error in the response
        if hasattr(response, "error") and response.error is not None:
            print(f"Error inserting row: {response.error}")
        else:
            print(f"Row inserted: {data}".encode("utf-8", errors="replace"))


if __name__ == "__main__":
    main()
