from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from supabase import create_client, Client
from datetime import datetime
from dateutil import parser
import re

# Google Sheets and Supabase setup (as you already have)
SERVICE_ACCOUNT_FILE = (
    "Keepa/email_tracking/keygoogle/email-parsing-406909-231994f0fedc.json"
)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEET_ID = "1YI_fEURtgM0qTcKPPtpJy5sK-wjpixWKBr0yCihIQGo"  # "1dCeofufqB03beEf15XZc6jB_LGs1Ih5_rfLZfgpd5kk"
RANGE_NAME = "label Email_Tracking!A1:AH"
SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def parse_date(date_str):
    if not date_str or date_str.strip() == "" or date_str.lower() == "n/a":
        return None
    try:
        return parser.parse(date_str, fuzzy=True).date().isoformat()
    except (ValueError, TypeError):
        return None


def safe_int_conversion(value):
    try:
        return int(value)
    except ValueError:
        return None


def parse_string(date_str):
    if not date_str or date_str.strip() == "" or date_str.lower() == "n/a":
        return None
    try:
        return date_str
    except (ValueError, TypeError):
        return None


def extract_number(price):
    if price is None:
        return None
    # Find all numeric parts in the string
    matches = re.findall(r"\d+\.?\d*", price.replace(",", ""))
    if matches:
        # Return the first match as a float
        return float(matches[0])
    else:
        return None


def process_order_numbers(str_order):
    """
    Processes the order numbers in a DataFrame by removing hyphens.

    Args:
    df (pd.DataFrame): The DataFrame containing the order numbers.
    column_name (str): The name of the column containing the order numbers.

    Returns:
    pd.DataFrame: The DataFrame with processed order numbers.
    """
    # Replace hyphens with nothing (essentially removing them)

    return str_order.replace("-", "")


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

    # Delete all existing data in the table
    delete_response = (
        supabase.table("email_tracking_raw")
        .delete()
        .neq("email_sender", None)
        .execute()
    )

    # Check for an error in the delete response
    if hasattr(delete_response, "error") and delete_response.error is not None:
        print(f"Error deleting existing data: {delete_response.error}")
        return

    # Prepare and push data to Supabase
    for row in values[1:]:  # Skip the header row
        # Convert date strings to date objects if necessary
        email_sent_date = parse_date(row[7]) if len(row) > 7 else None
        order_date = parse_date(row[15]) if len(row) > 15 else None
        earliest_estimate_arrival_date = parse_date(row[20]) if len(row) > 20 else None

        # Map columns from Google Sheet to Supabase table columns
        data = {
            "email_sent_date": email_sent_date,
            "email_sender": parse_string(row[2]) if len(row) > 2 else None,
            "order_number": parse_string(process_order_numbers(row[14]))
            if len(row) > 14
            else None,
            "order_date": order_date,
            "product_name": parse_string(row[16]) if len(row) > 16 else None,
            "size": parse_string(row[12])
            if len(row) > 12 and parse_string(row[12])
            else "No size",
            "color": parse_string(row[13])
            if len(row) > 13 and parse_string(row[13])
            else "No color",
            "author": parse_string(row[25]) if len(row) > 25 else None,
            "sku": parse_string(row[26]) if len(row) > 26 else None,
            "quantity": safe_int_conversion(row[27]) if len(row) > 27 else None,
            "total_items": safe_int_conversion(row[28]) if len(row) > 28 else None,
            "price": extract_number(parse_string(row[29])) if len(row) > 29 else None,
            "shipping_address": parse_string(row[30]) if len(row) > 30 else None,
            "product_category": parse_string(row[17]) if len(row) > 17 else None,
            "tracking_number": parse_string(row[18]) if len(row) > 18 else None,
            "estimate_arrival_quantity": safe_int_conversion(row[19])
            if len(row) > 19
            else None,
            "earliest_estimate_arrival_date": earliest_estimate_arrival_date,
            "delivery_status": parse_string(row[21].lower()) if len(row) > 21 else None,
            "quantity_received": safe_int_conversion(row[22])
            if len(row) > 22
            else None,
            "link_to_check_status": parse_string(row[23]) if len(row) > 23 else None,
            "email_purpose": parse_string(row[24]) if len(row) > 24 else None,
            "shipping_fee": extract_number(parse_string(row[31]))
            if len(row) > 31
            else None,
            "total_order_amount": extract_number(parse_string(row[32]))
            if len(row) > 32
            else None,
            "total_tax": extract_number(parse_string(row[33]))
            if len(row) > 33
            else None,
        }

        # Check if the record already exists based on composite primary key
        query = supabase.table("email_tracking_raw").select("*")
        conditions = [
            ("product_name", parse_string(row[16]) if len(row) > 16 else None),
            (
                "color",
                parse_string(row[13])
                if len(row) > 13 and parse_string(row[13])
                else "No color",
            ),
            (
                "size",
                parse_string(row[12])
                if len(row) > 12 and parse_string(row[12])
                else "No size",
            ),
            (
                "order_number",
                parse_string(process_order_numbers(row[14])) if len(row) > 14 else None,
            ),
            ("tracking_number", parse_string(row[18]) if len(row) > 18 else None),
            ("email_sent_date", email_sent_date),
        ]
        for field, value in conditions:
            if value is not None:
                query = query.eq(field, value)

        existing_records = query.execute()
        if existing_records.data and len(existing_records.data) > 0:
            print(f"Skipping duplicate record for composite key: {conditions}")
            continue

        # Check if any primary key field is NULL
        primary_key_fields = [
            data.get("product_name"),
            data.get("order_number"),
            data.get("tracking_number"),
            data.get("email_sent_date"),
        ]
        if any(field is None for field in primary_key_fields):
            print(f"Skipping insertion due to NULL in primary key for data")
            continue  # Skip this iteration and proceed to the next row

        response = supabase.table("email_tracking_raw").insert(data).execute()

        # Check for an error in the response
        if hasattr(response, "error") and response.error is not None:
            print(f"Error inserting row: {response.error}")
        else:
            print(f"Row inserted: {data}".encode("utf-8", errors="replace"))


if __name__ == "__main__":
    main()
