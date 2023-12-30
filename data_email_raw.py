from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from supabase import create_client, Client
from datetime import datetime
from dateutil import parser
import re
import psycopg2
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

# Google Sheets and Supabase setup (as you already have)
SERVICE_ACCOUNT_FILE = "keygoogle/email-parsing-406909-231994f0fedc.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
SPREADSHEET_ID = "1YI_fEURtgM0qTcKPPtpJy5sK-wjpixWKBr0yCihIQGo"  # "1dCeofufqB03beEf15XZc6jB_LGs1Ih5_rfLZfgpd5kk"
RANGE_NAME = "label Email_Tracking!A1:AH"
SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

conn = psycopg2.connect(
    dbname="postgres",  # Usually, this is 'postgres'
    user="postgres",  # Usually, this is 'postgres'
    password="5giE*5Y5Uexi3P2",
    host="db.sxoqzllwkjfluhskqlfl.supabase.co",
)
cursor = conn.cursor()

# Identify the newest date
cursor.execute("SELECT MAX(email_sent_date) FROM email_tracking_raw")
newest_date = cursor.fetchone()[0]


def parse_date(date_str):
    if not date_str:
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

    cursor = conn.cursor()

    # Retrieve the maximum email_sent_date from existing rows
    cursor.execute("SELECT MAX(email_sent_date) FROM email_tracking_raw")
    max_date = pd.to_datetime(
        cursor.fetchone()[0], format="%b %d, %Y, %H:%M:%S", errors="coerce"
    )

    # Convert values to a DataFrame
    columns = values[0]
    data = values[1:]
    df = pd.DataFrame(data, columns=columns)

    # Convert the 'Timestamp' column to datetime
    df["Date & Time Sent"] = pd.to_datetime(
        df["Date & Time Sent"], format="%b %d, %Y, %H:%M:%S", errors="coerce"
    )

    # Filter new data based on timestamp
    new_data = df[df["Date & Time Sent"] > max_date]
    # Construct and execute the DELETE query
    delete_response = (
        supabase.table("email_tracking_raw")
        .delete()
        .eq("email_sent_date", newest_date)
        .execute()
    )

    # Check for an error in the delete response
    if hasattr(delete_response, "error") and delete_response.error is not None:
        print(f"Error deleting existing data: {delete_response.error}")
        return

    # Prepare and push data to Supabase
    for index, row in new_data.iterrows():  # Skip the header row
        # Assuming 'data' is a dictionary representing the new data to insert
        try:
            # Compare email_sent_date with the maximum date
            # Convert date strings to date objects if necessary
            email_sent_date = (
                row["Date & Time Sent"].strftime("%Y-%m-%d")
                if row["Date & Time Sent"]
                else None
            )
            order_date = parse_date(row["Order Date"]) if row["Order Date"] else None
            earliest_estimate_arrival_date = (
                parse_date(row["Earliest Estimate Arrival Date"])
                if row["Earliest Estimate Arrival Date"]
                else None
            )

            # Map columns from Google Sheet to Supabase table columns
            data = {
                "email_sent_date": email_sent_date,
                "email_sender": parse_string(row["From"]) if row["From"] else None,
                "order_number": parse_string(process_order_numbers(row["Order Number"]))
                if row["Order Number"]
                else None,
                "order_date": order_date,
                "product_name": parse_string(row["Product name"])
                if row["Product name"]
                else None,
                "size": parse_string(row["Size"])
                if parse_string(row["Size"])
                else "No size",
                "color": parse_string(row["Color"])
                if parse_string(row["Color"])
                else "No color",
                "author": parse_string(row["Author"]) if row["Author"] else None,
                "sku": parse_string(row["SKU"]) if row["SKU"] else None,
                "quantity": safe_int_conversion(row["Quantity"])
                if row["Quantity"]
                else None,
                "total_items": safe_int_conversion(row["Total items"])
                if row["Total items"]
                else None,
                "price": extract_number(parse_string(row["Price"]))
                if row["Price"]
                else None,
                "shipping_address": parse_string(row["Shipping Address"])
                if row["Shipping Address"]
                else None,
                "product_category": parse_string(row["Product category"])
                if row["Product category"]
                else None,
                "tracking_number": parse_string(row["Tracking Number"])
                if row["Tracking Number"]
                else None,
                "estimate_arrival_quantity": safe_int_conversion(
                    row["Estimate Arrival Quantity"]
                )
                if row["Estimate Arrival Quantity"]
                else None,
                "earliest_estimate_arrival_date": earliest_estimate_arrival_date,
                "delivery_status": parse_string(row["Delivery Status"].lower())
                if row["Delivery Status"]
                else None,
                "quantity_received": safe_int_conversion(row["Quantity Received"])
                if row["Quantity Received"]
                else None,
                "link_to_check_status": parse_string(
                    row["Link website to check status"]
                )
                if row["Link website to check status"]
                else None,
                "email_purpose": parse_string(row["Email purpose"])
                if row["Email purpose"]
                else None,
                "shipping_fee": extract_number(parse_string(row["Shipping fee"]))
                if row["Shipping fee"]
                else None,
                "total_order_amount": extract_number(
                    parse_string(row["Total Order Amount"])
                )
                if row["Total Order Amount"]
                else None,
                "total_tax": extract_number(parse_string(row["Total Tax"]))
                if row["Total Tax"]
                else None,
            }

            # Check if the record already exists based on composite primary key
            query = supabase.table("email_tracking_raw").select("*")
            conditions = [
                (
                    "product_name",
                    parse_string(row["Product name"]) if row["Product name"] else None,
                ),
                (
                    "color",
                    parse_string(row["Color"]) if row["Color"] else "No color",
                ),
                (
                    "size",
                    parse_string(row["Size"]) if row["Size"] else "No size",
                ),
                (
                    "order_number",
                    parse_string(process_order_numbers(row["Order Number"]))
                    if row["Order Number"]
                    else None,
                ),
                (
                    "tracking_number",
                    parse_string(row["Tracking Number"])
                    if row["Tracking Number"]
                    else None,
                ),
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
                data.get("email_sent_date"),
            ]
            if any(field is None for field in primary_key_fields):
                print(f"Skipping insertion due to NULL in primary key for data")
                continue  # Skip this iteration and proceed to the next row
            # Perform the insert
            response = supabase.table("email_tracking_raw").insert(data).execute()
            with ThreadPoolExecutor() as executor:
                # Submit each row for processing
                futures = [executor.submit(response, row)]

                # Wait for all futures to complete
                concurrent.futures.wait(futures)
            # Check for an error in the response
            if hasattr(response, "error") and response.error is not None:
                print(f"Error inserting row: {response.error}")
            else:
                print(f"Row inserted: {data}".encode("utf-8", errors="replace"))

        except Exception as e:
            print("Error :", e)
            continue


if __name__ == "__main__":
    main()
    # Close the database connection
    cursor.close()
    conn.close()
