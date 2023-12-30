import psycopg2
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from supabase import create_client, Client
from google.oauth2.service_account import Credentials
import data_email_raw
import data_warehouse_raw
import email_tracking_main

# Supabase Connection Details
SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Google Sheets Credentials
SERVICE_ACCOUNT_FILE = "keygoogle/email-parsing-406909-231994f0fedc.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SPREADSHEET_ID = "1O7A06oyw5uW-ysw7R04FzohQ2506-G6iSEQpR4p-dFo"
RANGE_NAME = "A4:AJ"  # e.g., "Sheet1!A1:E"

# Setup Google Sheets connection
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet("DATA-kho-analytics")

# Read data from Google Sheets
google_sheets_data = sheet.get_values(RANGE_NAME)

# Extract headers and data
headers = google_sheets_data[0]  # Assuming the first row contains headers
data_rows = google_sheets_data[1:]  # All other rows are data

# Supabase query execution
supabase_query = """ SELECT
    concat(asin,order_number) code_id,
    asin,
    title,
    order_number,
    price price_update,
    MAX(delivery_status) AS delivery_status,
    MAX(CASE WHEN rn = 1 THEN tracking_number END) AS "Tracking Number 1",
    MAX(CASE WHEN rn = 2 THEN tracking_number END) AS "Tracking Number 2",
    MAX(CASE WHEN rn = 3 THEN tracking_number END) AS "Tracking Number 3",
    MAX(CASE WHEN rn = 4 THEN tracking_number END) AS "Tracking Number 4",
    MAX(CASE WHEN rn = 5 THEN tracking_number END) AS "Tracking Number 5",
    MAX(CASE WHEN rn = 1 THEN coalesce(quantity,coalesce(quantity_received,estimate_arrival_quantity)) END) AS "Qty Receive 1",
    MAX(CASE WHEN rn = 2 THEN coalesce(quantity,coalesce(quantity_received,estimate_arrival_quantity)) END) AS "Qty Receive 2",
    MAX(CASE WHEN rn = 3 THEN coalesce(quantity,coalesce(quantity_received,estimate_arrival_quantity)) END) AS "Qty Receive 3",
    MAX(CASE WHEN rn = 4 THEN coalesce(quantity,coalesce(quantity_received,estimate_arrival_quantity)) END) AS "Qty Receive 4",
    MAX(CASE WHEN rn = 5 THEN coalesce(quantity,coalesce(quantity_received,estimate_arrival_quantity)) END) AS "Qty Receive 5",
    case when lower(shipping_address) like '%3715%' then 'Mua'
        when lower(shipping_address) like '%6531%' then 'Dinh'
        when lower(shipping_address) like '%5301%' then 'Binh'
    end shipping_address_mod,
    shipping_fee,
    total_order_amount,
    total_tax
FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY asin,order_number, title ORDER BY tracking_number) AS rn
    FROM
        result_email_tracking
    where asin is not null
) AS subquery
GROUP BY
    asin,
    title,
    order_number,
    price,
    shipping_address,
    shipping_fee,
    total_order_amount,
    total_tax
ORDER BY
    asin,
    title,
    order_number;
 """
# data = supabase.table("result_email_tracking").execute_sql(supabase_query).data  # Modify as per your query and table

# Supabase PostgreSQL Connection Details
db_host = (
    "db.sxoqzllwkjfluhskqlfl.supabase.co"  # Replace with your actual database host
)
db_name = "postgres"  # Replace with your actual database name
db_user = "postgres"  # Replace with your actual database user
db_password = "5giE*5Y5Uexi3P2"  # Replace with your actual database password

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host=db_host, dbname=db_name, user=db_user, password=db_password
)

# Create a cursor object
cur = conn.cursor()

# Execute your query
# supabase_query = """ [Your SQL Query Here] """
cur.execute(supabase_query)

# Fetch the data
data = cur.fetchall()

# Close the cursor and connection
cur.close()
conn.close()


# Define column indexes for tracking numbers and quantities in Google Sheets
tracking_number_columns_start_index = (
    20  # Assuming 'Tracking Number 1' starts at column 22
)
quantity_columns_start_index = 30  # Assuming 'Qty Receive 1' starts at column 31
price_update_column_index = 35  # Assuming 'price_update' is at column 36
status_update_column_index = 19
shipping_address_update_column_index = 36
shipping_fee_update_column_index = 37
total_amount_update_column_index = 38
total_tax_update_column_index = 39


# Convert data to a list of dictionaries for easier processing
google_sheets_data_dicts = []
for row in data_rows:
    row_dict = dict(zip(headers, row))
    google_sheets_data_dicts.append(row_dict)


# Function to find row index by 'Code Ordernumber'
def find_row_index_by_code(code, sheet_data):
    for index, row in enumerate(
        sheet_data, start=2
    ):  # start=2 assuming headers are in the first row
        if row.get("Code Ordernumber") == code:
            return index
    return None


def create_google_sheets_row(supabase_row):
    # This function will convert a row from Supabase to the format required by Google Sheets
    # Adjust the indices and structure as per your requirement
    return [
        supabase_row[0],  # code_id
        "",
        "",
        supabase_row[1],  # asin
        supabase_row[2],  # title
        "",
        "",
        supabase_row[3],  # order_number
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        supabase_row[5],  # delivery_status
        supabase_row[6],  # tracking_number 1-5
        supabase_row[7],
        supabase_row[8],
        supabase_row[9],
        supabase_row[10],
        "",
        "",
        "",
        "",
        "",
        supabase_row[11],
        supabase_row[12],
        supabase_row[13],
        supabase_row[14],
        supabase_row[15],
        supabase_row[4],
        supabase_row[16],
        supabase_row[17],
        supabase_row[18],
        supabase_row[19],
        # Add other columns as needed, based on how they map to your Google Sheets
    ]


# Update Google Sheets based on Supabase data
# for row in data:
#     code_ordernumber = row[
#         0
#     ]  # Assuming the 'Code Ordernumber' is the first column in your query
#     row_index = find_row_index_by_code(code_ordernumber, google_sheets_data_dicts)
#     if row_index:
#         # Find the row in Google Sheets
#         for i, gs_row in enumerate(
#             google_sheets_data_dicts, start=2
#         ):  # start=2 assumes the first row is headers
#             if gs_row["Code Ordernumber"] == code_ordernumber:
#                 # Update tracking numbers
#                 for j in range(10):  # Assuming there are up to 10 tracking numbers
#                     tracking_number = row[
#                         j + 6
#                     ]  # Adjust the index based on your query structure
#                     if tracking_number and not gs_row[f"Tracking Number {j+1}"]:
#                         sheet.update_cell(
#                             i, tracking_number_columns_start_index + j, tracking_number
#                         )

#                 # Update quantities
#                 for j in range(5):  # Assuming there are up to 5 quantities
#                     quantity = row[
#                         j + 11
#                     ]  # Adjust the index based on your query structure
#                     if (
#                         quantity is not None
#                         and quantity != gs_row[f"Qty Receive {j+1}"]
#                     ):
#                         sheet.update_cell(i, quantity_columns_start_index + j, quantity)

#                 # Update price
#                 price = row[4]  # Adjust the index based on your query structure
#                 if price is not None and price != gs_row["price_update"]:
#                     sheet.update_cell(i, price_update_column_index, price)
#     else:
#         # If the code_id is not found, append a new row
#         new_row = create_google_sheets_row(row)
#         sheet.append_row(new_row)

# Initialize a list for batch updates
batch_updates = []

# Iterate over Supabase data and prepare batch updates
for row in data:
    code_ordernumber = row[0]  # Assuming the 'Code Ordernumber' is the first column
    row_index = find_row_index_by_code(code_ordernumber, google_sheets_data_dicts)

    if row_index:
        # Update tracking numbers and quantities in batch
        for j in range(4):  # Adjust range based on your data
            tracking_number = row[j + 6]
            if tracking_number:
                cell_address = gspread.utils.rowcol_to_a1(
                    row_index, tracking_number_columns_start_index + j
                )
                batch_updates.append(
                    {"range": f"{cell_address}", "values": [[tracking_number]]}
                )

        for j in range(4):  # Adjust range based on your data
            quantity = row[j + 11]
            if quantity is not None:
                cell_address = gspread.utils.rowcol_to_a1(
                    row_index, quantity_columns_start_index + j
                )
                batch_updates.append(
                    {"range": f"{cell_address}", "values": [[quantity]]}
                )

        delivery_status = row[5]
        if delivery_status is not None:
            cell_address = gspread.utils.rowcol_to_a1(
                row_index, status_update_column_index
            )
            batch_updates.append({"range": f"{cell_address}", "values": [[delivery_status]]})

        # Update price
        price = row[4]
        if price is not None:
            cell_address = gspread.utils.rowcol_to_a1(
                row_index, price_update_column_index
            )
            batch_updates.append({"range": f"{cell_address}", "values": [[price]]})
        shipping_add = row[16]
        if shipping_add is not None:
            cell_address = gspread.utils.rowcol_to_a1(
                row_index, shipping_address_update_column_index
            )
            batch_updates.append(
                {"range": f"{cell_address}", "values": [[shipping_add]]}
            )
        shipping_fee = row[17]
        if shipping_fee is not None:
            cell_address = gspread.utils.rowcol_to_a1(
                row_index, shipping_fee_update_column_index
            )
            batch_updates.append(
                {"range": f"{cell_address}", "values": [[shipping_fee]]}
            )
        total_amount = row[18]
        if total_amount is not None:
            cell_address = gspread.utils.rowcol_to_a1(
                row_index, total_amount_update_column_index
            )
            batch_updates.append(
                {"range": f"{cell_address}", "values": [[total_amount]]}
            )
        total_tax = row[19]
        if total_tax is not None:
            cell_address = gspread.utils.rowcol_to_a1(
                row_index, total_tax_update_column_index
            )
            batch_updates.append({"range": f"{cell_address}", "values": [[total_tax]]})
    else:
        # If the code_id is not found, append a new row
        new_row = create_google_sheets_row(row)
        sheet.append_row(new_row)

# Send all updates in a single batch
if batch_updates:
    sheet.batch_update(batch_updates)

# Done!
