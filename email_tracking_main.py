import pandas as pd
from supabase import create_client, Client
from fuzzywuzzy import process
import numpy as np
import re
from fuzzywuzzy import fuzz

# Supabase setup
SUPABASE_URL = "https://sxoqzllwkjfluhskqlfl.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InN4b3F6bGx3a2pmbHVoc2txbGZsIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MDIyODE1MTcsImV4cCI6MjAxNzg1NzUxN30.FInynnvuqN8JeonrHa9pTXuQXMp9tE4LO0g5gj0adYE"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def convert_floats_to_ints(df, fill_value=0):
    """
    Converts all columns of type float64 in a DataFrame to int64, handling NaN values.

    Args:
    df (pd.DataFrame): The DataFrame to convert.
    fill_value (int, optional): The value to use for replacing NaN values. Default is 0.

    Returns:
    pd.DataFrame: The DataFrame with float64 columns converted to int64.
    """
    for column in df.select_dtypes(include=["float64"]).columns:
        # Handle NaN values by filling with fill_value
        df[column] = df[column].fillna(fill_value)
        # Check for infinite values and handle them if necessary
        if np.isinf(df[column]).any():
            # You can choose how to handle infinite values here
            df[column] = df[column].replace([np.inf, -np.inf], fill_value)
        # Convert the column to int64
        df[column] = df[column].astype("int64")
    return df


def load_data_from_supabase(table_name):
    query = supabase.table(table_name).select("*")
    response = query.execute()

    # Check for errors in the response
    if hasattr(response, "error") and response.error is not None:
        raise Exception(f"Error fetching data from {table_name}: {response.error}")

    # Convert to DataFrame
    df = pd.DataFrame(response.data)
    # Additional filtering for the email_tracking_raw table
    if table_name == "email_tracking_raw":
        # Fill NA/NaN values with an empty string before filtering
        df["delivery_status"] = df["delivery_status"].fillna("")
        df = df[
            df["delivery_status"].str.lower().str.startswith(("shipped", "delivered"))
        ]
        df = convert_floats_to_ints(df)

    return df


# Load data from your tables
data_warehouse_df = load_data_from_supabase("data_warehouse_raw")
email_tracking_df = load_data_from_supabase("email_tracking_raw")


def concat_columns(row):
    # Initialize product_name
    product_name = row["product_name"]

    # Check and concatenate 'color' if it's not 'No color'
    if row["color"] != "No color":
        product_name += ", " + row["color"]

    # Check and concatenate 'size' if it's not 'No size'
    if row["size"] != "No size":
        product_name += ", " + row["size"]

    return product_name


# Now data_warehouse_df and email_tracking_df are pandas DataFrames containing your data

# Prepare for fuzzy matching (assuming dataframes are loaded into data_warehouse_df and email_tracking_df)
data_warehouse_df["title_normalized"] = data_warehouse_df["title"].str.lower()
email_tracking_df["product_name"] = email_tracking_df.apply(concat_columns, axis=1)
email_tracking_df["product_name_normalized"] = email_tracking_df[
    "product_name"
].str.lower()


# Fuzzy matching
def get_best_match(order_number, title, product_names, threshold=80):
    """
    Find the best match for a given title with a score above the threshold.

    :param order_number: Order number to match.
    :param title: Title to match.
    :param product_names: DataFrame containing product names.
    :param threshold: Minimum acceptable match score (0-100).
    :return: The best match if above the threshold, otherwise None.
    """
    if title:
        # Filter product names by order number
        filtered_product_names = product_names[
            product_names["order_number"] == order_number
        ]["product_name_normalized"]

        # Perform fuzzy matching
        matches = process.extractOne(title, filtered_product_names)
        if matches and matches[1] >= threshold:
            return matches[0]
    return None


# def remove_special_characters(text):
#     """ Remove all special characters from a given text, keeping only alphanumeric characters and spaces. """
#     return re.sub(r'[^A-Za-z0-9 ]+', '', text)

# Fuzzy matching
# def get_best_match(title, threshold=50):
#     """
#     Find the best match for a given title with a score above the threshold.

#     :param title: Title to match.
#     :param threshold: Minimum acceptable match score (0-100).
#     :return: The best match if above the threshold, otherwise None.
#     """
#     if title:
#         # Preprocess the title
#         processed_title = remove_special_characters(title)

#         # Initialize best match and highest score
#         best_match = None
#         highest_score = 0

#         # Iterate through each product name for comparison
#         for product_name in email_tracking_df["product_name_normalized"]:
#             processed_product_name = remove_special_characters(product_name)
#             score = fuzz.ratio(processed_title, processed_product_name)

#             # Update best match if a higher score is found
#             if score > highest_score and score >= threshold:
#                 highest_score = score
#                 best_match = product_name

#         return best_match
#     return None


# Apply fuzzy matching to each row of the DataFrame
data_warehouse_df["best_match_product_name"] = data_warehouse_df.apply(
    lambda row: get_best_match(
        row["order_number"], row["title_normalized"], email_tracking_df
    ),
    axis=1,
)

# Select only the necessary columns from data_warehouse_df for the merge
data_warehouse_subset = data_warehouse_df[
    ["asin", "order_number", "best_match_product_name", "title"]
]


# Perform left join on order_number and tracking_number
result_df = pd.merge(
    data_warehouse_subset,
    email_tracking_df,
    left_on=["order_number", "best_match_product_name"],
    right_on=["order_number", "product_name_normalized"],
    how="right",
)

# Specify the file path for the JSON file
output_file_path = "output_email.json"

# Export the DataFrame to a JSON file
result_df.to_json(output_file_path, orient="records", lines=True, date_format="iso")


# Drop the extra normalized and match columns if not needed
result_df.drop(
    [
        "best_match_product_name",
        "product_name_normalized",
    ],
    axis=1,
    inplace=True,
)

# # Specify the file path for the JSON file
# output_file_path = "output_eamil.json"

# # Export the DataFrame to a JSON file
# result_df.to_json(output_file_path, orient='records', lines=True, date_format='iso')

# print(f"Data exported to {output_file_path}")

# CREATE TABLE target_table (
#     id SERIAL PRIMARY KEY,
#     asin VARCHAR(255),
#     email_sent_date DATE,
#     email_sender VARCHAR(255),
#     order_number VARCHAR(255),
#     order_date DATE,
#     product_name VARCHAR(255),
#     product_category VARCHAR(255),
#     tracking_number VARCHAR(255),
#     estimate_arrival_quantity INTEGER,
#     earliest_estimate_arrival_date DATE,
#     delivery_status VARCHAR(255),
#     quantity_received INTEGER,
#     link_to_check_status TEXT,
#     email_purpose VARCHAR(255)
# );

# Convert the DataFrame to a list of dictionaries, replacing NaN values with None
data_to_insert = [
    {k: (None if pd.isna(v) else v) for k, v in row.items()}
    for _, row in result_df.iterrows()
]

# Delete all existing data in the table
delete_response = (
    supabase.table("result_email_tracking").delete().gte("id", 0).execute()
)  # or any other always-true condition

# Check for an error in the delete response
if hasattr(delete_response, "error") and delete_response.error is not None:
    print(f"Error deleting existing data: {delete_response.error}")

# Insert the data into Supabase
response = supabase.table("result_email_tracking").insert(data_to_insert).execute()

# Check for an error in the response
if hasattr(response, "error") and response.error is not None:
    print(f"Error inserting row: {response.error}")
else:
    print(f"Row inserted: {data_to_insert}".encode("utf-8", errors="replace"))
