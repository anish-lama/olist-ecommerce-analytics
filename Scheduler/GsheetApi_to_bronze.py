import pandas as pd, gspread, psycopg2
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SERVICE_ACCOUNT_FILE = "olistdataprj-0ffaea892c96.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

credentials = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)

sheets_service = build("sheets", "v4", credentials = credentials)

SPREADSHEETS = {
    "customers": ("1fZAEtNiP_PGYAmmkLSIFmv84A517Y62xn-veGButtrA", "olist_customers_dataset"),
    "orders": ("1Dfvz_4FA-zyB9u871WKEdgX7gIxEQ08k7iPAE2smn7k", "olist_orders_dataset"),
    "order_payments": ("1YPIhFsbratMeYKtghH5AHwDsE1BkMCnWNUFonkpElXI", "olist_order_payments_dataset"),
    "reviews": ("1ZpJnx1U_FGw9pWfBm3uojPTot_ZqwKki-YaKBMbO3WY", "olist_order_reviews_dataset"),
    "product_category_name_translation" : ("1ieJZUR_ZC4cf8ko_03MKM0iEeaNSPLl9griCOIEn5L4", "product_category_name_translation"),
    "products": ("1poFhv65zLAm-5z609uA_nf7kBa557pG3n9Bxh8-0KRw", "olist_products_dataset"),
    "sellers": ("1zpGQtkMSMYEaIXs96tCVtvnzJLKuvzrA5WsQBSt7IQI", "olist_sellers_dataset"),
    "order_items": ("1UbMlxfz0rESUilcRnIjS-9Lw7iQeml1ZpUj59wGuUbI", "olist_order_items_dataset"),
    "geolocation": ("1yagtv1sF0CeoTCq59wtrGAbNJrtOt_EXAoSh4r8cRqs", "olist_geolocation_dataset")
}


conn  = psycopg2.connect(
    host="localhost",
    database = "OlistEcommerce",
    user = "postgres",
    password = "9841"
)

cursor = conn.cursor()

def load_sheet_to_postgres(table_name, spreadsheet_id, tab_name):
   
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId = spreadsheet_id,
        range=f"{tab_name}!A:Z"
    ).execute()

    rows = result.get("values", [])

    if not rows:
        print(f"No data found for {table_name}")
        return
    
    
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = df.replace(r'^\s*$', None, regex=True)

    df = df.where(pd.notna(df), None)


    cursor.execute(f"TRUNCATE TABLE bronze.{table_name};")

    cols = ", ".join(df.columns)
    placeholders = ", ".join(["%s"] * len(df.columns))

    insert_query = f"INSERT INTO bronze.{table_name} ({cols}) VALUES ({placeholders})"

    for row in df.itertuples(index=False, name=None):
        cursor.execute(insert_query, row)
    
    conn.commit()
    print(f"Loaded {len(df)} rows into {table_name}")


for table, (sheet_id, tab) in SPREADSHEETS.items():
    print(f"Syncing: {table}")
    load_sheet_to_postgres(table, sheet_id, tab)

cursor.close()
conn.close()

print("Sync completed!")