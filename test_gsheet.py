import gspread
from oauth2client.service_account import ServiceAccountCredentials

def test_google_sheets_connection(creds_json_path, sheet_name):
    try:
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(creds_json_path, scope)
        client = gspread.authorize(creds)
        sheet = client.open(sheet_name).sheet1
        print(f"Successfully connected to Google Sheet: {sheet_name}")
        print(f"First Row: {sheet.row_values(1)}")
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")

if __name__ == "__main__":
    # Replace with your actual path and sheet name
    creds_path = "service_account_credentials.json"
    sheet_name = "Devin's WebScrapper Airtable"
    test_google_sheets_connection(creds_path, sheet_name)
