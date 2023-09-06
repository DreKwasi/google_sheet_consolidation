import gspread
from google.oauth2 import service_account
import pandas as pd
import time

def gs_auth(creds_dict):
    credentials = service_account.Credentials.from_service_account_info(creds_dict)
    client = gspread.authorize(credentials)
    return client

def get_all_dfs(client, spreadsheet_name):
    start_time = time.time()
    workbook = client.open(spreadsheet_name)
    
    list_stock_overview = []
    
    for worksheet in workbook.worksheets():
        worksheet_name = worksheet.title
        print(worksheet_name)

        values = worksheet.get_all_values()
        header = values[0]
        data = values[1:]

        df = pd.DataFrame(data, columns=header)
        df["Facility"] = worksheet_name.strip()
        list_stock_overview.append(df)

        time.sleep(1.5)

    workbook_df = pd.concat(list_stock_overview, ignore_index=True)
    end_time = time.time()
    print(f"Elapsed Time: {round(end_time - start_time)}s")
    return workbook_df

if __name__ == "__main__":
    credentials_dict = {
        "type": os.getenv("TYPE"),
        "project_id": os.getenv("PROJECT_ID"),
        "private_key_id": os.getenv("PRIVATE_KEY_ID"),
        "private_key": os.getenv("PRIVATE_KEY").replace("\\n", "\n"),  # Replace escaped newline characters
        "client_email": os.getenv("CLIENT_EMAIL"),
        "client_id": os.getenv("CLIENT_ID"),
        "auth_uri": os.getenv("AUTH_URI"),
        "token_uri": os.getenv("TOKEN_URI"),
        "auth_provider_x509_cert_url": os.getenv("AUTH_PROVIDER_X509_CERT_URL"),
        "client_x509_cert_url": os.getenv("CLIENT_X509_CERT_URL"),
    }

    client = gs_auth(credentials_dict)
    spreadsheet_name = "****"  # Replace with the actual spreadsheet name
    data = get_all_dfs(client, spreadsheet_name)
    data.to_csv("raw_data/stock_df.csv", index=False)
