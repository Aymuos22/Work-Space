import logging
import os
import json
import pyodbc
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('HTTP trigger function processed a request.')

    try:
       
        config = req.get_json()
        file_system_name = config.get('file_system_name')
        file_path = config.get('file_path')
        file_type = config.get('file_type')
        database_config = config.get('database_config')

       
        if not file_system_name or not file_path or not file_type or not database_config:
            return func.HttpResponse(
                "Missing one or more required parameters.",
                status_code=400
            )

        
        file_content = download_file_from_adls(file_system_name, file_path)

        
        if file_type.lower() == 'json':
            data = process_json(file_content)
        elif file_type.lower() == 'parquet':
            data = process_parquet(file_content)
        else:
            return func.HttpResponse(
                "Unsupported file type.",
                status_code=400
            )

       
        insert_data_into_database(data, database_config)

        return func.HttpResponse("Data processed and inserted successfully.", status_code=200)

    except Exception as e:
        logging.error(f"Error processing request: {e}")
        return func.HttpResponse(f"Error processing request: {e}", status_code=500)


def download_file_from_adls(file_system_name, file_path):
    account_url = os.environ["ADLS_ACCOUNT_URL"]
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url, credential)

    file_system_client = service_client.get_file_system_client(file_system_name)
    file_client = file_system_client.get_file_client(file_path)

    download = file_client.download_file()
    file_content = download.readall()

    return file_content


def process_json(file_content):
    return json.loads(file_content)


def process_parquet(file_content):
    import io
    buffer = io.BytesIO(file_content)
    return pd.read_parquet(buffer)


def insert_data_into_database(data, database_config):
    conn_str = database_config["connection_string"]
    table_name = database_config["table_name"]

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    columns = ", ".join(data[0].keys())
    values_placeholder = ", ".join(["?"] * len(data[0]))
    insert_stmt = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholder})"

    for record in data:
        values = tuple(record.values())
        cursor.execute(insert_stmt, values)

    conn.commit()
    cursor.close()
    conn.close()

