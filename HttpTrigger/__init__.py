import logging

import azure.functions as func
import os
import pandas as pd

from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
connection_string = "DefaultEndpointsProtocol=https;AccountName=makropoc;AccountKey=vEdFli8A7lFF/fl5goxmWQ+Bp8Hvf+Eek9zD8aPhHnz6D9XLsh/MCbfpC6Lg5XnLQazOFWgr9PgM25YMhG/WFw==;EndpointSuffix=core.windows.net"
from io import StringIO, BytesIO

# To setup azure-sql-connector
import sqlalchemy as sa
import pyodbc
server = 'makro-sql.database.windows.net'
database = 'zendesk'
username = 'makro-root'
password = '0894750257Ten'
driver= '{ODBC Driver 17 for SQL Server}'

import urllib
params = urllib.parse.quote_plus(
    f"DRIVER={driver};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password}"
)
engine = sa.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
print(engine)

import json
import codecs

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    container = req.params.get('container')
    if container:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client("zendesk")
        blob_list = container_client.list_blobs("comments/")
        blob_name_list = []
        for blob in blob_list:
            blob_client = blob_service_client.get_blob_client(container, blob.name)
            test = [blob_client, container, blob.name]
            file_path = str(blob.name)
            with open(file_path, "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())
            break
        
        # write the DataFrame to a table in the sql database
        object_dict = json.load(codecs.open(file_path, 'r', 'utf-8-sig'))
        object_dict = object_dict['comments']
        object_dict = [{comment['id'], comment['author_id']} for comment in object_dict]
        df = pd.DataFrame(object_dict)
        df.to_sql("comments_azure_function", engine, index=False)

        return func.HttpResponse(f"container {str(object_dict)}!")
    else:
        return func.HttpResponse(
            "Please pass a name on the query string or in the request body",
            status_code=400
        )
