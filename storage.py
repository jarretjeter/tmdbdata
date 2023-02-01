from azure.storage.blob import BlobServiceClient
import json
import logging
from logging import INFO
import sys
import typer

logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

blobs = typer.Typer()

# Configurations
file = open('./config.json', 'r')
config = json.loads(file.read())
storage_account = config['storage_account']
stor_conn_str = storage_account['conn_str']
container_name = storage_account['container']
container_movie_dir = storage_account['movie_dir']

blob_service_client = BlobServiceClient.from_connection_string(stor_conn_str)



@blobs.command("containers")
def show_containers():
    """
    List all containers in a storage account
    """
    all_containers = blob_service_client.list_containers(include_metadata=True)
    for container in all_containers:
        print(f"Container: {container['name']}")



@blobs.command("upload")
def blobs_upload(region: str, year: str):
    """
    Upload a single file to an Azure blob container
    """
    filename = f"{region}_movie_data_{year}-merged.csv"
    container_sub_dir = f"{container_name}/{container_movie_dir}/{region}_movie_data"
    try:
        blob_client = blob_service_client.get_blob_client(container=container_sub_dir, blob=filename)
        path = f"./data/{region}_movie_data_{year}/{filename}"

        with open(path, "rb") as data:
            logger.info(f"Uploading to Azure Storage as blob: {filename}")
            blob_client.upload_blob(data)
            logger.info(f"Uploaded {filename} successfully")
    except Exception as ex:
        print(f"Exception: \n{ex}")



if __name__ == "__main__":
    blobs()