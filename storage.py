from ast import literal_eval
from azure.storage.blob import BlobServiceClient
import json
import logging
from logging import INFO
import pandas as pd
import pymysql.cursors
import sys
import typer

logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging

storage = typer.Typer()

file = open('./config.json', 'r')
config = json.loads(file.read())

# Azure Storage Configuarations
storage_account = config['storage_account']
stor_conn_str = storage_account['conn_str']
container_name = storage_account['container']
container_movie_dir = storage_account['movie_dir']

# MySQL Configurations
database = config['database']
user = database['user']
passwd = database['passwd']
db_name = database['db_name']

blob_service_client = BlobServiceClient.from_connection_string(stor_conn_str)



@storage.command("containers")
def show_containers():
    """
    List all containers in a storage account
    """
    all_containers = blob_service_client.list_containers(include_metadata=True)
    for container in all_containers:
        print(f"Container: {container['name']}")



@storage.command("upload")
def blob_upload(region: str, year: str):
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
            blob_client.upload_blob(data, overwrite=True)
            logger.info(f"Uploaded {filename} successfully")
    except Exception as ex:
        print(f"Exception: \n{ex}")


# CREATE FUNCTION TO DELETE BLOBS FROM CLI


def insert_movies(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row into MySQL movies table
    """
    sql = """INSERT INTO `movies` (`id`, `original_title`, `title`, `language`, `release_date`)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY
                    UPDATE original_title=VALUES(original_title), title=VALUES(title), language=VALUES(language), release_date=VALUES(release_date)"""
    cursor.execute(sql, (row.ID, row.ORIGINAL_TITLE, row.TITLE, row.ORIGINAL_LANGUAGE, 
                    row.RELEASE_DATE))


def insert_genres(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row into MySQL genres table
    """
    sql = """INSERT INTO `genres` (`id`, `name`)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY
                    UPDATE name=VALUES(name)"""
    genres_list = row.GENRES
    genres_list = literal_eval(genres_list)
    if len(genres_list) >= 1:
        for genre in genres_list:
            id = genre['id']
            name = genre['name']
            cursor.execute(sql, (id, name))


def insert_directors(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row into MySQL directors table
    """
    sql = """INSERT INTO `directors` (`id`, `name`)
                VALUES (%s, %s)
                ON DUPLICATE KEY
                UPDATE name=VALUES(name)"""
    director_list = row.DIRECTORS
    director_list = literal_eval(director_list)
    if len(director_list) >= 1:
        for director in director_list:
            id = director['id']
            name = director['name']
            cursor.execute(sql, (id, name))


def insert_actors(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row into MySQL actors table
    """
    sql = """INSERT INTO `actors` (`id`, `name`)
                VALUES (%s, %s)
                ON DUPLICATE KEY
                UPDATE name=VALUES(name)"""
    cast_list = row.CAST
    cast_list = literal_eval(cast_list)
    if len(cast_list) >= 1:
        for member in cast_list:
            print(member)
            print(type(member))
            id = member['id']
            name = member['name']
            cursor.execute(sql, (id, name))


def to_mysql(df: pd.DataFrame, year: int):
    """
    Insert Pandas DataFrame rows into a MySQL table.

    Args:
        df: pd.DataFrame
            DataFrame object to use
    Returns: None
    """
    try:
        num_rows = len(df)
        inserted = 0
        for row in df.itertuples(index=False):

            conn = pymysql.connect(host='localhost',
                                user=user,
                                password=passwd,
                                database=db_name,
                                cursorclass=pymysql.cursors.DictCursor)
            with conn:
                with conn.cursor() as cursor:
                    insert_movies(row=row, cursor=cursor)
                    insert_genres(row=row, cursor=cursor)
                    insert_directors(row=row, cursor=cursor)
                    insert_actors(row=row, cursor=cursor)
                conn.commit()
                inserted += 1
        # Insertions per table
        logger.info(f"Table insertions for {year} complete. {inserted}/{num_rows} rows inserted.")
    
    except pymysql.Error as e:
        logger.info(e)
        conn.rollback()



if __name__ == "__main__":
    storage()