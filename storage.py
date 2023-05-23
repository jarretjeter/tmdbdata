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
def show_containers() -> str:
    """
    List all containers in a storage account
    """
    all_containers = blob_service_client.list_containers(include_metadata=True)
    for container in all_containers:
        print(f"Container: {container['name']}")


@storage.command("upload")
def blob_upload(region: str, year: str) -> None:
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
    except Exception as e:
        print(e)


def insert_movies(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL movies table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `movies` (`id`, `original_title`, `title`, `language`, `release_date`)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY
                    UPDATE original_title=VALUES(original_title), title=VALUES(title), language=VALUES(language), release_date=VALUES(release_date)"""
    cursor.execute(sql, (row.ID, row.ORIGINAL_TITLE, row.TITLE, 
                        row.ORIGINAL_LANGUAGE, row.RELEASE_DATE))


def insert_plots(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL plots table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT into `plots` (`plot`, `movie_id`)
            VALUES(%s, %s)
            ON DUPLICATE KEY
            UPDATE plot=VALUES(plot)"""
    cursor.execute(sql, (row.PLOT, row.ID))


def insert_genres(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL genres table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `genres` (`id`, `name`)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY
                    UPDATE name=VALUES(name)"""
    genres_list = row.GENRES
    # Change from str back to list[dict]
    genres_list = literal_eval(genres_list)
    if len(genres_list) >= 1:
        for genre in genres_list:
            id = genre['id']
            name = genre['name']
            cursor.execute(sql, (id, name))


def insert_movie_genres(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL movie_genres table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `movie_genres` (`movie_id`, `genre_id`)
            VALUES (%s, %s)
            ON DUPLICATE KEY
            UPDATE movie_id=VALUES(movie_id)"""
    genres_list = row.GENRES
    genres_list = literal_eval(genres_list)
    movie_id = row.ID
    if len(genres_list) >= 1:
        for genre in genres_list:
            genre_id = genre['id']
            cursor.execute(sql, (movie_id, genre_id))


def insert_directors(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL directors table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
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


def insert_movie_directors(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL the movie_directors table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `movie_directors` (`movie_id`, `director_id`)
                VALUES (%s, %s)
                ON DUPLICATE KEY
                UPDATE movie_id=VALUES(movie_id)"""
    director_list = row.DIRECTORS
    director_list = literal_eval(director_list)
    movie_id = row.ID
    if len(director_list) >= 1:
        for director in director_list:
            id = director['id']
            cursor.execute(sql, (movie_id, id))


def insert_actors(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL actors table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `actors` (`id`, `name`)
                VALUES (%s, %s)
                ON DUPLICATE KEY
                UPDATE name=VALUES(name)"""
    cast_list = row.CAST
    cast_list = literal_eval(cast_list)
    if len(cast_list) >= 1:
        for member in cast_list:
            id = member['id']
            name = member['name']
            cursor.execute(sql, (id, name))


def insert_movie_actors(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL the movie_actors table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `movie_actors` (`movie_id`, `actor_id`)
                VALUES (%s, %s)
                ON DUPLICATE KEY
                UPDATE movie_id=VALUES(movie_id)"""
    cast_list = row.CAST
    cast_list = literal_eval(cast_list)
    movie_id = row.ID
    if len(cast_list) >= 1:
        for actor in cast_list:
            actor_id = actor['id']
            cursor.execute(sql, (movie_id, actor_id))


def insert_countries(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL countries table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `countries` (`id`, `name`)
                VALUES (%s, %s)
                ON DUPLICATE KEY
                UPDATE name=VALUES(name)"""
    country_list = row.PRODUCTION_COUNTRIES
    country_list = literal_eval(country_list)
    if len(country_list) >= 1:
        for country in country_list:
            id = country['iso_3166_1']
            name = country['name']
            cursor.execute(sql, (id, name))


def insert_companies(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL companies table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    company_list = row.PRODUCTION_COMPANIES
    company_list = literal_eval(company_list)
    if len(company_list) >= 1:
        for company in company_list:
            id = company['id']
            name = company['name']
            country_id = company['origin_country']

            if country_id != 'no info':
                sql = """INSERT INTO `companies` (`id`, `name`, `country`)
                    VALUES(%s, %s, %s)
                    ON DUPLICATE KEY
                    UPDATE name=VALUES(name), country=VALUES(country)"""
                cursor.execute(sql, (id, name, country_id))
            else:
                sql = """INSERT INTO `companies` (`id`, `name`)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY
                    UPDATE name=VALUES(name)"""
                cursor.execute(sql, (id, name))


def insert_movie_revenue(row, cursor: pymysql.cursors.DictCursor) -> None:
    """
    insert pd.DataFrame row values into MySQL movie_revenue table

    Args:
        row: pd.DataFrame row
            Tuple to access
        cursor: PyMySQL DictCursor object
            executes SQL statement
    Returns: None
    """
    sql = """INSERT INTO `movie_revenue` (`movie_id`, `revenue`, `budget`)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY
            UPDATE revenue=VALUES(revenue)"""
    movie_id = row.ID
    fin_dict = row.FINANCIAL
    revenue = fin_dict['revenue']
    budget = fin_dict['budget']
    cursor.execute(sql, (movie_id, revenue, budget))


def to_mysql(df: pd.DataFrame, year: int) -> None:
    """
    Insert Pandas DataFrame rows into a MySQL table.

    Args:
        df: pd.DataFrame
            DataFrame object to iterate over
        year: int
            Passed to the function from main(). Simply logs the year back after completion.
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
                    insert_plots(row=row, cursor=cursor)
                    insert_genres(row=row, cursor=cursor)
                    insert_movie_genres(row=row, cursor=cursor)
                    insert_directors(row=row, cursor=cursor)
                    insert_movie_directors(row=row, cursor=cursor)
                    insert_actors(row=row, cursor=cursor)
                    insert_movie_actors(row=row, cursor=cursor)
                    insert_countries(row=row, cursor=cursor)
                    insert_companies(row=row, cursor=cursor)
                    insert_movie_revenue(row=row, cursor=cursor)

                conn.commit()
                inserted += cursor.rowcount
        # Insertions per table
        logger.info(f"Table insertions for {year} complete. {inserted}/{num_rows} rows inserted.")
    
    except pymysql.Error as e:
        logger.info(e)
        conn.rollback()



if __name__ == "__main__":
    storage()