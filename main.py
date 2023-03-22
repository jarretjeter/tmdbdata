import concurrent.futures
import glob
import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter, Retry
from storage import blob_upload
import sys
import tmdbsimple as tmdb
import time
import typer
app = typer.Typer()

# Configurations
file = open('./config.json', 'r')
config = json.loads(file.read())
tmdb.API_KEY = config['tmdb_api_key']

sess = requests.Session()
retries = Retry(total=5, backoff_factor=0.7, status_forcelist=[500, 503, 504])
tmdb.REQUESTS_SESSION = sess
tmdb.REQUESTS_SESSION.mount("https://", HTTPAdapter(max_retries=retries))
tmdb.REQUESTS_TIMEOUT = (3600)
discover = tmdb.Discover()

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger: logging.Logger = logging


def output_csv(region: str, year: int, df: pd.DataFrame, filename: str):
    """
    Create an output subdirectory and save a csv to it
    """
    data_dir = "./data"
    output_dir = Path(f"{data_dir}/{region}_movie_data_{year}")
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / filename, index=False)

@app.command("merge_dfs")
def merge_dfs(region: str, year: int, missing=None) -> pd.DataFrame:
    """
    Create and save a merged dataframe from related csv's
    If called from within get_movies(), will only merge if there are no missing pages for the selected year
    """
    if missing == None or missing[year] == []:
        sub_dir = f"./data/{region}_movie_data_{year}"
        try:
            csv_list = [file for file in glob.glob(f'{sub_dir}/*.csv')]
            logger.info(f"Merging {region} movie dataframes for YEAR: {year}...")
            df = pd.concat([pd.read_csv(csv) for csv in csv_list])
            df.sort_values(by=['REVENUE'], ascending=False, inplace=True)
            df.drop_duplicates(inplace=True)
            logger.info("Saving merged dataframe to csv file")
            filename = f"{region}_movie_data_{year}-merged.csv"
            output_csv(region=region, year=year, df=df, filename=filename)
            return df
        except ValueError as e:
            logger.info(e)
    else:
        logger.info("All pages not found, cannot merge")
        return    


def retry_missing(region: str, year: int, mssng_pages):
    """
    Attempt to retrieve any missing pages from earlier failed requests
    """
    data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
    logger.info("Attempting retrieval of missing pages...")
    for page in mssng_pages[year][:]:
        try:
            response = discover.movie(region=region, page=page, primary_release_year=year, include_adult=False, with_runtime_gte='40')
            for result in discover.results:
                movie = tmdb.Movies(result['id'])
                id = movie.info()['id']
                data_dict['ID'].append(id)
                title = movie.info()['title']
                data_dict['TITLE'].append(title)
                og_title = movie.info()['original_title']
                data_dict['ORIGINAL_TITLE'].append(og_title)
                release_date = movie.info()['release_date']
                data_dict['RELEASE_DATE'].append(release_date)
                og_lang = movie.info()['original_language']
                data_dict['ORIGINAL_LANGUAGE'].append(og_lang)
                director_list = []
                crew_members = movie.credits()['crew']
                for member in crew_members:
                    if member['job'] == "Director":
                        director_list.append(member['name'])
                data_dict['DIRECTOR'].append(director_list)
                genres = movie.info()['genres']
                genre_list = []
                for genre in genres:
                    genre_list.append(genre['name'])
                data_dict['GENRES'].append(genre_list)
                prod_countries = movie.info()['production_countries']
                prod_countries_list = []
                for country in prod_countries:
                    prod_countries_list.append(country['name'])
                data_dict['PRODUCTION_COUNTRIES'].append(prod_countries_list)
                revenue = movie.info()['revenue']
                data_dict['REVENUE'].append(revenue)

            df = pd.DataFrame(data_dict)
            filename = f"{region}_movie_data_{year}-{page}.csv"
            output_csv(region=region, year=year, df=df, filename=filename)
            logger.info(f"Successful retrieval of: YEAR {year} PAGE {page}")
            mssng_pages[year].remove(page)
            logger.info(mssng_pages)
            data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
        except requests.exceptions.RequestException as e:
            logger.info(e)
            logger.info(f"Failed retrieval of: YEAR {year} PAGE {page}")
    return mssng_pages


@app.command("list_pages")
def list_pages(region: str, year: int):
    """
    Retrieve total pages of discover.movie() search
    """
    response = discover.movie(region=region, primary_release_year=year, include_adult=False, with_runtime_gte='40')
    pages = [page for page in range(1, response['total_pages'] + 1)]
    logger.info(f"YEAR {year}: PAGES {pages}")
    return pages


@app.command("get_page")
def get_page(region: str, year: int, page: int=1, retry: bool=False):
    """
    Obtain metadata for each film on discover.movie() response
    """
    data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
    failed_page = None
    response = discover.movie(region=region, page=page, primary_release_year=year, include_adult=False, with_runtime_gte='40')
    try:
        for result in discover.results:
            movie = tmdb.Movies(result['id'])
            id = movie.info()['id']
            data_dict['ID'].append(id)
            title = movie.info()['title']
            data_dict['TITLE'].append(title)
            og_title = movie.info()['original_title']
            data_dict['ORIGINAL_TITLE'].append(og_title)
            release_date = movie.info()['release_date']
            data_dict['RELEASE_DATE'].append(release_date)
            og_lang = movie.info()['original_language']
            data_dict['ORIGINAL_LANGUAGE'].append(og_lang)
            director_list = []
            crew_members = movie.credits()['crew']
            for member in crew_members:
                if member['job'] == "Director":
                    director_list.append(member['name'])
            data_dict['DIRECTOR'].append(director_list)
            genres = movie.info()['genres']
            genre_list = []
            for genre in genres:
                genre_list.append(genre['name'])
            data_dict['GENRES'].append(genre_list)
            prod_countries = movie.info()['production_countries']
            prod_countries_list = []
            for country in prod_countries:
                prod_countries_list.append(country['name'])
            data_dict['PRODUCTION_COUNTRIES'].append(prod_countries_list)
            revenue = movie.info()['revenue']
            data_dict['REVENUE'].append(revenue)

        logger.info(f"Saving YEAR: {year}, PAGE: {page} to csv")
        df = pd.DataFrame(data_dict)
        filename = f"{region}_movie_data_{year}-{page}.csv"
        output_csv(region=region, year=year, df=df, filename=filename)
        logger.info(f"Retry attempt for YEAR: {year}, Page: {page} successful") if retry else None
        data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
    except requests.exceptions.RequestException as e:
        logger.info(e)
        logger.info(f"Failed to get YEAR: {year}, PAGE: {page}")
        failed_page = page
    return region, year, failed_page


@app.command("run_main")
def main(region: str, year_start: int, year_end: int, upload: bool=True):
    """
    
    """
    tm1 = time.perf_counter()
    year_range = range(year_start, year_end + 1)
    futures = []
    mssng_pages = {}
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for year in year_range:
            pages = list_pages(region, year)
            mssng_pages[year] = []
            for page in pages:
                futures.append(executor.submit(get_page, region, year, page))

        for future in concurrent.futures.as_completed(futures):
            f_year = future.result()[1]
            f_mssng_page = future.result()[2]
            mssng_pages[f_year].append(f_mssng_page) if f_mssng_page != None else None
            logger.info(f"Missing: {mssng_pages}")

        for year in year_range:
            retry_missing(region=region, year=year, mssng_pages=mssng_pages) if mssng_pages[year] != [] else None
            merge_dfs(region, year, missing=mssng_pages)
            blob_upload(region=region, year=year) if upload else None
    tm2 = time.perf_counter()
    print(f"Total time elapsed: {tm2 - tm1:0.2f} seconds")


if __name__ == "__main__":
    app()