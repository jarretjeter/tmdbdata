import glob
import json
import logging
from logging import INFO
import os
import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from storage import blobs_upload
import sys
import tmdbsimple as tmdb
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

logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger: logging.Logger = logging


# create optional upload to blob storage
@app.command("merge_dfs")
def merge_dfs(region: str, year: int, missing_pages=None) -> pd.DataFrame:
    """
    Create and save a merged dataframe from related csv's
    If called from within get_movies(), will only merge if there are no missing pages for the selected year
    """
    if missing_pages == None or missing_pages[year] == []:
        sub_dir = f"./data/{region}_movie_data_{year}"
        try:
            csv_list = [file for file in glob.glob(f'{sub_dir}/*.csv')]
            logger.info(f"Merging {region} movie dataframes for year: {year}")
            df = pd.concat([pd.read_csv(csv) for csv in csv_list])
            df.sort_values(by=['REVENUE'], ascending=False, inplace=True)
            df.drop_duplicates(inplace=True)
            filename = f"{region}_movie_data_{year}-merged.csv"
            logger.info("Saving merged dataframe to csv file")
            df.to_csv(f"{sub_dir}/{filename}", index=False)
            logger.info("Save complete")
            return df
        except ValueError as e:
            logger.info(e)
    else:
        logger.info("Not all pages found, skipping merge")
        return


@app.command("get_movies")
def get_movies(region: str, year_start: int, year_end: int, start_page=1):
    """
    Retrieves data on movies of a specified range of years from the tmdb api
    region: the country to filter by
    year_start: the first year to iterate through
    year_end: the last year to iterate through
    start_page: page of results to retrieve data from. Make sure start_page is within max page range
    """

    data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
    year_range = range(year_start, year_end + 1)
    start_page = int(start_page)
    discover = tmdb.Discover()
    missing_pages = {}
    for year in year_range:
        sub_dir = f"./data/{region}_movie_data_{year}"
        if not os.path.exists(sub_dir): os.mkdir(sub_dir)
        missing_pages[year] = []

        response = discover.movie(region=region, page=start_page, primary_release_year=year, include_adult=False, with_runtime_gte='40')
        logger.info(f"Starting at YEAR: {year}, PAGE: {start_page}/{response['total_pages']}")

        for page in range(start_page, response['total_pages'] + 1):
            try:
                response = discover.movie(region=region, page=page, primary_release_year=year, include_adult=False, with_runtime_gte='40')
                logger.info(f"Year:{year}, Page: {page}")
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

                logger.info(f"Saving current page {page} to csv")
                df = pd.DataFrame(data_dict)
                df.to_csv(f'{sub_dir}/{region}_movie_data_{year}-{page}.csv', index=False)
                data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
            except requests.exceptions.RequestException as e:
                logger.info(e)
                missing_pages[year].append(page)
                logger.info(f"""Failed to reach page: {page}\nPages Missing: {missing_pages}\nTrying next page""")
        merge_dfs(region, year, missing_pages=missing_pages)
        logger.info(f"Missing pages for year {year}: {missing_pages[year]}\nTotal: {missing_pages}")
        blobs_upload(region=region, year=year)
        # Begin iterating through next year of movies, starting at page 1
        start_page = 1
    logger.info(f"Total Pages Missing: {missing_pages}")
    return


if __name__ == "__main__":
    app()