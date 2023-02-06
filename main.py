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

logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging



### WRITE CODE TO CHECK IF ANY PAGES ARE MISSING
@app.command("merge_dfs")
def merge_dfs(region: str, year: int, page=None, total_pages=None) -> pd.DataFrame:
    """
    Create and save a merged dataframe from related csv's
    If called from within get_movies(), will only merge if it finds the last page number/csv
    """
    if page == total_pages:
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
        logger.info("Could not find last page. Skipping..")
        return



@app.command("get_movies")
def get_movies(region: str, year_start: int, year_end: int, start_page=1):
    """
    Retrieves data on movies of a specified range of years from the tmdb api
    region: the country to filter by
    year_start: the first year to iterate through
    year_end: the last year to iterate through
    start_page: page of results to retrieve data from
    """

    data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
    year_range = range(year_start, year_end + 1)
    discover = tmdb.Discover()
    logger.info("start get_movies")
    # MAKE SURE PAGE NUM IS WITHIN MAX PAGE RANGE
    start_page = 25
    for year in year_range:
        sub_dir = f"./data/{region}_movie_data_{year}"
        if not os.path.exists(sub_dir): os.mkdir(sub_dir)
        response = discover.movie(region=region, page=start_page, primary_release_year=year, include_adult=False, with_runtime_gte='40')

        try:
            for page in range(start_page, response['total_pages'] + 1):
                logger.info(f"YEAR: {year}, PAGE: {page} / {response['total_pages']}")
                logger.info(f"TOTAL RESULTS: {response['total_results']}")
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

                logger.info(f"Saving current page {page} to csv")
                df = pd.DataFrame(data_dict)
                df.to_csv(f'{sub_dir}/{region}_movie_data_{year}-{page}.csv', index=False)
                logger.info('saved')
                data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
        except requests.exceptions.RequestException as e:
            logger.info(e)
            start_page = 1
        print(start_page)
        print(page)
        print(response['total_pages'])
        merge_dfs(region, year, page=page, total_pages=response['total_pages'])
        blobs_upload(region=region, year=year)
        start_page = 1
    return


if __name__ == "__main__":
    app()