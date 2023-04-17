from ast import literal_eval
import json
import logging
from logging import INFO
import pandas as pd
from pathlib import Path
import requests
from requests.adapters import HTTPAdapter, Retry
import sys
import tmdbsimple as tmdb
import typer

movies = typer.Typer(no_args_is_help=True)

# tmdb configuration
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


def output_csv(region: str, year: int, df: pd.DataFrame, filename: str) -> None:
    """
    Create an output subdirectory and save a csv to it

    Args:
        region: str
            The country that the subdirectory will have included in its name
        year: int
            The year that the subdirectory will have included in its name
        df: pd.Dataframe
            dataframe object to save
        filename: int
            name of the output csv file
    Returns: None
    """
    data_dir = "./data"
    output_dir = Path(f"{data_dir}/{region}_movie_data_{year}")
    output_dir.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_dir / filename, index=False)


@movies.command("merge_dfs")
def merge_dfs(region: str, year: int, missing=None) -> pd.DataFrame:
    """
    Create and save a merged dataframe from related csv's
    If called from within main(), will only merge if there are no missing pages for the selected year

    Args:
        region: str
            Country that will be included in the csv filename
        year: int
            Year that will be included in the csv filename
        missing: dict, default None
            (Optional) The dictionary to check for any pages missing
    Returns: pd.DataFrame
    """
    if missing == None or missing[year] == []:
        sub_dir = Path(f"./data/{region}_movie_data_{year}")
        try:
            # IGNORE MERGED CSV's, IF ANY
            csv_list = [file for file in sub_dir.glob("**/*") if 'merged' not in file.name]
            logger.info(f"Merging {region} movie dataframes for YEAR: {year}")
            df = pd.concat([pd.read_csv(csv) for csv in csv_list])
            df.drop_duplicates(inplace=True)
            # Reading from csv converts dict types to str.. Convert back to dict to sort by values.
            df['FINANCIAL'] = df['FINANCIAL'].apply(literal_eval)
            df.sort_values(by=['FINANCIAL'], key=lambda k: k.str['revenue'], ascending=False, inplace=True)
            logger.info("Saving merged dataframe to csv file")
            filename = f"{region}_movie_data_{year}-merged.csv"
            output_csv(region=region, year=year, df=df, filename=filename)
            return df
        except ValueError as e:
            logger.info(e)
    else:
        logger.info("All pages not found, cannot merge")
        return    


def retry_missing(region: str, year: int, mssng_pages, output=True) -> dict:
    """
    Attempt to retrieve any missing pages from earlier failed requests

    Args:
        region: str
            Country to filter by
        year: int
            Year to filter by
        mssng_pages: dict
            Dictionary consisting of key-value pairs of {year: [list of page numbers missing]}
    Returns: dict[int, list[int]] of any pages still missing
    """
    data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'PLOT': [], 'DIRECTORS': [], 'CAST': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'PRODUCTION_COMPANIES': [], 'FINANCIAL': []}
    logger.info("Attempting retrieval of missing pages...")
    for page in mssng_pages[year][:]:
        try:
            response = discover.movie(region=region, page=page, primary_release_year=year, include_adult=False, with_runtime_gte='40')
            for result in discover.results:
                movie = tmdb.Movies(result['id'])
                data_dict['ID'].append(result['id'])
                gen_info = get_gen_info(movie=movie)
                data_dict['TITLE'].append(gen_info[0])
                data_dict['ORIGINAL_TITLE'].append(gen_info[1])
                data_dict['RELEASE_DATE'].append(gen_info[2])
                data_dict['ORIGINAL_LANGUAGE'].append(gen_info[3])
                data_dict['PLOT'].append(gen_info[4])
                data_dict['DIRECTORS'].append(get_directors(movie=movie))
                data_dict['CAST'].append(get_cast(movie=movie))
                data_dict['GENRES'].append(movie.info()['genres'])
                funders = get_funders(movie=movie)
                data_dict['PRODUCTION_COUNTRIES'].append(funders[0])
                data_dict['PRODUCTION_COMPANIES'].append(funders[1])
                data_dict['FINANCIAL'].append(get_financials(movie=movie))


            df = pd.DataFrame(data_dict)
            df.sort_values(by=['FINANCIAL'], key=lambda k: k.str['revenue'], ascending=False, inplace=True)
            filename = f"{region}_movie_data_{year}-{page}.csv"
            if output:
                output_csv(region=region, year=year, df=df, filename=filename)
                logger.info(f"Successful retrieval of: YEAR {year} PAGE {page}")
            mssng_pages[year].remove(page)
            logger.info(mssng_pages)
            data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'PLOT': [], 'DIRECTORS': [], 'CAST': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'PRODUCTION_COMPANIES': [], 'FINANCIAL': []}
        except requests.exceptions.RequestException as e:
            logger.info(e)
            logger.info(f"Failed retrieval of: YEAR {year} PAGE {page}")
    return mssng_pages


@movies.command("list_pages")
def list_pages(region: str, year: int) -> list:
    """
    Retrieve total pages of discover.movie() search

    Args:
        region: str
            Country to filter by
        year: int
            Year to filter by
    Returns: a list[int] of all pages returned from search response
    """
    response = discover.movie(region=region, primary_release_year=year, include_adult=False, with_runtime_gte='40')
    pages = [page for page in range(1, response['total_pages'] + 1)]
    logger.info(f"YEAR {year}: PAGES {pages}")
    return pages


def get_gen_info(movie: tmdb.Movies) -> tuple:
    """
    Obtain some general information for a movie

    Args:
        movies: tmdb.Movies object
            Object containing the required data to extract
    Returns: tuple[str, str, str, str, str]
    """
    title = movie.info()['title']
    og_title = movie.info()['original_title']
    release_date = movie.info()['release_date']
    og_lang = movie.info()['original_language']
    plot = movie.info()['overview']
    if plot == "":
        plot = "No info."
    plot = plot.replace("\n", "").replace("\r", "").replace("\t", " ")

    return title, og_title, release_date, og_lang, plot


def get_directors(movie: tmdb.Movies) -> list:
    """
    Get a movie's directors

    Args:
        movies: tmdb.Movies object
            Object containing the required data to extract
    Returns: list[dict]
    """
    crew_list = movie.credits()['crew']
    director_list = []
    if crew_list != []:
        for member in crew_list:
            if member['job'] == 'Director':
                director_dict = {}
                director_dict['id'] = member['id']
                director_dict['name'] = member['name']
                director_list.append(director_dict) if director_dict != {} else None
        return director_list
    else:
        return crew_list


def get_cast(movie: tmdb.Movies) -> list:
    """
    Get a movie's cast members

    Args:
        movies: tmdb.Movies object
            Object containing the required data to extract
    Returns: list[dict]
    """
    cast_list = movie.credits()['cast']
    # New list will contain a dict of only the 'id' and 'name' key-value pairs, everything else is unnecessary
    new_cast_list = []
    if cast_list != []:
        for member in cast_list:
            cast_dict = {}
            cast_dict['id'] = member['id']
            cast_dict['name'] = member['name']
            new_cast_list.append(cast_dict) if cast_dict != {} else None
        return new_cast_list
    else:
        return cast_list


def get_funders(movie: tmdb.Movies) -> tuple:
    """
    Get the companies that helped produce a movie

    Args:
        movies: tmdb.Movies object
            Object containing the required data to extract
    Returns: tuple[list, list]
    """
    country_list = movie.info()['production_countries']
    company_list = movie.info()['production_companies']
    if company_list != []:
        for company in company_list:
            company.pop('logo_path')
            for k in company:
                if company[k] == '':
                    company[k] = 'no info'    
    return country_list, company_list


def get_financials(movie: tmdb.Movies) -> dict:
    """
    Get a movie's budget and revenue

    Args:
        movies: tmdb.Movies object
            Object containing the required data to extract
    Returns: dict[str, int]
    """
    finance_dict = {}
    finance_dict['budget'] = movie.info()['budget']
    finance_dict['revenue'] = movie.info()['revenue']
    return finance_dict


@movies.command("get_data")
def get_data(region: str, year: int, page: int=1, output=True) -> tuple:
    """
    Obtain metadata for each film returned from discover.movie() response

    Args:
        region: str
            Country to filter by
        year: int
            Year to filter by
        page: int
            Page number to send a request to
    Returns: tuple[str, int, int, pd.DataFrame]
    """
    data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'PLOT': [], 'DIRECTORS': [], 'CAST': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'PRODUCTION_COMPANIES': [], 'FINANCIAL': []}
    failed_page = None
    response = discover.movie(region=region, page=page, primary_release_year=year, include_adult=False, with_runtime_gte='40')
    try:
        for result in discover.results:
            movie = tmdb.Movies(result['id'])
            data_dict['ID'].append(result['id'])
            gen_info = get_gen_info(movie=movie)
            data_dict['TITLE'].append(gen_info[0])
            data_dict['ORIGINAL_TITLE'].append(gen_info[1])
            data_dict['RELEASE_DATE'].append(gen_info[2])
            data_dict['ORIGINAL_LANGUAGE'].append(gen_info[3])
            data_dict['PLOT'].append(gen_info[4])
            data_dict['DIRECTORS'].append(get_directors(movie=movie))
            data_dict['CAST'].append(get_cast(movie=movie))
            data_dict['GENRES'].append(movie.info()['genres'])
            funders = get_funders(movie=movie)
            data_dict['PRODUCTION_COUNTRIES'].append(funders[0])
            data_dict['PRODUCTION_COMPANIES'].append(funders[1])
            data_dict['FINANCIAL'].append(get_financials(movie=movie))

        df = pd.DataFrame(data_dict)
        df.sort_values(by=['FINANCIAL'], key=lambda k: k.str['revenue'], ascending=False, inplace=True)
        filename = f"{region}_movie_data_{year}-{page}.csv"
        if output:
            logger.info(f"Saving YEAR: {year}, PAGE: {page} to csv")
            output_csv(region=region, year=year, df=df, filename=filename)
    except requests.exceptions.RequestException as e:
        logger.info(e)
        logger.info(f"Failed to get YEAR: {year}, PAGE: {page}")
        failed_page = page
    return region, year, failed_page, df



if __name__ == "__main__":
    movies()