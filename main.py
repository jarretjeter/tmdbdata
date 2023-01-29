import logging
from logging import INFO
import os
import pandas as pd
import requests
import sys
import tmdbsimple as tmdb
import typer

app = typer.Typer()

API_KEY = os.environ.get("TMDB_API_KEY")
tmdb.API_KEY = API_KEY


logging.basicConfig(format='[%(levelname)-5s][%(asctime)s][%(module)s:%(lineno)04d] : %(message)s',
                    level=INFO,
                    stream=sys.stderr)
logger: logging.Logger = logging


@app.command("get_movies")
def get_movies(region: str, year_start: int, year_end: int):
    """
    
    """
    data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}

    # year_range = range(year_start, year_end + 1) if year_end == int else range(year_start, year_start + 1)
    year_range = range(year_start, year_end + 1)
    batch = 1
    discover = tmdb.Discover()
    # page_num = 1
    for year in year_range:
        # while page_num < 3:
        logger.info(f'current year: {year}')
        response = discover.movie(region=region, page=1, primary_release_year=year, sort_by='primary_release_date_asc', include_adult=False)
        try:
            for page in range(1, response['total_pages'] + 1):
                print("PAGE:", page,'/',response["total_pages"])
                print("TOTAL RESULTS:",response['total_results'])
                response = discover.movie(region=region, page=page, primary_release_year=year, sort_by='primary_release_date_asc', include_adult=False)
                # print(response['results'][0].keys())
                for result in discover.results:
                    if len(data_dict['ID']) < 400:
                        # if len(data_dict['ID']) < 500:

                        movie = tmdb.Movies(result['id'])
                        # print(movie.info().keys())
                        id = movie.info()['id']
                        # print("ID:", id)
                        data_dict['ID'].append(id)
                        title = movie.info()['title']
                        # print("TITLE:", title)
                        data_dict['TITLE'].append(title)
                        og_title = movie.info()['original_title']
                        # print("ORIGINAL_TITLE:", og_title)
                        data_dict['ORIGINAL_TITLE'].append(og_title)
                        release_date = movie.info()['release_date']
                        # print("RELEASE_DATE:", release_date)
                        data_dict['RELEASE_DATE'].append(release_date)
                        og_lang = movie.info()['original_language']
                        # print("ORIGINAL_LANGUAGE:", og_lang)
                        data_dict['ORIGINAL_LANGUAGE'].append(og_lang)
                        director_list = []
                        crew_members = movie.credits()['crew']
                        for member in crew_members:
                            if member['job'] == "Director":
                                # print('NAME:',member['name'])
                                # print(crew)
                                # logger.info('appending director name')
                                director_list.append(member['name'])
                        # logger.info('appending director list')
                        data_dict['DIRECTOR'].append(director_list)
                        genres = movie.info()['genres']
                        # print("GENRES:")
                        genre_list = []
                        for genre in genres:
                            # print(genre['name'])
                            genre_list.append(genre['name'])
                        data_dict['GENRES'].append(genre_list)
                        prod_countries = movie.info()['production_countries']
                        # print("PRODUCTION_COUNTRIES:")
                        prod_countries_list = []
                        for country in prod_countries:
                            # print(country['name'])
                            prod_countries_list.append(country['name'])
                        data_dict['PRODUCTION_COUNTRIES'].append(prod_countries_list)
                        revenue = movie.info()['revenue']
                        # print("REVENUE:", revenue)
                        data_dict['REVENUE'].append(revenue)
                # page_num += 1
                    # return
                    else:
                        logger.info("Saving current batch to csv")
                        df = pd.DataFrame(data_dict)
                        df.to_csv(f'./data/{region}_movie_data{batch}.csv', index=False)

                        data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
                        batch += 1
                logger.info('next page')
        except requests.exceptions.RequestException as e:
            logger.info(e)
            logger.info("Saving current batch to csv")
            df = pd.DataFrame(data_dict)
            df.to_csv(f'./data/{region}_movie_data{batch}.csv', index=False)

            data_dict = {'ID': [], 'TITLE': [], 'ORIGINAL_TITLE': [], 'RELEASE_DATE': [], 'ORIGINAL_LANGUAGE': [], 'DIRECTOR': [], 'GENRES': [], 'PRODUCTION_COUNTRIES': [], 'REVENUE': []}
            batch += 1

    df = pd.DataFrame(data_dict)
    df.to_csv(f'./data/{region}_movie_data{batch}.csv', index=False)


if __name__ == "__main__":
    app()