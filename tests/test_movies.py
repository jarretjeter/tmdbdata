import pandas as pd
from pathlib import Path
import sys
import tmdbsimple as tmdb
import unittest
from unittest.mock import patch, MagicMock

# Having trouble importing movies module. Add the root project directory to Python path to find it.
root_dir = str(Path(__file__).resolve().parent.parent)
sys.path.append(root_dir)

import movies

class TestMovies(unittest.TestCase):

    # Use patch decorator to mock functions using http requests
    @patch('movies.discover.movie')
    def test_list_pages(self, mock_discover_movie):
        # Control the value that the mocked object returns to my function
        mock_discover_movie.return_value = {'total_pages': 6}
        result = movies.list_pages('US', 1914)
        mock_discover_movie.assert_called_with(region='US', primary_release_year=1914, include_adult=False, with_runtime_gte='40')

        self.assertIsInstance(result, list)
        # returned result should match expected value
        self.assertEqual(result, [1, 2, 3, 4, 5, 6])
        for item in result:
            self.assertIsInstance(item, int, 'each result item should be int value')

    @patch('movies.tmdb.Movies')
    def test_get_gen_info(self, mock_tmdb_Movies):
        # mock the object returned from tmdb.Movies()
        movie = MagicMock()
        # set its attribute values
        movie.title = 'The Matrix'
        movie.original_title = 'The Matrix'
        movie.release_date = '1999-03-30'
        movie.original_language = 'en'
        movie.overview = 'Set in the 22nd century, The Matrix tells the story of a computer hacker who joins a group of underground insurgents fighting the vast and powerful computers who now rule the earth.'
        # mocked tmdb.Movies() will return a movie object with the above defined values
        mock_tmdb_Movies.return_value = movie
        result = movies.get_gen_info(tmdb.Movies())
        self.assertIsInstance(result, tuple)
        for item in result:
            self.assertIsInstance(item, str, 'each result item is a str value')

    @patch('movies.tmdb.Movies')
    def test_get_directors(self, mock_tmdb_Movies):
        movie = MagicMock()
        movie.credits.return_value = {'crew': [{
            'gender': 1,
            'id': 9339,
            'known_for_department': 'Writing',
            'name': 'Lilly Wachowski',
            'department': 'Directing',
            'job': 'Director'},
            {
            'gender': 1,
            'id': 9340,
            'known_for_department': 'Writing',
            'name': 'Lana Wachowski',
            'department': 'Directing',
            'job': 'Director'}]}
        result = movies.get_directors(movie)
        
        self.assertIsInstance(result, list, 'result should return a list')
        for item in result:
            self.assertIsInstance(item, dict, 'if not empty, list can only contain dict items')
            for k, v in item.items():
                self.assertIsInstance(k, str, 'dictionary key should be a str')
                self.assertIsInstance(v, (str, int), 'key values can be str or int')

    @patch('movies.tmdb.Movies')
    def test_get_cast(self, mock_tmdb_Movies):
        movie = MagicMock()
        movie.credits.return_value = {'cast': [
            {'id': 6384, 'name': 'Keanu Reeves'}, 
            {'id': 2975, 'name': 'Laurence Fishburne'}, 
            {'id': 530, 'name': 'Carrie-Anne Moss'}
            ]}
        result = movies.get_cast(movie)

        self.assertIsInstance(result, list, 'result should return a list')
        for item in result:
            self.assertIsInstance(item, dict, 'if not empty, list can only contain dict items')
            for k, v in item.items():
                self.assertIsInstance(k, str, 'dictionary key should be a str')
                self.assertIsInstance(v, (str, int), 'key values can be str or int')

    @patch('movies.tmdb.Movies')
    def test_get_funders(self, mock_tmdb_Movies):
        movie = MagicMock()
        movie.production_countries =  [{'iso_3166_1': 'US', 'name': 'United States of America'}]
        movie.production_companies = [{'id': 79,
            'logo_path': '/tpFpsqbleCzEE2p5EgvUq6ozfCA.png',
            'name': 'Village Roadshow Pictures',
            'origin_country': 'US'}]
        
        result = movies.get_funders(movie)
        self.assertIsInstance(result, tuple)
        for item in result:
            self.assertIsInstance(item, list, 'each result should be a list')
            for list_item in item:
                self.assertIsInstance(list_item, dict, 'list item should be a dict, if not empty')
                for k, v in list_item.items():
                    self.assertIsInstance(k, str, 'each key should be a str value')
                    self.assertIsInstance(v, (int, str), 'key values can be int or str')

    @patch('movies.tmdb.Movies')
    def test_get_financials(self, mock_tmdb_Movies):
        movie = MagicMock()
        movie.budget = 63000000
        movie.revenue = 463517383
        result = movies.get_financials(movie)
        self.assertIsInstance(result, dict)
        for k, v in result.items():
            self.assertIsInstance(k, str)
            self.assertIsInstance(v, int)

    @patch('pandas.DataFrame.to_csv')
    def test_output_csv(self, mock_to_csv):
        data_dir = './data'
        region = 'US'
        year = '2077'
        df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        filename = f'{region}_movie_data_{year}-1.csv'
        movies.output_csv(region, year, df, filename)
        test_dir = Path(f"{data_dir}/{region}_movie_data_{year}")
        self.assertTrue(test_dir.is_dir())
        mock_to_csv.assert_called_with(test_dir/filename, index=False)

    @patch('pandas.DataFrame.to_csv')
    def test_merge_dfs(self, mock_to_csv):
        region = 'US'
        year = '1913'
        df = movies.merge_dfs(region, year, missing=None)
        self.assertIsInstance(df, pd.DataFrame)
        


if __name__ == '__main__':
    unittest.main()