from collections import namedtuple
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
        movie.info.return_value = {
            'production_countries': [{'iso_3166_1': 'US', 'name': 'United States of America'}], 
            'production_companies':[{'id': 79,
            'logo_path': '/tpFpsqbleCzEE2p5EgvUq6ozfCA.png',
            'name': 'Village Roadshow Pictures',
            'origin_country': 'US'}]
            }
        
        result = movies.get_funders(movie)

        for item in result:
            self.assertIsInstance(item, list, 'each result should be a list')
            for list_item in item:
                self.assertIsInstance(list_item, dict, 'list item should be a dict, if not empty')
                for k, v in list_item.items():
                    self.assertIsInstance(k, str, 'each key should be a str value')
                    self.assertIsInstance(v, (int, str), 'key values can be int or str')



if __name__ == '__main__':
    unittest.main()