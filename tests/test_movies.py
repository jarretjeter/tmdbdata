from pathlib import Path
import sys
import tmdbsimple as tmdb
import unittest
from unittest.mock import patch

# Having trouble importing movies module. Add the root project directory to Python path to find it.
root_dir = str(Path(__file__).resolve().parent.parent)
sys.path.append(root_dir)

import movies

class TestMovies(unittest.TestCase):

    @patch('movies.list_pages')
    def test_list_pages(self, mock_list_pages):
        mock_list_pages.return_value = [1, 2, 3]
        result = movies.list_pages()
        self.assertIsInstance(result, list)
        for item in result:
            self.assertIsInstance(item, int, 'each result item is a str value')

    @patch('movies.get_gen_info')
    def test_get_gen_info(self, mock_get_gen_info):
        mock_get_gen_info.return_value = ('The Matrix', 'The Matrix', '1999-03-30', 'en', 'lorem ipsum')
        result = movies.get_gen_info()
        for item in result:
            self.assertIsInstance(item, str, 'each result item is a str value')

    @patch('movies.get_directors')
    def test_get_directors(self, mock_get_directors):
        mock_get_directors.return_value = [{'id': 9339, 'name': 'Lilly Wachowski'}, {'id': 9340, 'name': 'Lana Wachowski'}]
        result = movies.get_directors()
        
        self.assertIsInstance(result, list, 'result should return a list')
        for item in result:
            self.assertIsInstance(item, dict, 'if not empty, list can only contain dict items')
            for k, v in item.items():
                self.assertIsInstance(k, str, 'dictionary key should be a str')
                self.assertIsInstance(v, (str, int), 'key values can be str or int')

    @patch('movies.get_cast')
    def test_get_cast(self, mock_get_cast):
        mock_get_cast.return_value = [{'id': 6384, 'name': 'Keanu Reeves'}, {'id': 2975, 'name': 'Laurence Fishburne'}, {'id': 530, 'name': 'Carrie-Anne Moss'}]
        result = movies.get_cast()

        self.assertIsInstance(result, list, 'result should return a list')
        for item in result:
            self.assertIsInstance(item, dict, 'if not empty, list can only contain dict items')
            for k, v in item.items():
                self.assertIsInstance(k, str, 'dictionary key should be a str')
                self.assertIsInstance(v, (str, int), 'key values can be str or int')

    @patch('movies.get_funders')
    def test_get_funders(self, mock_get_funders):
        mock_get_funders.return_value = (
            [{'iso_3166_1': 'US', 'name': 'United States of America'}], 
            [{'id': 420, 'name': 'Marvel Studios', 'origin_country': 'US'}]
        )
        result = movies.get_funders()

        for item in result:
            self.assertIsInstance(item, list, 'each result should be a list')
            for list_item in item:
                self.assertIsInstance(list_item, dict, 'list item should be a dict, if not empty')
                for k, v in list_item.items():
                    self.assertIsInstance(k, str, 'each key should be a str value')
                    self.assertIsInstance(v, (int, str), 'key values can be int or str')



if __name__ == '__main__':
    unittest.main()