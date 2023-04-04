![vizualization](./img/blue_short-8e7b30f73a4020692ccca9c88bafe5dcb6f8a62a4c6bc55cd9ba82bb2cd95f6c.svg)
# The Movie Database Data

#### By Jarret Jeter

### A python script to extract film data from The Movie Database's API using the api wrapper [tmdbsimple](https://github.com/celiao/tmdbsimple).


## Technologies Used
* _azure data lake storage gen2_
* _azure batch_
* _powerBI_
* _python_
* _pandas_
* _tmdbsimple_
* _typer_

## Description
The intent of this project was to get revenue data on the highest earning American films from 2000 to 2022, but seeing some of the data I wasn't entirely sure what makes a film "American"(Produced entirely in America? American setting? What if it's set in America but produced by the United Kingdom?). I settled on obtaining data for any films released in the US, foreign or not, and the revenue generated per year. 

The script uses 7 functions: list_pages() to retrieve the total pages returned from a search, get_data() to retrieve data for a specific search result page, output_csv() to save the page data to a csv in a subdirectory, retry_missing() reattempts to get the failed pages from a dictionary, merge_dfs() to create a single csv of all the page results, blob_upload() uploads to Azure Data Lake Storage, main() for orchestration of the previous functions and concurrency. There's an optional function, show_containers() to list available containers in your azure storage account. For faster processing, the script can be run on an azure batch pool node.

![visualization](./img/us_movie_revenue.png)

## Setup/Installation Requirements
You'll need a [tmdb](https://www.themoviedb.org/) account to access the site's API as well as an [Azure](https://azure.microsoft.com/en-us/products/storage/data-lake-storage/) storage account.
* Clone this repository (https://github.com/jarretjeter/reddit-scraper.git) onto your local computer from github
* In VS Code or another text editor, open this project
* With your terminal, install a python3.8 virtual environment in the project's directory, activate it and enter the command 'pip install -r requirements.txt' to get the necessary dependencies.
* Create a file named "config.json" in the root directory and enter your tmdb API and Azure storage details into so the main.py script can access them.
* Once that's setup you can run the commands 'python main.py run_main {region} {year_start} {year_end} [optional]{--upload / --no-upload}' in the terminal to begin fetching the data.

## Known Bugs
* none currently

## to-do:
* additionally store data in MySQL for querying

## License
[MIT](https://github.com/jarretjeter/tmdbdata/blob/main/LICENSE.txt)

_Copyright (c) January 24 2023 Jarret Jeter_