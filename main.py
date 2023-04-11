import concurrent.futures
import logging
from logging import INFO
from movies import *
from storage import blob_upload, to_mysql
import sys
import time
import typer

app = typer.Typer(no_args_is_help=True)

logging.basicConfig(format='[%(asctime)s][%(module)s:%(lineno)04d] : %(message)s', level=INFO, stream=sys.stderr)
logger: logging.Logger = logging


@app.command("run_main")
def main(region: str, year_start: int, year_end: int, upload: bool=True) -> any:
    """
    Pipeline orchestration to get all data for every page in a specified range of years

    Args:
        region: str
            Country to filter by
        year_start: int
            Year to start filtering at
        year_end: int
            Year to stop filtering at
        upload: bool, default True
            (Optional) Upload data to Azure storage
    Returns: None
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
                futures.append(executor.submit(get_data, region, year, page))

        for future in concurrent.futures.as_completed(futures):
            f_year = future.result()[1]
            f_page = future.result()[2]
            mssng_pages[f_year].append(f_page) if f_page != None else None
            
        logger.info(f"Missing: {mssng_pages}")
        for year in year_range:
            retry_missing(region=region, year=year, mssng_pages=mssng_pages) if mssng_pages[year] != [] else None
            df = merge_dfs(region, year, missing=mssng_pages)
            if upload:
                blob_upload(region=region, year=year)
                to_mysql(df=df, year=year)

    tm2 = time.perf_counter()
    print(f"Total time elapsed: {tm2 - tm1:0.2f} seconds")


if __name__ == "__main__":
    app()