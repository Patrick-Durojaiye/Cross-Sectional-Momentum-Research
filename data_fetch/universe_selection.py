import os
import time
import logging
from typing import Optional, List
from datetime import datetime, timedelta

import pandas as pd


from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    filename='scraper.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

class CoinMarketCapScraper:
    CMC_BASE_URL: str = "https://coinmarketcap.com/historical/"
    SLEEP_PAGE_LOAD: int = 2
    SCROLL_PAUSE_TIME_IN_SECS: int = 2
    SCROLL_INCREMENT_IN_PX: int = 1000

    def __init__(self, headless: bool = True):
        """
        Initialize the CoinMarketCapScraper.

        Parameters
        ----------
        headless : bool
            Whether to run the browser in headless mode.
        """
        self.driver = self._create_driver(headless)

    def _create_driver(self, headless: bool) -> webdriver.Chrome:
        """
        Create and configure a Chrome WebDriver instance.

        Parameters
        ----------
        headless : bool
            Whether to run the browser in headless mode.

        Returns
        -------
        webdriver.Chrome
            Configured Chrome WebDriver instance.
        """
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--lang=en-US")

        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()), options=options
        )
        return driver

    def scroll_page(self) -> None:
        """
        Scroll the page to load all dynamic content.
        """
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script(
                f"window.scrollBy(0, {self.SCROLL_INCREMENT_IN_PX});"
            )
            time.sleep(self.SCROLL_PAUSE_TIME_IN_SECS)
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

    def parse_table(self, soup: BeautifulSoup) -> Optional[pd.DataFrame]:
        """
        Parse the table content from the BeautifulSoup object.

        Parameters
        ----------
        soup : BeautifulSoup
            BeautifulSoup object containing the parsed HTML.

        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame containing the parsed table data, or None if no table is found.
        """
        main_content = soup.find('div', class_='cmc-table-listing')
        if not main_content:
            return None
        tables = main_content.find_all('table')
        if not tables:
            return None

        table = tables[-1]
        headers: List[str] = [
            header.text.strip() for header in table.find('thead').find_all('th')
        ]
        rows: List[List[str]] = [
            [cell.text.strip() for cell in row.find_all('td')]
            for row in table.find('tbody').find_all('tr')
        ]

        return pd.DataFrame(rows, columns=headers)

    def get_snapshot(self, snap_date: str) -> Optional[pd.DataFrame]:
        """
        Fetch and parse cryptocurrency snapshot data from CoinMarketCap for a given date.

        Parameters
        ----------
        snap_date : str
            Date of the snapshot in 'YYYYMMDD' format.

        Returns
        -------
        Optional[pd.DataFrame]
            DataFrame containing the scraped snapshot data, or None if an error occurred.
        """
        try:
            self.driver.get(f"{self.CMC_BASE_URL}{snap_date}")
            time.sleep(self.SLEEP_PAGE_LOAD)
            self.scroll_page()
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            df_snap = self.parse_table(soup)
        except Exception as e:
            logger.error(
                f"An error occurred while scraping the snapshot date '{snap_date}'. Error: {e}"
            )
            return None
        else:
            logger.info(f"Successfully scraped the snapshot date '{snap_date}'.")
            return df_snap

    def generate_snapshot_dates(
        self, start_date_str: str, end_date_str: str, delta_days: int = 7
    ) -> List[str]:
        """
        Generate a list of snapshot dates between start_date and end_date, stepping by delta_days.

        Parameters
        ----------
        start_date_str : str
            Start date in 'YYYYMMDD' format.
        end_date_str : str
            End date in 'YYYYMMDD' format.
        delta_days : int, optional
            Step size in days, by default 7.

        Returns
        -------
        List[str]
            List of dates in 'YYYYMMDD' format.
        """
        dates = []
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.strptime(end_date_str, '%Y%m%d')
        delta = timedelta(days=delta_days)
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date.strftime('%Y%m%d'))
            current_date += delta
        return dates

    def save_snapshot(
        self, df: pd.DataFrame, snap_date: str, formats: List[str] = ['parquet']
    ):
        """
        Save the snapshot DataFrame in the specified formats.

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to save.
        snap_date : str
            Snapshot date in 'YYYYMMDD' format, used in the filename.
        formats : List[str], optional
            List of formats to save ('csv', 'parquet'), by default ['parquet'].
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.abspath(os.path.join(script_dir, os.pardir))
        parquet_dir = os.path.join(parent_dir, 'data', 'uncleaned')
        os.makedirs(parquet_dir, exist_ok=True)

        for fmt in formats:
            if fmt == 'csv':
                csv_filename = f"universe_snapshot_{snap_date}.csv"
                df.to_csv(csv_filename, index=False)
                logger.info(f"Data saved to {csv_filename}")
            elif fmt == 'parquet':
                parquet_filename = f"universe_snapshot_{snap_date}.parquet"
                parquet_filepath = os.path.join(parquet_dir, parquet_filename)
                df.to_parquet(parquet_filepath, index=False)
                logger.info(f"Data saved to {parquet_filepath}")
            else:
                logger.warning(f"Format '{fmt}' not supported. Skipping.")

    def scrape(
        self,
        start_date: str,
        end_date: str,
        delta_days: int = 7,
        save_formats: List[str] = ['parquet'],
    ):
        """
        Main method to scrape data over a range of dates.

        Parameters
        ----------
        start_date : str
            Start date in 'YYYYMMDD' format.
        end_date : str
            End date in 'YYYYMMDD' format.
        delta_days : int, optional
            Number of days between snapshots, by default 7.
        save_formats : List[str], optional
            Formats to save the data ('csv', 'parquet'), by default ['parquet'].
        """
        snapshot_dates = self.generate_snapshot_dates(
            start_date, end_date, delta_days
        )
        for snapshot_date in snapshot_dates:
            df_snapshot = self.get_snapshot(snapshot_date)
            if df_snapshot is not None:
                print(f"Snapshot for date {snapshot_date}:")
                print(df_snapshot.head())
                self.save_snapshot(df_snapshot, snapshot_date, formats=save_formats)
            else:
                print(f"No data for date {snapshot_date}")

    def close(self):
        """
        Close the WebDriver instance.
        """
        self.driver.quit()


if __name__ == "__main__":
    scraper = CoinMarketCapScraper(headless=True)
    try:
        start_date = '20230101'  # Start date in 'YYYYMMDD' format
        end_date = '20241027'    # End date in 'YYYYMMDD' format
        scraper.scrape(
            start_date=start_date,
            end_date=end_date,
            delta_days=7,
            save_formats=['parquet']
        )
    finally:
        scraper.close()