import pandas as pd
from bs4 import BeautifulSoup
import csv
import cloudscraper


class TickerScrapper:
    ticker = ''
    ticker_info = {}

    def __init__(self, ticker):
        self.ticker = ticker
        self.get_page_content()

    def get_page_content(self):
        scraper = cloudscraper.create_scraper()

        url = "https://fundamentus.com.br/detalhes.php?papel="
        response = scraper.get(url + self.ticker)

        soup = BeautifulSoup(response.text, features="lxml")
        tables = soup.find_all("table")

        overview_table = tables[0]
        df = pd.read_html(str(overview_table), decimal=',', thousands='.')[0]
        self.ticker_info['papel'] = df[1][0]
        self.ticker_info['empresa'] = df[1][2]
        self.ticker_info['setor'] = df[1][3]
        self.ticker_info['subsetor'] = df[1][4]
        self.ticker_info['data_ult_cotacao'] = df[3][1]

    def get(self):
        return self.ticker_info


TickerScrapper('VIIA3').get_page_content()
