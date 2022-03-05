import pandas as pd
from pathlib import Path
from datetime import datetime
from yahooquery import Ticker
import json
from bs4 import BeautifulSoup
import csv
import cloudscraper

MIN_PATRIMONIO = 300000000.00
CRESCIMENTO = 'crescRec'
MARGEM_LIQUIDA = 'margemLiq'
MARGEM_EBIT = 'margemEbit'
ROE = 'roe'
DIVIDEND_YIELD = 'dividendYield'
ROIC = 'roic'
ROIC_RANKING = 'roicRanking'
EV_EBIT = 'evByEbit'
EV_EBIT_RANKING = 'evByEbitRanking'
PATRIMONIO = 'patrimonioLiquido'
MAGIC = 'magicRanking'
SMALLCAP = 'smallcap'
PAPEL = 'papel'
INDUSTRIA = 'industria'
NOME_EMPRESA_LONGO = 'nomeEmpresaLongo'
NOME_EMPRESA = 'nomeEmpresa'
SETOR = 'setor'
CACHE_FILE_NAME = 'fundamentus_crawler/ticker.json'
TEMPORARY_SHEET = 'out.csv'

PARSED_COLUMN_NAMES = {
    "Papel": "papel",
    "Cotação": "cotacao",
    "P/L": "pByL",
    "P/VP": "pByVp",
    "PSR": "psr",
    "Div.Yield": "dividendYield",
    "P/Ativo": "pByAtivo",
    "P/Cap.Giro": "pByCapitalGiro",
    "P/EBIT": "pByEbit",
    "P/Ativ Circ.Liq": "pByAtivoCircLiq",
    "EV/EBIT": "evByEbit",
    "EV/EBITDA": "evByEbitda",
    "Mrg Ebit": "margemEbit",
    "Mrg. Líq.": "margemLiq",
    "Liq. Corr.": "liqCorr",
    "ROIC": "roic",
    "ROE": "roe",
    "Liq.2meses": "liqDoisMeses",
    "Patrim. Líq": "patrimonioLiquido",
    "Dív.Brut/ Patrim.": "dividaBrutaByPatrimonio",
    "Cresc. Rec.5a": "crescRec",
    "Setor": "setor",
    "Indústria": "industria",
    "Smallcap": "smallcap",
    "EV/EBIT Ranking": "evByEbitRanking",
    "ROIC Ranking": "roicRanking",
    "Magic Ranking": "magicRanking"
}


class FundamentusScraper():
    df = {}
    ticker_dict = {}

    def main(self):
        self.get_initial_data()
        self.setup()
        self.decorate_data()
        self.set_small_cap_column()
        self.set_ev_ebti_ranking_row()
        self.set_roic_ranking_column()
        self.set_magic_ranking_row()
        self.save_results()

    def get_initial_data(self):
        scraper = cloudscraper.create_scraper()

        url = "https://www.fundamentus.com.br/resultado.php"
        response = scraper.get(url)

        soup = BeautifulSoup(response.text)
        table = soup.select_one("table#resultado")
        headers = [th.text for th in table.select("tr th")]

        with open(TEMPORARY_SHEET, "w") as f:
            wr = csv.writer(f)
            wr.writerow(headers)
            wr.writerows([[td.text for td in row.find_all("td")]
                          for row in table.select("tr + tr")])

    def setup(self):
        self.ticker_dict = self.get_initial_ticker_dict()

        def parse_percentage_fields(name):
            self.df[name] = self.df[name].str.strip('%').replace('\.', '', regex=True).replace(
                ',', '.', regex=True).astype(float)

        self.df = pd.read_csv(TEMPORARY_SHEET, decimal=',', thousands='.')

        self.df = self.df.rename(
            columns=PARSED_COLUMN_NAMES)

        percentage_fields = [CRESCIMENTO,
                             MARGEM_LIQUIDA,
                             MARGEM_EBIT,
                             ROE,
                             DIVIDEND_YIELD,
                             ROIC]

        for i in percentage_fields:
            parse_percentage_fields(i)

        self.df = self.df.loc[self.df[EV_EBIT] > 0]

    def get_initial_ticker_dict(self):
        try:
            f = open(CACHE_FILE_NAME, "r")
            print(f)
            return json.load(f)
        except:
            print('e')
            return {}

    def decorate_data(self):

        def get_ticker(name):
            if name in self.ticker_dict:
                print(name, 'from cache')
                return self.ticker_dict[name]

            print(name, 'fetching')
            new_ticker = Ticker(name)

            ticker_data = {}

            try:
                ticker_data[INDUSTRIA] = new_ticker.asset_profile[name]['industry']
            except:
                print(name, 'error INDUSTRIA')
            try:
                ticker_data[SETOR] = new_ticker.asset_profile[name]['sector']
            except:
                print(name, 'error SETOR')
            try:
                ticker_data[NOME_EMPRESA_LONGO] = new_ticker.quote_type[name]['longName']
            except:
                print(name, 'error NOME_EMPRESA_LONGO')
            try:
                ticker_data[NOME_EMPRESA] = new_ticker.quote_type[name]['shortName']
            except:
                print(name, 'error NOME_EMPRESA')

            self.ticker_dict[name] = ticker_data
            return ticker_data

        def get_row_value(row, field):
            ticker_name = row[PAPEL] + '.SA'
            ticker = get_ticker(ticker_name)

            if field in ticker:
                return ticker[field]

            return ''

        industry_column = self.df.apply(
            lambda row: get_row_value(row, INDUSTRIA), axis=1)
        summary_column = self.df.apply(
            lambda row: get_row_value(row, SETOR), axis=1)
        nome_empresa_longo_column = self.df.apply(
            lambda row: get_row_value(row, NOME_EMPRESA_LONGO), axis=1)
        nome_empresa_column = self.df.apply(
            lambda row: get_row_value(row, NOME_EMPRESA), axis=1)

        self.df[SETOR] = summary_column
        self.df[INDUSTRIA] = industry_column
        self.df[NOME_EMPRESA_LONGO] = nome_empresa_longo_column
        self.df[NOME_EMPRESA] = nome_empresa_column
        return self.df

    def set_small_cap_column(self):
        def get_row_value(row):
            if row[PATRIMONIO] > MIN_PATRIMONIO:
                return False
            return True

        ranking_column_values = self.df.apply(
            lambda row: get_row_value(row), axis=1)

        self.df[SMALLCAP] = ranking_column_values
        return self.df

    def set_roic_ranking_column(self):
        self.df = self.df.sort_values(by=[ROIC], ascending=False)

        total_rows = len(self.df.index)
        ranking_column_values = range(1, total_rows + 1)

        self.df[ROIC_RANKING] = ranking_column_values

        return self.df

    def set_ev_ebti_ranking_row(self):
        self.df = self.df.sort_values(by=[EV_EBIT])

        total_rows = len(self.df.index)
        ranking_column_values = range(1, total_rows + 1)

        self.df[EV_EBIT_RANKING] = ranking_column_values

        return self.df

    def set_magic_ranking_row(self):
        def get_row_value(row):
            return row[EV_EBIT_RANKING] + row[ROIC_RANKING]

        ranking_column_values = self.df.apply(
            lambda row: get_row_value(row), axis=1)

        self.df[MAGIC] = ranking_column_values
        self.df = self.df.sort_values(by=[MAGIC])

        return self.df

    def save_results(self):
        folder = datetime.now().strftime("%d-%b-%Y")
        filename = datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)") + '.csv'
        json_filename = datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)") + '.json'

        json_filepath = Path('json/' + folder + '/' + json_filename)
        filepath = Path('results/' + folder + '/' + filename)
        filepath.parent.mkdir(parents=True, exist_ok=True)
        self.df.to_csv(filepath)

        with open(CACHE_FILE_NAME, 'w') as outfile:
            outfile.write(json.dumps(self.ticker_dict))

        result = self.df.to_json(orient="records")
        parsed = json.loads(result)
        json_string = json.dumps(parsed)

        json_filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(json_filepath, 'w') as outfile:
            outfile.write(json_string)


FundamentusScraper().main()
