import pandas as pd
from pathlib import Path
from datetime import datetime
from fundamentus_crawler.ticker_scrapper import TickerScrapper
import json
from bs4 import BeautifulSoup
import cloudscraper
import time
from datetime import date

MIN_PATRIMONIO = 300000000.00
CRESCIMENTO = 'crescRec'
MARGEM_LIQUIDA = 'margemLiq'
MARGEM_EBIT = 'margemEbit'
ROE = 'roe'
P_VP = 'pByVp'
P_EBIT = 'pByEbit'
DIVIDEND_YIELD = 'dividendYield'
ROIC = 'roic'
ROIC_RANKING = 'roicRanking'
EV_EBIT = 'evByEbit'
EV_EBIT_RANKING = 'evByEbitRanking'
PATRIMONIO = 'patrimonioLiquido'
MAGIC = 'magicRanking'
MAGIC_VALUE = 'magicValue'
SMALLCAP = 'smallcap'
PAPEL = 'papel'
NOME_EMPRESA = 'empresa'
SETOR = 'setor'
SUBSETOR = 'subsetor'
DATA_ULTIMA_COTACAO = 'ultCotacao'
VALOR_MERCADO = 'valorMercado'
COTACAO = 'cotacao'
NUMERO_ACOES = 'numeroAcoes'
EBIT = 'ebit'
VALOR_FIRMA = 'valorFirma',
VALOR_MERCADO = 'valorMercado',
COTACAO_TO_TOP30 = 'cotacaoToTop30'

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
    "Subsetor": "subsetor",
    "Smallcap": "smallcap",
    "EV/EBIT Ranking": "evByEbitRanking",
    "ROIC Ranking": "roicRanking",
    "Magic Ranking": "magicRanking",
    "Data Últ. Cotação": "ultCotacao",
    "Valor de mercado": "valorMercado",
    "EBIT": "ebit",
    "Valor de mercado": "valorMercado",
    "Valor de firma": "valorFirma",
    "Cotacao para top30": "cotacaoToTop30"
}


class FundamentusScraper():
    df = {}
    ticker_dict = {}

    def main(self):
        self.get_initial_data()
        self.setup()
        self.crawl_stock_data()
        self.set_valor_mercado_column()
        self.set_numero_acoes_column()
        self.set_ebit_column()
        self.set_valor_firma_column()
        self.remove_old_tickers()
        self.set_small_cap_column()
        self.set_ev_ebti_ranking_row()
        self.set_roic_ranking_column()
        self.set_magic_ranking_row()
        self.get_cotacao_to_top_column()
        self.save_results()

    def get_initial_data(self):
        scraper = cloudscraper.create_scraper()

        url = "https://www.fundamentus.com.br/resultado.php"
        response = scraper.get(url)

        soup = BeautifulSoup(response.text, features="lxml")
        table = soup.select_one("table#resultado")

        self.df = pd.read_html(str(table), decimal=',', thousands='.')[0]
        print(self.df)

    def setup(self):
        self.ticker_dict = self.get_initial_ticker_dict()

        def parse_percentage_fields(name):
            self.df[name] = self.df[name].str.strip('%').replace('\.', '', regex=True).replace(
                ',', '.', regex=True).astype(float)

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

    def crawl_stock_data(self):

        def get_ticker(name):
            if name in self.ticker_dict:
                print(name, 'from cache')
                return self.ticker_dict[name]

            print(name, 'fetching')
            ticker = TickerScrapper(name).get()
            ticker_data = {
                "papel": ticker["papel"],
                "empresa": ticker["empresa"],
                "setor": ticker["setor"],
                "subsetor": ticker["subsetor"],
                "data_ult_cotacao": ticker["data_ult_cotacao"],
            }

            self.ticker_dict[name] = ticker_data
            return ticker_data

        def get_row_value(row, field):
            ticker_name = row[PAPEL]
            ticker = get_ticker(ticker_name)

            if field in ticker:
                return ticker[field]

            return ''

        setor_column = self.df.apply(
            lambda row: get_row_value(row, SETOR), axis=1)
        subsetor_column = self.df.apply(
            lambda row: get_row_value(row, SUBSETOR), axis=1)
        nome_empresa_column = self.df.apply(
            lambda row: get_row_value(row, NOME_EMPRESA), axis=1)
        data_ultima_cotacao_column = self.df.apply(
            lambda row: get_row_value(row, 'data_ult_cotacao'), axis=1)

        self.df[SUBSETOR] = subsetor_column
        self.df[SETOR] = setor_column
        self.df[NOME_EMPRESA] = nome_empresa_column
        self.df[DATA_ULTIMA_COTACAO] = data_ultima_cotacao_column
        return self.df

    def set_valor_mercado_column(self):
        def get_valor_mercado_row_value(row):
            print(row[PAPEL], row[P_VP], row[PATRIMONIO])
            return row[P_VP] * row[PATRIMONIO]

        valor_mercado_values = self.df.apply(
            lambda row: get_valor_mercado_row_value(row), axis=1)

        self.df[VALOR_MERCADO] = valor_mercado_values
        return self.df

    def set_numero_acoes_column(self):
        def get_numero_acoes_row_value(row):
            try:
                return row[VALOR_MERCADO] / row[COTACAO]
            except:
                return 0

        numero_acoes_values = self.df.apply(
            lambda row: get_numero_acoes_row_value(row), axis=1)

        self.df[NUMERO_ACOES] = numero_acoes_values
        return self.df

    def set_ebit_column(self):
        def get_ebit_row_value(row):
            try:
                return row[COTACAO] / row[P_EBIT] * row[NUMERO_ACOES]
            except:
                return 0

        ebit_values = self.df.apply(
            lambda row: get_ebit_row_value(row), axis=1)

        self.df[EBIT] = ebit_values
        return self.df

    def set_valor_firma_column(self):
        def get_valor_firma_row_value(row):
            try:
                return row[EBIT] * row[EV_EBIT]
            except:
                return 0

        valor_firma_values = self.df.apply(
            lambda row: get_valor_firma_row_value(row), axis=1)

        self.df[VALOR_FIRMA] = valor_firma_values
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

        magic_value_column_values = self.df.apply(
            lambda row: get_row_value(row), axis=1)
        total_rows = len(self.df.index)
        ranking_column_values = range(1, total_rows + 1)

        self.df[MAGIC_VALUE] = magic_value_column_values
        self.df = self.df.sort_values(by=[MAGIC_VALUE])
        self.df[MAGIC] = ranking_column_values

        return self.df

    def remove_old_tickers(self):
        def get_row_value(row):
            splited = row[DATA_ULTIMA_COTACAO].split('/')
            dia = splited[0]
            mes = splited[1]
            ano = splited[2]
            return ano + '-' + mes + '-' + dia

        data_ultima_cotacao_column_values = self.df.apply(
            lambda row: get_row_value(row), axis=1)

        today = date.today()

        if (today.month == 1):
            minimum_date = date(today.year - 1, 12, 1)
        else:
            minimum_date = date(today.year, today.month, 1)

        print(self.df)

        self.df[DATA_ULTIMA_COTACAO] = data_ultima_cotacao_column_values
        self.df = self.df.loc[self.df[DATA_ULTIMA_COTACAO]
                              > minimum_date.isoformat()]

        print(self.df)

        return self.df

    def get_cotacao_to_top_column(self):

        top30 = self.df.loc[self.df[MAGIC] == 30].iloc[0]
        top30_magic_score = top30[MAGIC_VALUE]

        def get_cotacao_to_top_row_value(row):
            try:
                if (row[MAGIC] < 30):
                    return 0

                this_row_roic_ranking = row[ROIC_RANKING]
                desired_ev_ebit_ranking = top30_magic_score - this_row_roic_ranking
                print('-', row[PAPEL], top30_magic_score,
                      this_row_roic_ranking)
                if (desired_ev_ebit_ranking <= 0):
                    return 0

                lookup_item = self.df.loc[self.df[EV_EBIT_RANKING]
                                          == desired_ev_ebit_ranking].iloc[0]

                divida_liquida = row[VALOR_MERCADO] - row[VALOR_FIRMA]
                potencial_valor_mercado = (
                    row[EBIT] * lookup_item[EV_EBIT]) - divida_liquida

                potencial_valor = potencial_valor_mercado / row[NUMERO_ACOES]

                if (potencial_valor > row[COTACAO]):
                    return 0

                return potencial_valor
            except:
                return 0

        cotacao_to_top_values = self.df.apply(
            lambda row: get_cotacao_to_top_row_value(row), axis=1)

        self.df[COTACAO_TO_TOP30] = cotacao_to_top_values
        return self.df

    def save_results(self):
        folder = datetime.now().strftime("%d-%b-%Y")
        filename = datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)") + '.csv'
        json_filename = datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)") + '.json'

        front_filepath = Path('../stock-picking/src/data.json')
        latest_filepath = Path('fundamentus_crawler/json/latest.json')
        json_filepath = Path('fundamentus_crawler/json/' +
                             folder + '/' + json_filename)
        filepath = Path('fundamentus_crawler/results/' +
                        folder + '/' + filename)
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
        with open(latest_filepath, 'w') as outfile:
            outfile.write(json_string)
        with open(front_filepath, 'w') as outfile:
            outfile.write(json_string)


FundamentusScraper().main()
