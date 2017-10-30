import csv
import datetime as dt
from SampleStrategy import SampleStrategy
from MarketSimulator import MarketSimulator
from Analyze import PortfolioAnalyzer

def sp500symbols():
    out = []
    with open('symbols/snp500.csv', 'r') as fin:
        reader = csv.reader(fin)
        for row in reader:
            out.append(row[0])
    return out

def test(ls_symbols, s_market_sym, dt_start, dt_end, f_starting_cash):

    id = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    profilerFile = 'strategyTest/EventStudy_' + id + '.pdf'
    ordersFile = 'strategyTest/orders_' + id + '.csv'
    valuesFile = 'strategyTest/values_' + id + '.csv'

    def analyze():
        PortfolioAnalyzer(valuesFile, s_market_sym).run()

    def doTest(df_price):
        SampleStrategy(df_price, s_market_sym, profilerFile, ordersFile).studyEvents()
        MarketSimulator(f_starting_cash, ordersFile, valuesFile, 1, analyze).run()

    import HistoricalData
    ls_symbols.append(s_market_sym)
    HistoricalData.requestMultiple(ls_symbols, dt_start, dt_end, 'ADJUSTED_LAST', '1 day', doTest)

