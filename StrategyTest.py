import csv
import datetime as dt
import pdb
from MarketSimulator import MarketSimulator
from Analyze import PortfolioAnalyzer
import HistoricalData

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
        from SampleStrategy import SampleStrategy
        SampleStrategy(df_price, s_market_sym, profilerFile, ordersFile).studyEvents()
        MarketSimulator(f_starting_cash, ordersFile, valuesFile, 1, analyze).run()

    ls_symbols.append(s_market_sym)
    HistoricalData.requestMultiple(ls_symbols, dt_start, dt_end, 'ADJUSTED_LAST', '1 day', doTest)

def testBollinger(ls_symbols, s_market_sym, dt_start, dt_end, f_starting_cash, n_band_width, n_days_to_look_back):

    id = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    s_bollinger_index_out_file = 'strategyTest/bollinger-index-' + id + '.csv'
    s_plot_out_file_prefix = 'strategyTest/bollinger-' + id;
    s_orders_out_file = 'strategyTest/bollinger-orders-' + id + '.csv'
    s_values_out_file = 'strategyTest/bollinger-values-' + id + '.csv'

    def doTest(df_price):
        from BollingerBandAnalysis import BollingerBandAnalysis
        BollingerBandAnalysis(df_price, s_bollinger_index_out_file, s_plot_out_file_prefix, s_orders_out_file, s_values_out_file).run(n_band_width, n_days_to_look_back, f_starting_cash)
        PortfolioAnalyzer(s_values_out_file, s_market_sym).run()

    HistoricalData.requestMultiple(ls_symbols, dt_start, dt_end, 'ADJUSTED_LAST', '1 day', doTest)
