import datetime as dt

def execute():

    # simulateMarket()
    # loadContracts()
    # testSampleStrategy()

    testBollingerAnalysis()

def testBollingerAnalysis():
    from StrategyTest import testBollinger, sp500symbols
    testBollinger(sp500symbols(), 'SPY', dt.datetime(2017, 1, 1), dt.datetime(2017, 6, 1), 50000, 2, 20)
    # testBollinger(['MSFT', 'AMZN', 'GOOG', 'AAPL'], 'SPY', dt.datetime(2017, 1, 1), dt.datetime(2017, 6, 1), 10000, 1, 20)

def testSampleStrategy():
    from StrategyTest import test, sp500symbols
    test(sp500symbols(), 'SPY', dt.datetime(2016, 10, 1), dt.datetime(2017, 10, 1), 100000)

def simulateMarket():
    from MarketSimulator import MarketSimulator
    MarketSimulator(1000000, "testdata/orders.csv", "testdata/values.csv", 1, callback=analyzePortfolio).run()

def analyzePortfolio():
    from Analyze import PortfolioAnalyzer
    PortfolioAnalyzer("testdata/values.csv", "SPY").run()

def loadContracts():
    import SymbolsToContracts
    SymbolsToContracts.load('symbols/snp500.csv')


