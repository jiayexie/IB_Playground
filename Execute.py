
def execute():

    # simulateMarket()

    import SymbolsToContracts
    SymbolsToContracts.load('symbols/snp500.csv')

def simulateMarket():
    from MarketSimulator import MarketSimulator
    MarketSimulator(1000000, "testdata/orders.csv", "testdata/values.csv", 1, callback=analyzePortfolio).run()

def analyzePortfolio():
    from Analyze import PortfolioAnalyzer
    PortfolioAnalyzer("testdata/values.csv", "SPY").run()
