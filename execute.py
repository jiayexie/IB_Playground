import datetime as dt

def execute():
    # import historicalData
    # endDate = dt.datetime(2016,12,31,16)
    # historicalData.request("MSFT", endDate, "TRADES", "1 Y", "1 day", print)
    # historicalData.request("GOOG", endDate, "TRADES", "1 Y", "1 day", print)

    from marketsim import MarketSimulator
    MarketSimulator(1000000, "testdata/orders-short.csv", "testdata/values-short.csv").run()
