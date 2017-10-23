import datetime as dt

def execute():
    import historicalData
    import contracts

    def reqHist(contract):
        historicalData.request(contract, dt.datetime(2016, 12, 31, 16), "TRADES", "1 Y", "1 day", print)

    contracts.request("MSFT", reqHist)

