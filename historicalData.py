import datetime as dt
import numpy as np
import pandas as pd
import pdb
import logging

# types
from ibapi.common import *
from ibapi.order_condition import *
from ibapi.contract import *
from ibapi.order import *
from ibapi.order_state import *
from ibapi.execution import Execution
from ibapi.execution import ExecutionFilter
from ibapi.commission_report import CommissionReport
from ibapi.scanner import ScannerSubscription
from ibapi.ticktype import *

from app import TestApp
import contracts
from util import Singleton, SetupLogger

class Request:
    def __init__(self, contract, dataType, dataCallback, endDate, endCallback):
        self.contract = contract
        self.dataType = dataType
        self.dataCallback = dataCallback
        self.endDate = endCallback
        self.endCallback = endCallback

global timeFormat, requestDict, nextReqId

timeFormat = "%Y%m%d %H:%M:%S"
dateFormat = "%Y%m%d"

requestDict = {}
nextReqId = 0

def request(symbol: str, endDate: dt.datetime=None, dataType="ADJUSTED_LAST", durationStr="1 Y", barSizeStr="1 day", callback=None):
    SetupLogger()
    csvFileName = csvFileNameForRequest(symbol, endDate, dataType, durationStr, barSizeStr)

    foundInCache = False
    try:
        df = pd.read_csv(csvFileName, index_col=0, parse_dates=[0])
        foundInCache = True
    except:
        pass

    if foundInCache:
        logging.debug("Serving %s historical data from cache", symbol)
        callback(symbol, df)
    else:
        def actuallyRequest(contract: Contract):
            print("Requesting historical data for %s", symbol)
            global nextReqId
            reqId = nextReqId = nextReqId + 1

            df = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "count", "WAP"])

            def dataCallback(bar: BarData):
                try:
                    date = dt.datetime.strptime(bar.date, timeFormat)
                except:
                    date = dt.datetime.strptime(bar.date, dateFormat)
                if date > endDate:
                    resolveEnd(reqId, None, None)
                df.loc[date] = np.array([bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average])

            def endCallback():
                df.to_csv(csvFileName)
                callback(symbol, df)

            req = Request(contract, dataType, dataCallback, endDate, endCallback)
            requestDict[reqId] = req

            queryTime = "" if endDate == None or dataType == "ADJUSTED_LAST" else endDate.strftime(timeFormat)
            TestApp.Instance().reqHistoricalData(nextReqId, contract, queryTime, durationStr, barSizeStr, dataType, 1, 1, False, [])

        contracts.request(symbol, actuallyRequest)

def csvFileNameForRequest(symbol: str, endDate: dt.datetime, dataType="ADJUSTED_LAST", durationStr="1 Y", barSizeStr="1 day"):
    if endDate == None:
        endDate = dt.datetime.now()
    if barSizeStr.endswith("day"):
        endDateStr = endDate.strftime("%Y%m%d")
    else:
        endDateStr = endDate.strftime("%Y%m%d%H%M%S")

    filename = "hist/" + symbol + "_" + endDateStr + "_" + dataType + "_" + durationStr.replace(" ", "") + "_" + barSizeStr.replace(" ", "") + ".csv"
    return filename

def cancel(reqId):
    TestApp.Instance().cancelHistoricalData(reqId)
    del requestDict[reqId]

def cancelAll():
    logging.debug("Canceling historical data requests")
    cancel(reqId for reqId in requestDict.keys())

def resolve(reqId:int, bar:BarData):
    req = requestDict.get(reqId)
    if req != None:
        logging.debug("Receiving historical data for %s", req.contract.symbol)
        if req.dataCallback != None:
            req.dataCallback(bar)

def resolveEnd(reqId: int, start: str, end: str):
    req = requestDict.get(reqId)
    if req != None:
        print ("Closing historical data for", req.contract.symbol)
        if req.endCallback != None:
            req.endCallback()
            del requestDict[reqId]

