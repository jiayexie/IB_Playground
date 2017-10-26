import datetime as dt
import numpy as np
import pandas as pd
import pdb
import logging
import math

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
    def __init__(self, contract, dataType, dataCallback, endCallback):
        self.contract = contract
        self.dataType = dataType
        self.dataCallback = dataCallback
        self.endCallback = endCallback

global timeFormat, requestDict, nextReqId

timeFormat = "%Y%m%d %H:%M:%S"
dateFormat = "%Y%m%d"

requestDict = {}
nextReqId = 0

def request(symbol: str, startDate: dt.datetime=None, endDate: dt.datetime=None, dataType="ADJUSTED_LAST", barSizeStr="1 day", callback=None):
    SetupLogger()
    csvFileName = csvFileNameForRequest(symbol, startDate, endDate, dataType, barSizeStr)

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
            print("Requesting historical data for", symbol)
            global nextReqId
            reqId = nextReqId = nextReqId + 1

            df = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "count", "WAP"])

            start = startDate
            end = endDate
            duration = dt.timedelta(days=1)
            if dataType == "ADJUSTED_LAST":
                duration += dt.datetime.now() - start
            else:
                duration += end - start
            if duration.days > 365:
                durationStr = str(math.ceil(duration.days / 365)) + " Y"
            else:
                durationStr = str(math.ceil(duration.days / 28)) + " M"

            if barSizeStr == "1 day":
                start = start.replace(hour=0)
                end = end.replace(hour=16)

            def dataCallback(bar: BarData):
                try:
                    date = dt.datetime.strptime(bar.date, timeFormat)
                except:
                    date = dt.datetime.strptime(bar.date, dateFormat)
                if date > end:
                    resolveEnd(reqId, None, None)
                elif date >= start:
                    df.loc[date] = np.array([bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average])

            def endCallback():
                for column in df.columns.values:
                    df[column] = df[column].fillna(method="ffill")
                    df[column] = df[column].fillna(method="bfill")
                    df[column] = df[column].fillna(1.0)
                df.to_csv(csvFileName)
                callback(symbol, df)

            req = Request(contract, dataType, dataCallback, endCallback)
            requestDict[reqId] = req

            queryTime = "" if endDate == None or dataType == "ADJUSTED_LAST" else endDate.strftime(timeFormat)
            TestApp.Instance().reqHistoricalData(nextReqId, contract, queryTime, durationStr, barSizeStr, dataType, 1, 1, False, [])

        contracts.request(symbol, actuallyRequest)

def csvFileNameForRequest(symbol: str, startDate: dt.datetime, endDate: dt.datetime, dataType="ADJUSTED_LAST",  barSizeStr="1 day"):
    if endDate == None:
        endDate = dt.datetime.now()
    if startDate == None:
        startDate = dt.datetime.now()
    if barSizeStr.endswith("day"):
        startDateStr = startDate.strftime("%Y%m%d")
        endDateStr = endDate.strftime("%Y%m%d")
    else:
        startDateStr = startDate.strftime("%Y%m%d%H%M%S")
        endDateStr = endDate.strftime("%Y%m%d%H%M%S")

    filename = "hist/" + symbol + "_" + startDateStr + "_" + endDateStr + "_" + dataType + "_" + barSizeStr.replace(" ", "") + ".csv"
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

