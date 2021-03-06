import datetime as dt
import time
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

from App import TestApp
import Contracts
from Util import Singleton, SetupLogger

logger = logging.getLogger('HistoricalData')
logger.setLevel(logging.DEBUG)

class Request:
    def __init__(self, contract, dataType, dataCallback, endCallback):
        self.contract = contract
        self.dataType = dataType
        self.dataCallback = dataCallback
        self.endCallback = endCallback

class QueuedRequest:
    def __init__(self, symbol, startDate, endDate, dataType, barSizeStr, callback):
        self.symbol = symbol
        self.startDate = startDate
        self.endDate = endDate
        self.dataType = dataType
        self.barSizeStr = barSizeStr
        self.callback = callback

global timeFormat, requestDict, nextReqId, queuedReqs

timeFormat = "%Y%m%d %H:%M:%S"
dateFormat = "%Y%m%d"

requestDict = {}
nextReqId = 0

queuedReqs = []

def requestMultiple(symbols: list, startDate: dt.datetime, endDate: dt.datetime, dataType: str, barSizeStr: str, callback = None):
    data = []
    symbols_in_order = []
    def receive(symbol, df):
        logger.debug("Got data for %s", symbol)
        data.append(df)
        symbols_in_order.append(symbol)
        logger.debug('Got data for %d symbols', len(data))
        if len(data) == len(symbols):
            logger.debug("Got all the data we need.")
            df_data = pd.concat(data, keys=symbols_in_order)
            df_data = df_data['close'].unstack().T # Convert into 2D df of closing price with date as index and symbol as column
            for column in df_data.columns.values:
                df_data[column] = df_data[column].fillna(method="ffill")
                df_data[column] = df_data[column].fillna(method="bfill")
                df_data[column] = df_data[column].fillna(1.0)
            callback(df_data)
    for symbol in symbols:
        request(symbol, startDate, endDate, "ADJUSTED_LAST", "1 day", receive)

def request(symbol: str, startDate: dt.datetime=None, endDate: dt.datetime=None, dataType="ADJUSTED_LAST", barSizeStr="1 day", callback=None):
    csvFileName = csvFileNameForRequest(symbol, startDate, endDate, dataType, barSizeStr)

    foundInCache = False
    try:
        df = pd.read_csv(csvFileName, index_col=0, parse_dates=[0])
        foundInCache = True
    except:
        pass

    if foundInCache:
        logger.debug("Serving %s historical data from cache", symbol)
        callback(symbol, df)
    else:
        if len(requestDict) >= 50:
            print ("Queuing request for symbol", symbol)
            queuedReqs.append(QueuedRequest(symbol, startDate, endDate, dataType, barSizeStr, callback))
            return

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

        Contracts.request(symbol, actuallyRequest)

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
    logger.debug("Canceling historical data requests")
    cancel(reqId for reqId in requestDict.keys())

def resolve(reqId:int, bar:BarData):
    req = requestDict.get(reqId)
    if req != None:
        logger.debug("Receiving historical data for %s", req.contract.symbol)
        if req.dataCallback != None:
            req.dataCallback(bar)

def resolveEnd(reqId: int, start: str, end: str):
    req = requestDict.get(reqId)
    if req != None:
        logger.debug("Closing historical data for %s", req.contract.symbol)
        if req.endCallback != None:
            req.endCallback()
        del requestDict[reqId]
        popFromQueue()

def handleError(reqId: int):
    req = requestDict.get(reqId)
    if req != None:
        logger.debug("Historical data for %s cannot be retrieved", req.contract.symbol)
        if req.endCallback != None:
            req.endCallback()
        del requestDict[reqId]
        popFromQueue()

def popFromQueue():
    if len(queuedReqs) != 0:
        req = queuedReqs.pop(0)
        request(req.symbol, req.startDate, req.endDate, req.dataType, req.barSizeStr, req.callback)
