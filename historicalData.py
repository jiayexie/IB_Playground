import datetime as dt
import numpy as np
import pandas as pd
import pdb

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
from util import Singleton, printWhenExecuting

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

def request(contract: Contract, endDate: dt.datetime=None, dataType="ADJUSTED_LAST", durationStr="1 Y", barSizeStr="1 day", callback=None):
    csvFileName = csvFileNameForRequest(contract, endDate, dataType, durationStr, barSizeStr)
    
    # pdb.set_trace()
    try:
        df = pd.read_csv(csvFileName, index_col=0, parse_dates=[0])
        callback(df)
    except:
        global nextReqId
        nextReqId = nextReqId + 1
       
        df = pd.DataFrame(columns=["open", "high", "low", "close", "volume", "count", "WAP"])

        def dataCallback(bar: BarData):
            try:
                date = dt.datetime.strptime(bar.date, timeFormat)
            except:
                date = dt.datetime.strptime(bar.date, dateFormat)
            df.loc[date] = np.array([bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average])
        
        def endCallback():
            # pdb.set_trace()
            df.to_csv(csvFileName)
            callback(df)

        req = Request(contract, dataType, dataCallback, endCallback)
        requestDict[nextReqId] = req

        queryTime = "" if endDate == None else endDate.strftime(timeFormat)
        TestApp.Instance().reqHistoricalData(nextReqId, contract, queryTime, durationStr, barSizeStr, dataType, 1, 1, False, [])

        return nextReqId

def csvFileNameForRequest(contract: Contract, endDate: dt.datetime, dataType="ADJUSTED_LAST", durationStr="1 Y", barSizeStr="1 day"):
    if endDate == None:
        endDate = dt.datetime.now()
    if barSizeStr.endswith("day"):
        endDateStr = endDate.strftime("%Y%m%d")
    else:
        endDateStr = endDate.strftime("%Y%m%d%H%M%S")

    filename = "hist/" + contract.symbol + "_" + endDateStr + "_" + dataType + "_" + durationStr.replace(" ", "") + "_" + barSizeStr.replace(" ", "") + ".csv"
    return filename

def cancelAllHistoricalDataRequests():
    for reqId in requestDict.keys():
        TestApp.Instance().cancelHistoricalData(reqId)
        del requestDict[reqId]

def resolve(reqId:int, bar:BarData):
    print("HistoricalData. ", reqId, " Date:", bar.date, "Open:", bar.open,
          "High:", bar.high, "Low:", bar.low, "Close:", bar.close, "Volume:", bar.volume,
          "Count:", bar.barCount, "WAP:", bar.average)
    req = requestDict[reqId]
    if req != None:
        print ("Resolving reqId", reqId)
        if req.dataCallback != None:
            req.dataCallback(bar)
   
def resolveEnd(reqId: int, start: str, end: str):
    print("HistoricalDataEnd ", reqId, "from", start, "to", end)
    req = requestDict[reqId]
    if req != None:
        print ("Closing reqId", reqId)
        if req.endCallback != None:
            req.endCallback()
            del requestDict[reqId]

