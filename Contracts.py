import sys
import datetime as dt
import numpy as np
import pandas as pd
import pdb
import csv

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

class Request:
    def __init__(self, symbol, callback):
        self.symbol = symbol
        self.callback = callback

nextReqId = 1000
requestDict = {}
fileName = "contracts.csv"
contractDict = {}

def loadDict():
    if len(contractDict) == 0:
        with open(fileName, mode='r') as fin:
            reader = csv.reader(fin)
            for row in reader:
                contract = Contract()
                contract.symbol = row[0]
                contract.secType = row[1]
                contract.currency = row[2]
                contract.primaryExchange = row[3]
                contract.exchange = row[4]
                contractDict[contract.symbol] = contract

def writeToDict(contract: Contract):
    contractDict[contract.symbol] = contract
    print ("Saving contract: symbol:%s, secType:%s, currency:%s, primaryExchange:%s, exchange:%s" % (
        contract.symbol,
        contract.secType,
        contract.currency,
        contract.primaryExchange,
        contract.exchange
    ))
    with open(fileName, mode='a') as fout:
        writer = csv.writer(fout)
        writer.writerow([
            contract.symbol,
            contract.secType,
            contract.currency,
            contract.primaryExchange,
            contract.exchange
        ])


def request(symbol, callback = None):
    loadDict()
    contract = contractDict.get(symbol)
    if contract != None:
        callback(contract)
        return

    print ("Contract not found in local cache. Requesting contract for", symbol)
    global nextReqId
    nextReqId = nextReqId + 1
    req = Request(symbol, callback)
    requestDict[nextReqId] = req
    TestApp.Instance().reqMatchingSymbols(nextReqId, symbol)

def resolve(reqId, contractDescriptions: ListOfContractDescription):
    req = requestDict[reqId]
    if req != None:
        count = len(contractDescriptions)
        if count == 0:
            print ("Did not find contracts for", req.symbol)
        elif count == 1:
            print ("Exactly one contract found for", req.symbol)
            contract = contractDescriptions[0].contract
            requestDetail(reqId, contract)
        else:
            print (count, "contracts found for", req.symbol, ", using first one")
            contract = contractDescriptions[0].contract
            print ("Contract: conId:%s, symbol:%s, secType:%s, currency:%s, primaryExchange:%s" % (
                contract.conId,
                contract.symbol,
                contract.secType,
                contract.currency,
                contract.primaryExchange
            ))
            requestDetail(reqId, contract)

def requestDetail(reqId, contract:Contract):
    TestApp.Instance().reqContractDetails(reqId, contract)

def resolveDetail(reqId, contract:ContractDetails):
    req = requestDict[reqId]
    if req != None:
        writeToDict(contract.summary)
        req.callback(contract.summary)

