import sys
import datetime as dt
import numpy as np
import pandas as pd
import pdb
import csv
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

from App import TestApp

class Request:
    def __init__(self, symbol, callback):
        self.symbol = symbol
        self.callback = callback

global nextReqId, requestDict, fileName, contractDict

nextReqId = 1000
requestDict = {}
fileName = "contracts.csv"
contractDict = {}

logger = logging.getLogger('Contracts')
logger.setLevel(logging.DEBUG)

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
    logger.debug("Saving contract: symbol:%s, secType:%s, currency:%s, primaryExchange:%s, exchange:%s",
        contract.symbol,
        contract.secType,
        contract.currency,
        contract.primaryExchange,
        contract.exchange
    )
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
        if callback != None:
            callback(contract)
        return

    logger.debug("Contract not found in local cache. Requesting contract for %s", symbol)
    nextReqId = nextReqId + 1
    req = Request(symbol, callback)
    requestDict[nextReqId] = req
    TestApp.Instance().reqMatchingSymbols(nextReqId, symbol)

def resolve(reqId, contractDescriptions: ListOfContractDescription):
    req = requestDict[reqId]
    if req != None:
        count = len(contractDescriptions)
        if count == 0:
            logger.debug("Did not find contracts for %s", req.symbol)
        elif count == 1:
            logger.debug("Exactly one contract found for %s", req.symbol)
            contract = contractDescriptions[0].contract
            requestDetail(reqId, contract)
        else:
            logger.debug("%d contracts found for %s", count, req.symbol)
            for cd in contractDescriptions:
                contract = cd.contract
                if contract.currency == 'USD':
                    logger.debug("Using first USD contract")
                    logger.debug("Contract: conId:%s, symbol:%s, secType:%s, currency:%s, primaryExchange:%s",
                        contract.conId,
                        contract.symbol,
                        contract.secType,
                        contract.currency,
                        contract.primaryExchange
                    )
                    requestDetail(reqId, contract)
                    break

def requestDetail(reqId, contract:Contract):
    TestApp.Instance().reqContractDetails(reqId, contract)

def resolveDetail(reqId, contract:ContractDetails):
    req = requestDict[reqId]
    if req != None:
        writeToDict(contract.summary)
        if req.callback != None:
            req.callback(contract.summary)

