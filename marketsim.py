import datetime as dt
import numpy as np
import pandas as pd
import sys
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

class OrderWrapper:
    def __init__(self, date:dt.datetime, contract:Contract, order:Order):
        self.date = date
        self.contract = contract
        self.order = order

def ordersFromCsv(csvFileName):
    with open (csvFilename, 'rb') as fin:
        reader = csv.reader(fin)
        for row in reader:
            order = OrderWrapper(int(row[0]), int(row[1]), int(row[2]), row[3], row[4], int(row[5]))
            ls_orders.append(order)

            if order.s_symbol not in ls_symbols:
                ls_symbols.append(order.s_symbol)
                d_shares[order.s_symbol] = 0 

    ls_orders = sorted(ls_orders, key=lambda order: order.dt_date)
    dt_start = ls_orders[0].dt_date
    dt_end = ls_orders[-1].dt_date

