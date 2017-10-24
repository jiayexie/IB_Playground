import datetime as dt
import numpy as np
import pandas as pd
import sys
import csv
import math
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

from orders import Orders
import contracts
import historicalData

class OrderWrapper:
    def __init__(self, date:dt.datetime, symbol:str, order:Order):
        self.date = date
        self.symbol = symbol
        self.order = order

class MarketSimulator:
    def __init__(self, initValue, ordersFile, valuesFile):
        self.initValue = initValue
        self.ordersFile = ordersFile
        self.valuesFile = valuesFile
        self.orders = []
        self.symbols = []
        self.symbols_in_order = []
        self.shares = {}
        self.data = []
        self.df_data = {}

    def run(self):
        with open (self.ordersFile, 'r') as fin:
            reader = csv.reader(fin)
            for row in reader:
                date = dt.datetime(int(row[0]), int(row[1]), int(row[2]), 16)
                symbol = row[3]
                action = row[4]
                quantity = float(row[5])
                order = OrderWrapper(date, symbol, Orders.MarketOrder(action, quantity))
                self.orders.append(order)

                if order.symbol not in self.symbols:
                    self.symbols.append(order.symbol)
                    self.shares[order.symbol] = 0

        self.orders = sorted(self.orders, key=lambda order: order.date)
        startDate = self.orders[0].date
        endDate = self.orders[-1].date + dt.timedelta(days=1)

        '''
        To work around the issue that IB API does not allow querying ADJUSTED_LAST with an end date,
        we call it with no end date, and manually cancel the request when date > desired endDate
        This means we need to calculate durationStr relative to now in order to get a good start date
        '''
        duration = dt.datetime.now() - startDate
        if duration.days > 365:
            durationStr = str(math.ceil(duration.days / 365)) + " Y"
        else:
            durationStr = str(math.ceil(duration.days / 28)) + " M"

        print ("Needing", self.symbols)

        self.symbols_in_order = []
        def receive(symbol, df):
            self.onDataReceived(symbol, df)
        for symbol in self.symbols:
            historicalData.request(symbol, endDate, "ADJUSTED_LAST", durationStr, "1 day", receive)

    def onDataReceived(self, symbol, df):
        print ("Got data for ", symbol)
        self.data.append(df)
        self.symbols_in_order.append(symbol)
        if len(self.data) == len(self.symbols):
            self.df_data = pd.concat(self.data, keys=self.symbols_in_order)
            self.df_data = self.df_data['close'].unstack().T # Convert into 2D df of closing price with date as index and symbol as column
            print (self.df_data)
            print ("Got all the data we need. Simulating..")
            self.simulate()

    def simulate(self):
        cash = self.initValue
        n_days = len(self.df_data)
        n_orders = len(self.orders)
        i_order = 0

        with open(self.valuesFile, 'w') as fout:
            writer = csv.writer(fout)
            started = False
            for i in range(0, n_days):
                date = self.df_data.index[i].to_datetime() + dt.timedelta(hours=16)
                print ("Simulating ", date)
                while i_order < n_orders and self.orders[i_order].date < date:
                    i_order += 1

                while i_order < n_orders and self.orders[i_order].date == date:
                    started = True
                    order = self.orders[i_order]
                    price = self.df_data[order.symbol][i]
                    quantity = order.order.totalQuantity if order.order.action.lower() == "buy" else -order.order.totalQuantity
                    amount = price * quantity
                    cash -= amount
                    self.shares[symbol] += quantity
                    i_order += 1

                    print ("After", order.order.action, "of", order.order.totalQuantity, "cash =", cash, "shares =", self.shares[symbol])

                value = cash
                for symbol in self.symbols:
                    value += self.shares[symbol] * self.df_data[symbol][i]

                if started:
                    writer.writerow([date.year, date.month, date.day, value])


