import csv
import numpy as np
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt
import datetime as dt
import pdb

import util

PORT_NAME = 'Portfolio'

class PortfolioAnalyzer:
    def __init__(self, valuesFile, baselineSymbol):
        self.valuesFile = valuesFile
        self.baselineSymbol = baselineSymbol

    def run(self):
        print ("Start analyizing..")
        with open (self.valuesFile, 'r') as fin:
            reader = csv.reader(fin)
            dates = []
            values = []
            for row in reader:
                date = dt.datetime(int(row[0]), int(row[1]), int(row[2]), 16)
                dates.append(date)
                value = float(row[3])
                values.append(value)

        self.df_price = pd.DataFrame(data=values, index=dates, columns=[PORT_NAME])
        startDate = self.df_price.index[0]
        endDate = self.df_price.index[-1]

        def receive(symbol, df):
            print ("Baseline data received")
            self.df_price[symbol] = pd.Series(data=df['close'].values, index=self.df_price.index)
            self.analyze()

        import historicalData
        historicalData.request(self.baselineSymbol, startDate, endDate, "ADJUSTED_LAST", "1 day", receive)


    def evaluate(self, na_normalized_price):
        na_rets = na_normalized_price.copy()
        util.returnize0(na_rets)
        vol = np.std(na_rets)
        daily_ret = np.mean(na_rets)
        cum_ret = na_normalized_price[-1] / na_normalized_price[0]
        sharpe = np.sqrt(252) * daily_ret / vol

        return sharpe, cum_ret, vol, daily_ret



    def analyze(self):
        na_normalized_price = self.df_price.values / self.df_price.values[0,:]

        dates = self.df_price.index.values

        plt.clf()
        plt.plot(dates, na_normalized_price)
        plt.legend(self.df_price.columns.values)
        plt.ylabel('Normalized close')
        plt.xlabel('Date')
        plt.savefig(self.valuesFile + '-analyzed.pdf', format='pdf')

        # Evaluate both fund and baseline
        sharpe, cum_ret, vol, daily_ret = self.evaluate(self.df_price[PORT_NAME])
        sharpe_b, cum_ret_b, vol_b, daily_ret_b = self.evaluate(self.df_price[self.baselineSymbol])
        print ('Details of the performance of the portfolio:')
        print ('')
        print ('Date Range: ', dates[0], 'to', dates[-1])
        print ('')
        print ('Sharpe Ratio of Fund:', sharpe)
        print ('Sharpe Ratio of ' + self.baselineSymbol + ':', sharpe_b)
        print ('')
        print ('Total Return of Fund:', cum_ret)
        print ('Total Return of ' + self.baselineSymbol + ':', cum_ret_b)
        print ('')
        print ('Standard Deviation of Fund:', vol)
        print ('Standard Deviation of ' + self.baselineSymbol + ':', vol_b)
        print ('')
        print ('Average Daily Return of Fund:', daily_ret)
        print ('Average Daily Return of ' + self.baselineSymbol + ':', daily_ret_b)
