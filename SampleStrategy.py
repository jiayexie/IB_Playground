import pandas as pd
import numpy as np
import copy
import csv
import EventProfiler as ep
import logging

logger = logging.getLogger('SampleStrategy')
logger.setLevel(logging.DEBUG)

class SampleStrategy:

    def __init__(self, df_price: pd.DataFrame, baselineSymbol: str, profilerOutFile: str, ordersOutFile: str):
        self.df_price = df_price
        self.baselineSymbol = baselineSymbol
        self.profilerOutFile = profilerOutFile
        self.ordersOutFile = ordersOutFile

    def findEventsAndGenerateOrders(self):
        ts_market = self.df_price[self.baselineSymbol]

        # Creating an empty dataframe
        df_events = copy.deepcopy(self.df_price)
        df_events = df_events * np.NAN

        # Time stamps for the event range
        ldt_timestamps = self.df_price.index

        ls_symbols = self.df_price.columns.values

        n_days = len(ldt_timestamps)

        with open(self.ordersOutFile, 'w') as fout:
            writer = csv.writer(fout)
            for s_sym in ls_symbols:
                for i in range(1, n_days):
                    # Calculating the returns for this timestamp
                    f_symprice_today = self.df_price[s_sym].ix[ldt_timestamps[i]]
                    f_symprice_yest = self.df_price[s_sym].ix[ldt_timestamps[i - 1]]
                    # f_marketprice_today = ts_market.ix[ldt_timestamps[i]]
                    # f_marketprice_yest = ts_market.ix[ldt_timestamps[i - 1]]
                    f_symreturn_today = (f_symprice_today / f_symprice_yest) - 1
                    # f_marketreturn_today = (f_marketprice_today / f_marketprice_yest) - 1

                    # Event is found if the symbol dropped at least 5% today
                    if f_symreturn_today <= -0.05:
                        df_events[s_sym].ix[ldt_timestamps[i]] = 1
                        today = ldt_timestamps[i]
                        writer.writerow([today.year, today.month, today.day, s_sym, 'Buy', 100])
                        sellday = ldt_timestamps[min(i+5, n_days-1)]
                        writer.writerow([sellday.year, sellday.month, sellday.day, s_sym, 'Sell', 100])

        return df_events

    def studyEvents(self):
        logger.debug("Finding events")
        df_events = self.findEventsAndGenerateOrders()
        logger.debug("Creating study")
        ep.eventprofiler(df_events, self.df_price, i_lookback=20, i_lookforward=20,
                s_filename=self.profilerOutFile, b_market_neutral=True, b_errorbars=True,
                s_market_sym='SPY')

