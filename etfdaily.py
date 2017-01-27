#!/usr/bin/env python
import datetime
import etfscrape
import time

no_data_past_year, no_data_now = etfscrape.no_data_tickers()

etf_metadata = etfscrape.getMetadata()
tickers = etf_metadata.keys()
for t in no_data_past_year:
    tickers.remove(t)
for t in no_data_now:
    tickers.remove(t)

# getPreviousData(tickers)
# reverseData(tickers)


date = datetime.datetime.now().date() - datetime.timedelta(1)

# date = datetime.datetime(2017, 1, 20)
etfscrape.getTodayData(tickers, date)