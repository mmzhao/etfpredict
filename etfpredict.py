import etfscrape
import numpy as np



no_data_past_year, no_data_now = etfscrape.no_data_tickers()

etf_metadata = etfscrape.getMetadata()
tickers = etf_metadata.keys()
for t in no_data_past_year:
    tickers.remove(t)
for t in no_data_now:
    tickers.remove(t)


spy = etfscrape.getETFData('SPY', folder='data_5year')
print spy.shape
print spy[0]
print spy[-1]