from bs4 import BeautifulSoup as Soup
import csv
import datetime
from django.utils.encoding import smart_str
import json
import numpy as np
import pandas as pd
import pandas_datareader.data as web
import os
import re
import requests
import time
import urllib

def scrapeMetadata():
    start = time.time()
    tries = 5
    kmax = 20
    s = requests.Session()
    etf_metadata = {}
    for k in range(1,kmax):
        for _ in range(tries):
            try:
                res = s.get("http://finance.yahoo.com/etf/lists?rcnt=100&page={0}".format(k))
                if res.status_code != 200:
                    raise Exception("failed request, status: {0}".format(res.status_code))
                html = smart_str(res.text)
                # with open('temp.txt', 'w') as f:
                #     f.write(html)
                soup = Soup(html, 'lxml')
                scripts = soup.findAll('script')
                script_lens = []
                for i in range(len(scripts)):
                    if scripts[i].string != None:
                        script_lens += [len(scripts[i].string)]
                    else:
                        script_lens += [0]
                # print script_lens
                # print np.argmax(script_lens)
                table = scripts[np.argmax(script_lens)].string
                if len(table) < 50000: # expect table to be around length 120k
                    raise Exception("request failed to retrieve table")
                tbody_start = table.find('<tbody>')
                tbody_end = table.find('<\/tbody>')
                table = table[tbody_start:tbody_end+9]
                table = table.replace('<tr', '\n<tr')
                table = table.replace('\/', '/')
                # print len(table)
                # with open('temp.txt', 'w') as f:
                #     f.write(table)
                soup = Soup(table, 'lxml')
                trs = soup.findAll('tr')
                print '[INFO] scraping batch {0} with {1} etfs' .format(k, len(trs))
                if k != kmax - 1 and len(trs) < 100:
                    raise Exception("request failed to retrieve full table")
                for i in range(len(trs)):
                    tds = trs[i].findAll('td')
                    if tds[1].string in etf_metadata:
                        print '[REPEATED TICKER]', tds[1].string
                        print '      old:', etf_metadata[tds[1].string]
                        print '      new:', [tds[0].string, tds[2].string, tds[3].string]
                    etf_metadata[tds[1].string] = [tds[0].string, tds[2].string, tds[3].string]
                    # for j in range(len(tds)):
                    #     print tds[j].string
                print '[INFO] scraped {0} etfs'.format(len(etf_metadata.keys()))
                time.sleep(1)
                break
            except KeyboardInterrupt:
                print "forced termination"
                return
            except Exception as e:
                print "[EXCEPTION] on batch {0} on try {1}".format(k, _+1)
                print e
                if _ == tries - 1:
                    print "[FAILED]: batch {0} failed".format(k)
                time.sleep(3)
                continue
    with open('etf_metadata.json', 'w') as f:
        json.dump(etf_metadata, f, indent=2)
    print "time to get today's data: {0}".format(time.time() - start)

def getMetadata():
    with open('etf_metadata.json', 'r') as f:
        etf_metadata = json.load(f)
    return etf_metadata

def getPreviousData(tickers, start=None, folder='data_default'):
    start_time = time.time()

    if not os.path.exists("{}".format(folder)):
        os.makedirs("{}".format(folder))

    # gets data from 1/1/2016 to 1/19/2017
    tries = 5
    s = requests.Session()

    if start == None:
        start = (1,1,2016)
    end = (2,7,2017)

    failed = []
    for i in range(len(tickers)):
        t = tickers[i]
        if (i+1)%100 == 0:
            print '[INFO] fetched {} etfs, time spent: {}'.format(i+1, time.time() - start_time)
        print '[INFO] fetching previous data for ticker {0}'.format((t, i))
        for _ in range(tries):
            try:
                url = "http://chart.finance.yahoo.com/table.csv?s={0}&a={1}&b={2}&c={3}&d={4}&e={5}&f={6}&g=d&ignore=.csv"\
                    .format(t, start[0]-1, start[1], start[2], end[0]-1, end[1], end[2])
                res = s.get(url)
                if res.status_code != 200:
                    raise Exception("failed request, status: {0}".format(res.status_code))
                with open("{}/{}.csv".format(folder, t), 'w') as f:
                    f.write(res.text)
                time.sleep(1)
                break
            except KeyboardInterrupt:
                print "forced termination"
                return
            except Exception as e:
                print "[EXCEPTION] on ticker {0} on try {1}".format(t, _+1)
                print e
                if _ == tries - 1:
                    failed += [t]
                    print "[FAILED]: ticker {0} failed".format(t)
                time.sleep(1)
                continue
    print failed
    print "[INFO] time to get today's data: {0}".format(time.time() - start_time)

def reverseData(tickers, folder='data_default'):
    for i in range(len(tickers)):
        t = tickers[i]
        df = pd.read_csv('{}/{}.csv'.format(folder, t))
        df = df.iloc[::-1]
        df.to_csv('{}/{}.csv'.format(folder, t), sep=',', index=False)
        if (i+1)%100 == 0:
            print 'finished reversing {0} ETFs'.format(i+1)

def getETFData(ticker, folder='data_default'):
    df = pd.read_csv('{}/{}.csv'.format(folder, ticker))
    return df.values

def getTodayData(tickers, date, folder='data_default'):
    log('[GETTING ETF DATA FOR {0}]'.format(date))
    start = time.time()
    tries = 3
    failed = []
    updated = 0
    for i in range(len(tickers)):
        t = tickers[i]
        if i and i%100 == 0:
            log('[INFO] got data for {0}/{1} ETFs so far today'.format(updated, i))
        for _ in range(tries):
            try:
                df = web.DataReader(t, 'yahoo', date, date)
                with open('{}/{}.csv'.format(folder, t), 'a') as f:
                    df.to_csv(f, header=False, sep=',')
                if len(df):
                    updated += 1
                break
            except KeyboardInterrupt:
                print "forced termination"
                return
            except Exception as e:
                log("[EXCEPTION] on ticker {0} on try {1}".format(t, _+1))
                print e
                if _ == tries - 1:
                    failed += [t]
                    log("[FAILED]: ticker {0} failed".format(t))
                time.sleep(1)
                continue
    if len(failed) > 0:
        log("[FAILED] tickers: {0}".format(failed))
    log("[INFO] scraped {0}/{1} tickers today".format(updated, len(tickers)))
    log("[INFO] time to get today's data: {0}\n".format(time.time() - start))

def fixData(tickers, folder='data_default'):
    for i in range(len(tickers)):
        t = tickers[i]
        df = pd.read_csv('{}/{}.csv'.format(folder, t))
        # print len(df.index)
        dropped = 0
        i = 1
        while i < len(df.index):
            if df.iloc[i]['Date'] == df.iloc[i - 1]['Date']:
                df.drop(i, inplace=True)
                dropped += 1
            else:
                i += 1
        # print len(df.index)
        df.to_csv('{}/{}.csv'.format(folder, t), index=False)
        print "[INFO] {} repeated days for {}".format(dropped, (t, i))


def no_data_tickers():
    # these have data but not through the api
    # tickers of ETFs that have a bugged previous data link
    no_data_past_year= [u'SLDR', u'CRBQ', u'ERGF', u'IFAS', u'724776', u'A1JTWB', u'TRNM', u'MOGLC', u'EMHD', u'TWTI', u'SSTU.TO', u'ASDR', u'AMPS', u'SLOW', u'ESR', u'A1JRLN', u'EMDI', u'EWHS', u'CNSF', u'HHDG', u'PXN', u'PUTX', u'RUDR', u'HEGE', u'AAIT', u'CHNB', u'NDQ', u'DSXJ', u'A1CSX3', u'ENGN', u'ETFSPHYSICAL', u'6KSA', u'AXJS', u'OSMS', u'DBSP', u'MONY', u'EVAL', u'CRUD', u'CHLC', u'BXUC', u'BXUB', u'EGRW', u'ALTL', u'GERJ', u'KBWI', u'LATM', u'TCHF', u'DSTJ']
    no_data_now = [u'LSC', u'SBEU', u'BSCG', u'QTWN', u'FXC.MX', u'RWV', u'FCFI', u'HREX', u'XLFS', u'QMEX', u'ROLA', u'DWTI', u'QKOR']
    return no_data_past_year, no_data_now

def log(message):
    with open('daily.log', 'a') as f:
        f.write(message + "\n")
    print message


if __name__ == '__main__':
    no_data_past_year, no_data_now = no_data_tickers()

    # scrapeMetadata()

    etf_metadata = getMetadata()
    tickers = etf_metadata.keys()
    for t in no_data_past_year:
        tickers.remove(t)
    for t in no_data_now:
        tickers.remove(t)

    getPreviousData(tickers, start=(1,1,2012), folder='data_5year')
    reverseData(tickers, folder='data_5year')


    # date = datetime.datetime.now().date()

    # date = datetime.datetime(2017, 1, 20)
    # getTodayData(tickers, date)
    # fixData(tickers, folder='data')



