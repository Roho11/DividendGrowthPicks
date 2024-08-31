### INACTIVE
# 1. Get all US stock tickers from advfn.com
# 2. With yfinace divide the stock tickers into dividend paying and not dividend paying (JSON files)
# 3. Get Snowall IDs based on stock ticker (2 versions) - CURRENTLY DOESN'T WORK

import json
import pandas as pd
import requests
import yfinance as yf
import os

def get_snowball_id_from_ticker_list(list_of_dividend_paying_stocks):
    
    snowball_id_dict = {}
    snowball_not_found = []

    for ticker in list_of_dividend_paying_stocks:
        try:
            url = f"https://snowball-analytics.com/_next/data/XEbEHDfSl-Qdfo-dMraex/public/asset/{ticker}.US.USD.json"

            querystring = {"slug":f"{ticker}.US.USD"}

            payload = ""
            headers = {
                "authority": "snowball-analytics.com",
                "accept": "*/*",
                "accept-language": "sl-SI,sl;q=0.9,en-GB;q=0.8,en;q=0.7",
    
                "referer": f"https://snowball-analytics.com/public/asset/{ticker}.US.USD",

                "sec-ch-ua-mobile": "?0",

                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
                "x-nextjs-data": "1"
                }

            response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
            if response.status_code == 404:
                print(f"Resource not found for {ticker}.")
                snowball_not_found.append(ticker)
                continue 
        
            json = response.json()
            assetInfoId = json['pageProps']['asset']['assetInfoId']
            snowball_id_dict[ticker] = assetInfoId 
        except KeyError:
            snowball_not_found.append(ticker)
            print(f'{ticker} not found on Snowball.')
    return snowball_id_dict, snowball_not_found
    
    print(f'{len(snowball_id_dict)} stock IDs found!')
    print(f'{len(snowball_not_found)} stocks were not able to be found on Snowball.')
    
def get_snowball_id_from_ticker_list_v2(list_of_dividend_paying_stocks):
    snowball_id_dict   = {}
    snowball_not_found = []

    for ticker in list_of_dividend_paying_stocks:
        try:
            url = "https://snowball-analytics.com/extapi/api/assets/search"

            querystring = {"search":f"{ticker}","assetTypes[]":["1","2","3","5","8","7"]}

            payload = ""
            headers = {"User-Agent": "Insomnia/2023.5.7"}

            response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

            #print(response.text)

            if response.status_code == 404:
                print(f"Resource not found (404) for {ticker}.")
                snowball_not_found.append(ticker)
                continue 
            
            json = response.json()
            assetInfoId = json[0]['pageProps']['asset']['assetInfoId'] # v tem primeru json vrne list zato izberemo 0
            snowball_id_dict[ticker] = assetInfoId 
        except KeyError:
            snowball_not_found.append(ticker)
            print(f'{ticker} not found (KeyError) on Snowball.')
    return snowball_id_dict, snowball_not_found
            
        
    print(f'{len(snowball_id_dict)} stock IDs found!')
    print(f'{len(snowball_not_found)} stocks were not able to be found on Snowball.')
    
def get_us_stocks():

    #dobimo vse ameriske delnice v DataFrame tickers
    #vergla 50min

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}

    exchanges = {'nyse': 'newyorkstockexchange', 'nasdaq': 'nasdaq', 'amex': 'americanstockexchange'} #so to vse ameriške borze?

    abeceda = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '0']

    tickers = pd.DataFrame()

    for key, value in exchanges.items():
        for char in abeceda:
            response = requests.get(f'https://www.advfn.com/{key}/{value}.asp?companies={char}', headers=headers)
            if response.status_code == 200:
                all_symbols = pd.read_html(response.text)
            else:
                print(f"Failed to retrieve data. Status code: {response.status_code}")
            df = pd.DataFrame(all_symbols[4]).drop([0,1]).drop(columns=2).reset_index(drop=True)
            tickers = pd.concat([tickers, df], ignore_index=True)
    
    print(f'Scraped {len(tickers)} rows of stocks.')
    return tickers

def split_dividend_stocks(tickers):
    ##### Izlocim delnice, ki ne izplacujejo dividend - not_found_stocks bi lahko vseeno kje preveril
    ##### to vergla cca 1h 

    #iz DataFramea pretvorimo samo tickerje v list all_stocks_list
    all_stocks_list = tickers[1].tolist()

    dividend_paying_stocks = []
    not_found_stocks       = []

    for symbol in all_stocks_list:
        try:
            stock = yf.Ticker(symbol)
            info = stock.info
            #print(info)
            if 'dividendRate' in info and info['dividendRate'] > 0:
                dividend_paying_stocks.append(symbol)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                #print(f"The requested resource ({symbol}) was not found (404 error).")
                not_found_stocks.append(symbol)

    print(f'Found {len(dividend_paying_stocks)} dividend stocks.')
    print(f'There were {len(not_found_stocks)} stocks with no dividend data!')

    dividend_file_path = os.path.join(os.path.dirname(__file__), "dividend_paying_stocks_list.json")
    not_found_file_path = os.path.join(os.path.dirname(__file__), "no_dividend_stocks_list.json")

    with open(dividend_file_path, 'w') as dividend_file:
        json.dump(dividend_paying_stocks, dividend_file)

    with open(not_found_file_path, 'w') as not_found_file:
        json.dump(not_found_stocks, not_found_file)
    return dividend_paying_stocks, not_found_stocks


#get_us_stocks()
#split_dividend_stocks(tickers)
#get_snowball_id_from_ticker_list(dividend_paying_stocks)