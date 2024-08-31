from datetime import datetime
import json
import pandas as pd
import requests
import time
import emoji
import numpy as np
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import seaborn as sns

#!pip uninstall -y yfinance
#!pip install yfinance --upgrade --no-cache-dir
import yfinance as yf

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

    #shrani jih lokalno
    dividend_file_path = '/Users/Roho11/Desktop/pyscripts/dividend_paying_stocks_list.json'
    not_found_file_path = '/Users/Roho11/Desktop/pyscripts/no_dividend_stocks_list.json'

    with open(dividend_file_path, 'w') as dividend_file:
        json.dump(dividend_paying_stocks, dividend_file)

    with open(not_found_file_path, 'w') as not_found_file:
        json.dump(not_found_stocks, not_found_file)
    return dividend_paying_stocks, not_found_stocks

def get_snowball_analytics(stock_ids):
    #dobimo analiticne podatke iz snowball
    all_div_stocks_data = []
    div_stock_data      = []
    try:
        for key, value in stock_ids.items():  
            url = "https://snowball-analytics.com/extapi/api/public/asset-info/summary"

            querystring = {"assetInfoId":f"{value}"}

            payload = ""
            headers = {
                "Accept": "application/json, text/plain, */*",
                "Authorization": "***REMOVED***",
                "Sec-Fetch-Site": "same-origin",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "en",
                "Sec-Fetch-Mode": "cors",
                "Host": "snowball-analytics.com",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
                #"Referer": "https://snowball-analytics.com/public/asset/APP.US.USD",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Cookie": "_clsk=aar3ld%7C1722576463093%7C1%7C1%7Cr.clarity.ms%2Fcollect; _ga_NV375FXVKD=GS1.1.1722576360.1.1.1722576463.34.0.0; _uetsid=b4a55010508f11ef982eb311d6a98e9f; _uetvid=b4a57ec0508f11efa0b5335dea2ea0f2; storeUserId=c19a93a1-8775-4385-924b-a740e27966f8; _clck=vrwayn%7C2%7Cfnz%7C0%7C1675; _fbp=fb.1.1722576371399.190183847727114273; AMP_bb1a3b9b1e=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJiNWY0NjQwMC01NWQyLTRlMWUtOWFmNy1kMTVhZjI5NmZkZDYlMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzIyNTc2MzYxMDkxJTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTcyMjU3NjQ2MTkzNCUyQyUyMmxhc3RFdmVudElkJTIyJTNBOCUyQyUyMnBhZ2VDb3VudGVyJTIyJTNBMCU3RA==; _ga=GA1.2.1766618733.1722576363; _gid=GA1.2.1876973246.1722576365; CookieScriptConsent={'googleconsentmap':{'ad_storage':'targeting','analytics_storage':'performance','ad_user_data':'targeting','ad_personalization':'targeting','functionality_storage':'functionality','personalization_storage':'functionality','security_storage':'functionality'},'bannershown':1,'action':'accept','consenttime':1720088611,'categories':'[\'performance\',\'targeting\',\'functionality\']','GoogleACString':'2~43.46.55.61.70.83.89.93.108.117.122.124.135.143.144.147.149.159.192.196.202.211.228.230.239.259.266.286.291.311.320.322.323.327.367.371.385.394.397.407.413.415.424.430.436.445.453.486.491.494.495.522.523.540.550.559.560.568.574.576.584.587.591.737.802.803.820.821.839.864.899.904.922.931.938.979.981.985.1003.1027.1031.1040.1046.1051.1053.1067.1085.1092.1095.1097.1099.1107.1135.1143.1149.1152.1162.1166.1186.1188.1205.1215.1226.1227.1230.1252.1268.1270.1276.1284.1290.1301.1307.1312.1345.1356.1364.1375.1403.1415.1416.1421.1423.1440.1449.1455.1495.1512.1516.1525.1540.1548.1555.1558.1570.1577.1579.1583.1584.1591.1603.1616.1638.1651.1653.1659.1667.1677.1678.1682.1697.1699.1703.1712.1716.1721.1725.1732.1745.1750.1765.1782.1786.1800.1810.1825.1827.1832.1838.1840.1842.1843.1845.1859.1866.1870.1878.1880.1889.1899.1917.1929.1942.1944.1962.1963.1964.1967.1968.1969.1978.1985.1987.2003.2008.2027.2035.2039.2047.2052.2056.2064.2068.2072.2074.2088.2090.2103.2107.2109.2115.2124.2130.2133.2135.2137.2140.2147.2150.2156.2166.2177.2183.2186.2205.2213.2216.2219.2220.2222.2225.2234.2253.2279.2282.2292.2305.2309.2312.2316.2322.2325.2328.2331.2334.2335.2336.2337.2343.2354.2357.2358.2359.2370.2376.2377.2387.2400.2403.2405.2407.2411.2414.2416.2418.2425.2440.2447.2461.2462.2465.2468.2472.2477.2481.2484.2486.2488.2493.2498.2501.2510.2517.2526.2527.2532.2535.2542.2552.2563.2564.2567.2568.2569.2571.2572.2575.2577.2583.2584.2596.2604.2605.2608.2609.2610.2612.2614.2621.2628.2629.2633.2636.2642.2643.2645.2646.2650.2651.2652.2656.2657.2658.2660.2661.2669.2670.2677.2681.2684.2687.2690.2695.2698.2713.2714.2729.2739.2767.2768.2770.2772.2784.2787.2791.2792.2798.2801.2805.2812.2813.2816.2817.2821.2822.2827.2830.2831.2834.2838.2839.2844.2846.2849.2850.2852.2854.2860.2862.2863.2865.2867.2869.2873.2874.2875.2876.2878.2880.2881.2882.2883.2884.2886.2887.2888.2889.2891.2893.2894.2895.2897.2898.2900.2901.2908.2909.2916.2917.2918.2919.2920.2922.2923.2927.2929.2930.2931.2940.2941.2947.2949.2950.2956.2958.2961.2963.2964.2965.2966.2968.2973.2975.2979.2980.2981.2983.2985.2986.2987.2994.2995.2997.2999.3000.3002.3003.3005.3008.3009.3010.3012.3016.3017.3018.3019.3025.3028.3034.3038.3043.3048.3052.3053.3055.3058.3059.3063.3066.3068.3070.3073.3074.3075.3076.3077.3089.3090.3093.3094.3095.3097.3099.3100.3106.3109.3112.3117.3119.3126.3127.3128.3130.3135.3136.3145.3150.3151.3154.3155.3163.3167.3172.3173.3182.3183.3184.3185.3187.3188.3189.3190.3194.3196.3209.3210.3211.3214.3215.3217.3219.3222.3223.3225.3226.3227.3228.3230.3231.3234.3235.3236.3237.3238.3240.3244.3245.3250.3251.3253.3257.3260.3270.3272.3281.3288.3290.3292.3293.3296.3299.3300.3306.3307.3309.3314.3315.3316.3318.3324.3328.3330.3331.3531.3731.3831.3931.4131.4531.4631.4731.4831.5231.6931.7235.7831.7931.8931.9731.10231.10631.10831.11031.11531.12831.13632.13731.14237.14332.15731.16831.16931.21233.23031.24431.25731.25931.26031.26831.27731.27831.28031.28731.28831.29631.31631~dv.','CMP':'CQCuU8AQCuU8AF2ACBENA_FsAP_gAEPgAAAAKYtV_G__bWlr8X73aftkeY1P9_h77sQxBhfJE-4FzLvW_JwXx2ExNA36tqIKmRIAu3bBIQNlHJDUTVCgaogVryDMak2coTNKJ6BkiFMRO2dYCF5vmwtj-QKY5vr993dx2B-t_dr83dzyz4VHn3a5_2a0WJCdA5-tDfv9bROb-9IOd_58v4v8_F_rE2_eT1l_tevp7D9-cts7_XW-9_fff79Ln_-uB_--Cl4BJhoVEAZZEhIQaBhBAgBUFYQEUCAAAAEgaICAEwYFOwMAl1hIgBACgAGCAEAAKMgAQAACQAIRABAAUCAACAQKAAMACAYCABgYAAwAWAgEAAIDoEKYEECgWACRmREKYEIQCQQEtlQgkAQIK4QhFngQQCImCgAABIAKwABAWCwOJJASsSCBLiDaAAAgAQCCACoRSdmAIIAzZaq8WTaMrSAtHzBc9pgGAAAA.YAAAAAAAAAAA','key':'1562ae68-146f-4c80-bedc-adcb71953775'}; _gcl_au=1.1.1862266822.1722576365; _rdt_em=0000000000000000000000000000000000000000000000000000000000000001; _rdt_uuid=1722576360955.d2f6f2d4-8d74-4ed6-817b-041f62468371; app_theme=dark; logged_in=true; AMP_MKTG_bb1a3b9b1e=JTdCJTdE; dicbo_id=%7B%22dicbo_fetch%22%3A1722576365331%7D; FPID=FPID2.2.pJqOkqPeEjzpB3pLhSi1qJxWfGRTpxFamyRu861DnDc%3D.1696345368",
                "X-User-Locale": "en-GB",
                "X-Instance": "en"
                }

            response = requests.request("GET", url, data=payload, headers=headers, params=querystring)

            qsr = response.json()
            #print(qsr)
            all_div_stocks_data.append(qsr)
        return all_div_stocks_data
    except KeyError:
        print('Snowball ne vrne analiz!')

#get_us_stocks()
#split_dividend_stocks(tickers)
#get_snowball_id_from_ticker_list(dividend_paying_stocks)


 
#rabis samo ta seznam us dividend stock list with tickers   
with open('/Users/Roho11/Desktop/pyscripts/usa_div_stocks_sb_id.json', 'r') as json_file:
    stock_ids_file = json.load(json_file)


div_stocks_data   = get_snowball_analytics(stock_ids_file)

noDiv_df = pd.DataFrame(div_stocks_data)
df = pd.DataFrame(div_stocks_data)

#za prvi izvoz v excel uporabim all_df, za nadaljno analizo pa df
all_df = pd.concat([df, noDiv_df], ignore_index=True)

l_today   = datetime.today()
l_datum   = l_today.strftime("%Y-%m-%d")
all_df.to_excel(f'/Users/Roho11/Desktop/pyscripts/dividendStocksAllData/DividendStockData {l_datum}.xlsx')
#df_clean = df[['lastUpdated', 'ticker', 'currentPrice', 'sector', 'industry', 'marketCapMln', 'marketCapName', 'eps', 'forwardEPS', 'pe', 'payoutRatio', 'divYieldFWD', 'freeCashFlowPayout', 'freeCashFlowPerShare' 'divPerYearFWD', 'divGrowth1Y', 'divGrowth3Y', 'divGrowth5Y', 'divGrowthStreak', 'divFrequency']]
df_clean = df[['lastUpdated', 'ticker', 'currentPrice', 'sector', 'industry','companyDescription', 'marketCapMln', 'marketCapName', 'eps', 'forwardEPS', 'pe', 'payoutRatio','divYieldFWD', 'divPerYearFWD', 'divGrowth1Y', 'divGrowth3Y', 'divGrowth5Y', 'divGrowthStreak', 'divFrequency']]
filtered_df = df_clean[
    (df_clean['payoutRatio'].between(30, 50)) &
    (df_clean['divYieldFWD'].between(1, 3.1)) 
    #(df_clean['divGrowth1Y'] > 5.0) &
    #(df_clean['divGrowthStreak'] > 5.0)
]

#filtered_df.info()

# Dodamo FCF, FCFP in stevilo delnic v df
def add_free_cash_flow(df):
    mc          = df['marketCapMln']
    cp          = df['currentPrice']
    divyield    = df['divYieldFWD']
    num_shares  = mc / cp
    time.sleep(3)
    tick        = yf.Ticker(df['ticker'])
    cf          = tick.cash_flow
    try:
        last_value  = cf.columns[0]
        fcf         = cf[last_value]['Free Cash Flow'] 
        fcfps       = fcf / num_shares
        fcfp        = divyield / fcfps
    except IndexError:
        print(f"Za ticker {df['ticker']} so podatki o cash flow: {cf}")
        fcfp = np.nan
        fcf = np.nan
        num_shares = np.nan
  
    return fcfp, fcf, num_shares
#filtered_df['freeCashFlow'] = filtered_df['ticker'].apply(get_free_cash_flow)

filtered_df[['freeCashFlowPayout', 'freeCashFlow', 'shareNum']] = filtered_df.apply(add_free_cash_flow, axis=1).apply(pd.Series)

final_df = filtered_df[
    (filtered_df['freeCashFlowPayout'].between(0.0, 0.7))
]

# test glede dividend - vrne vse zgodovino izplacanih dividend
#Dividend Growth Rate (CAGR) = [(Divn / Div0)^(1/n)] - 1

def get_fcf_data_from_info(all_div_stocks_data):
    #dodamo v dict free cash flow podatke / lahko pride do 404 napake na Yahoo
    #moral bi df razredciti z filtri in potem samo tistim dodati podatke o free cash flow, da me yf ne bana

    all_div_stocks_data_w_cf = []
    no_cash_flow_data = []
    for div_ticker in all_div_stocks_data:
        if 'ticker' in div_ticker:
            print(div_ticker['ticker'])
            try:
                ticker = yf.Ticker(div_ticker['ticker'])
                freeCashflow = ticker.info['freeCashflow'] #pogledni naslednji blok - lahko naredis z .cashflow
                sharesOutstanding = ticker.info['sharesOutstanding']
                impliedSharesOutstanding = ticker.info['impliedSharesOutstanding'] #uporabim tega ker je rezultat pri market cap / price of stock
                floatShares = ticker.info['floatShares']
                annualDividendPerYear = div_ticker['divPerYearFWD']
                if impliedSharesOutstanding > 0:
                    freeCashFlowPerShare = (freeCashflow / impliedSharesOutstanding) 
                    div_ticker['freeCashFlowPerShare'] = freeCashFlowPerShare
                else:
                    freeCashFlowPerShare = (freeCashflow / sharesOutstanding) 
                    div_ticker['freeCashFlowPerShare'] = freeCashFlowPerShare
                freeCashFlowPayout = annualDividendPerYear / freeCashFlowPerShare
                div_ticker['freeCashFlowPayout'] = freeCashFlowPayout
            except KeyError:
                print(f"{div_ticker['ticker']} has no free cash flow data.")
                no_cash_flow_data.append(div_ticker['ticker'])
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    print(f"The requested resource ({div_ticker['ticker']}) was not found (404 error).")
        else:
            print("Missing 'ticker' key in div_ticker")
        
            
        all_div_stocks_data_w_cf.append(div_ticker)
    print(f'{len(no_cash_flow_data)} stocks have no cash flow data.')
    return 
    
#store dividend stocks on 3/5 metrics: PayoutRatio, DivYield and FreeCashFlowPayout
#output_file_path = f"/Users/Roho11/Desktop/pyscripts/dividendStocks/DividendStockData3metrics {l_datum}.xlsx"
#final_df.to_excel(output_file_path, index=False)

####################################################################################
# Inflacija
####################################################################################

#continue with other 2 metrics: DivGrowth and EPS, FCFPS trend

db_url = 'postgresql+psycopg2://roho:t5aresuV@localhost:5432/snowballStocks'
engine = create_engine(db_url)
inf_df = pd.read_sql(f"SELECT * FROM inflation ORDER BY month desc LIMIT 1", engine)
   
inflation_1_year = inf_df['inflation1y'][0]
inflation_3_year = inf_df['inflation3y'][0]
inflation_5_year = inf_df['inflation5y'][0]

divstocks_allmetrics = final_df[
    (((final_df['divGrowth1Y'] >= inflation_1_year) &
    (final_df['divGrowth3Y'] >= inflation_3_year)) |
    
    ((final_df['divGrowth1Y'] >= inflation_1_year) &
    (final_df['divGrowth5Y'] >= inflation_5_year)) |
    
    ((final_df['divGrowth3Y'] >= inflation_3_year) &
    (final_df['divGrowth5Y'] >= inflation_5_year))) &
    
    (final_df['forwardEPS'] > final_df['eps']) 
    
    #(final_df['divGrowthStreak'] > 4)
]

data = divstocks_allmetrics.copy()

####################################################################################
# Analiza in izpisi
####################################################################################

#naredimo norme
max_div_growth_years = data['divGrowthStreak'].max()
data.loc[:, 'divGrowthStreak_norm'] = data['divGrowthStreak'].apply(lambda x: x / max_div_growth_years)

max_div_yield = 3.1
data.loc[:, 'divYieldFWD_norm'] = data['divYieldFWD'].apply(lambda x : x / max_div_yield)

min_payout_ratio = 30 # 30%
data.loc[:, 'payoutRatio_norm'] = data['payoutRatio'].apply(lambda x : min_payout_ratio / x)

max_free_cfp = 0.7 # 70%
data.loc[:, 'freeCashFlowPayout_norm'] = data['freeCashFlowPayout'].apply(lambda x : 1 - x / max_free_cfp)

data.loc[:, 'Inflation_norm1'] = data['divGrowth1Y'].apply(lambda x : 1.0 if x >= inflation_1_year else x / inflation_1_year)
data.loc[:, 'Inflation_norm3'] = data['divGrowth3Y'].apply(lambda x : 1.0 if x >= inflation_3_year else x / inflation_3_year)
data.loc[:, 'Inflation_norm5'] = data['divGrowth5Y'].apply(lambda x : 1.0 if x >= inflation_5_year else x / inflation_5_year)

data.loc[:, 'Inflation_norm'] = (data['Inflation_norm1'] + data['Inflation_norm3'] + data['Inflation_norm5']) / 3

# dolocimo ponder
growth_streak_ponder = 0.2
div_yield_ponder = 0.2
growth_vs_inflation_ponder = 0.2
payout_ratio_ponder = 0.2
fcfp_ponder = 0.2
ponder_sum = growth_streak_ponder+div_yield_ponder+growth_vs_inflation_ponder+payout_ratio_ponder+fcfp_ponder

#skalkuliramo koncni order
data['Points'] = (data['divGrowthStreak_norm'] * growth_streak_ponder +
data['divYieldFWD_norm'] * div_yield_ponder +
data['Inflation_norm'] * growth_vs_inflation_ponder +
data['payoutRatio_norm'] * payout_ratio_ponder +
data['freeCashFlowPayout_norm'] * fcfp_ponder 
)

sorted_data = data.sort_values(by=['Points'], ascending=False)

output_file_path = f"/Users/Roho11/Desktop/pyscripts/dividendStocks/DividendStockDataFullFilter {l_datum}.xlsx"
sorted_data.to_excel(output_file_path, index=False)

prev_date     = l_today - pd.Timedelta(weeks=1)
prev_date_f   = l_today.strftime("%Y-%m-%d")
df_previous = pd.read_excel(f"/Users/Roho11/Desktop/pyscripts/dividendStocks/DividendStockDataFullFilter {prev_date_f}.xlsx")

list_latest_stocks = list(sorted_data['ticker'])
list_previous_stocks = list(df_previous['ticker'])

#Izpis newcomers
newcomers = []
for i in list_latest_stocks:
    if i not in list_previous_stocks:
        newcomers.append(i)
print(newcomers)        
if len(newcomers) > 0:
    if len(newcomers) == 1:
        newcomers_text = "This week's newcomer \U0001F6F0"
    else:
        newcomers_text = "This week's newcomers \U0001F6F0"       
    for newcomer in newcomers:
        # Check if newcomer exists in 'ticker' column of df_latest
        if newcomer in sorted_data['ticker'].values:
            # Retrieve rows where 'ticker' matches 'newcomer'
            matching_rows = sorted_data[sorted_data['ticker'] == newcomer]
            row = matching_rows.iloc[0]

            newcomers_text += f'''\n\n${newcomer}      
Div growth streak: {round(row['divGrowthStreak'],0)} Y
Div yield FWD: {round(row['divYieldFWD'],2)} %
Payout ratio: {round(row['payoutRatio'],2)} %
FCF Payout: {round(row['freeCashFlowPayout']*100,2)} %
1Y Div. growth: {round(row['divGrowth1Y'],2)} %
3Y Div. growth: {round(row['divGrowth3Y'],2)} %
5Y Div. growth: {round(row['divGrowth5Y'],2)} %
             '''
        else:
            print(f"Ticker {newcomer} not found in df_latest.")
else:
    newcomers_text = "No newcomers."
with open('/Users/Roho11//Desktop/pyscripts/DivStocks_tweet.txt', 'w') as file:
    print(newcomers_text, file=file)  

#izpis top dividend yield per price pick
dvYields_high_threshold = sorted_data['divYieldFWD'].quantile(0.75)
CurrentPrice_low_threshold = sorted_data['currentPrice'].quantile(0.25)

HighDivYieldLowPrice = sorted_data[(sorted_data['divYieldFWD'] > dvYields_high_threshold) & (sorted_data['currentPrice'] < CurrentPrice_low_threshold)]

if len(HighDivYieldLowPrice) > 0:
    if len(HighDivYieldLowPrice) == 1:
        
        div_text = '\nHigh dividend yield & low price pick \U0001F33E'
    else:
        div_text = '\nHigh dividend yield & low price picks \U0001F33E'
    for index, row in HighDivYieldLowPrice.iterrows():
        div_text += f'''\n\n${row['ticker']}        
Div growth streak: {round(row['divGrowthStreak'],0)} Y
Div yield FWD: {round(row['divYieldFWD'],2)} %
Payout ratio: {round(row['payoutRatio'],2)} %
FCF Payout: {round(row['freeCashFlowPayout']*100,2)} %
1Y Div. growth: {round(row['divGrowth1Y'],2)} %
3Y Div. growth: {round(row['divGrowth3Y'],2)} %
5Y Div. growth: {round(row['divGrowth5Y'],2)} %
          '''
with open('/Users/Roho11//Desktop/pyscripts/DivStocks_tweet.txt', 'a') as file:
    print(div_text, file=file)   

#izpis top 3

top3 = sorted_data.head(3)
medals = {0:'\U0001F947', 1: '\U0001F948', 2: '\U0001F949'}
i = 0
text = 'Top 3 dividend growth stocks for this week are \U0001F447'
for index, row in top3.iterrows():
    text += f'''\n\n{medals[i]} ${row['ticker']}        
Div growth streak: {round(row['divGrowthStreak'],0)} Y
Div yield FWD: {round(row['divYieldFWD'],2)} %
Payout ratio: {round(row['payoutRatio'],2)} %
FCF Payout: {round(row['freeCashFlowPayout']*100,2)} %
1Y Div. growth: {round(row['divGrowth1Y'],2)} %
3Y Div. growth: {round(row['divGrowth3Y'],2)} %
5Y Div. growth: {round(row['divGrowth5Y'],2)} %
          '''
    i += 1 
with open('/Users/Roho11//Desktop/pyscripts/DivStocks_tweet.txt', 'a') as file:
    print(text, file=file) 
     
        
####################################################################################
# vizualizacije 
####################################################################################

#scatter plot current price vs div yield
plt.figure(figsize=(8, 6))
sns.scatterplot(x='currentPrice', y='divYieldFWD', hue='ticker', data=data)
plt.title('Current Price vs Forward Dividend Yield by Ticker')
plt.xlabel('Current Price ($)')
plt.ylabel('Forward Dividend Yield')
plt.legend(title='Ticker')
plt.savefig(f'/Users/Roho11/Desktop/pyscripts/dividendStocks/Price vs Div yield scatter {l_datum}.png')

#bar plot sestava tockovanja
ticker_names = data['ticker']
points = data['Points']
other_values = data[['divGrowthStreak_norm', 'divYieldFWD_norm','Inflation_norm', 'payoutRatio_norm', 'freeCashFlowPayout_norm']]

num_tickers = len(ticker_names)
bar_width = 0.157  # Width of the nested bars
index = range(num_tickers)  # Index for x-axis positions

plt.figure(figsize=(12, 8))

# Cooler color palette
main_bar_color = 'darkgray'  # Main bar color
nested_bar_colors = ['skyblue', 'coral','gold', 'aquamarine', 'plum']  # Nested bars colors

# Plotting the main bars for Points100
plt.bar(index, points, color=main_bar_color, label='Points')

# Plotting the nested bars for other values inside each main bar
for i, col in enumerate(other_values.columns):
    #print([x + (i - 2) * bar_width for x in index])
    #print(other_values[col])

    plt.bar([x + (i - 2) * bar_width for x in index], other_values[col], bar_width,
            label=col.rstrip('_norm'), color=nested_bar_colors[i])

plt.xlabel('Ticker')
plt.ylabel('Values')
plt.title('Main Bar (Points) with Nested Bars (Metrics Normalized Values)')
plt.xticks(index, ticker_names)
plt.legend()
plt.tight_layout()
plt.savefig(f'/Users/Roho11/Desktop/pyscripts/dividendStocks/Points metrics calculation {l_datum}.png')

sector_counts = sorted_data['sector'].value_counts()

plt.figure(figsize=(12, 8))
sector_counts.plot(kind='pie',  fontsize=15, style='_classic_test_patch', autopct='%.1f%%')
plt.axis('equal')  
plt.title('Sector distribution', fontsize=18, fontweight='bold')
plt.xlabel('Sector', weight="bold", color="#000000", fontsize=14, labelpad=20)
plt.ylabel('Counts', weight="bold", color="#000000", fontsize=14, labelpad=20)
plt.savefig(f'/Users/Roho11/Desktop/pyscripts/dividendStocks/Sector distribution {l_datum}.png')

corr_df = data[['Points', 'divGrowthStreak_norm', 'divYieldFWD_norm', 'payoutRatio_norm', 'freeCashFlowPayout_norm']]
plt.figure(figsize=(16, 10))
sns.heatmap(corr_df.corr(), annot=True)
plt.savefig(f'/Users/Roho11/Desktop/pyscripts/dividendStocks/Correlation {l_datum}.png')