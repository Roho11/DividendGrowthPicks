from config import db_url
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime

print("Starting the results2SQL script...")

dividendStocksResults_filepath = os.path.join(os.path.dirname(__file__), "dividendStocksResults") 
dividendStocksAllData_filepath = os.path.join(os.path.dirname(__file__), "dividendStocksAllData") 

results_files = os.listdir(dividendStocksResults_filepath)
alldata_files = os.listdir(dividendStocksAllData_filepath)

valid_results_files = []
for res in results_files:
    if res.startswith('DividendStockResults'):
        valid_results_files.append(res)
        
valid_alldata_files = []
for ad in alldata_files:
    if ad.startswith('DividendStockData'):
        valid_alldata_files.append(ad)
        
valid_results_files.sort(reverse=True)
valid_alldata_files.sort(reverse=True)

results_date = str(valid_results_files[0]).split(' ')[1].split('.')[0]
alldata_date = str(valid_alldata_files[0]).split(' ')[1].split('.')[0]
today_date   = str(datetime.today()).split(' ')[0]

if results_date == today_date and alldata_date == today_date:
    print('Both excel files from today found, inserting to database.')

    last_results_file = os.path.join(dividendStocksResults_filepath,valid_results_files[0])
    last_alldata_file = os.path.join(dividendStocksAllData_filepath,valid_alldata_files[0])

    results_df = pd.read_excel(last_results_file)
    alldata_df = pd.read_excel(last_alldata_file, index_col='Unnamed: 0')

    #PostgreSQL requires lowercase or it sets column names into " "
    results_df.columns = results_df.columns.str.lower()
    alldata_df.columns = alldata_df.columns.str.lower()

    #Results data to SQL
    engine        = create_engine(db_url)
    results_table = 'results'
    results_df.to_sql(results_table, engine, if_exists='append', index=False)

    print("Results data inserted successfully.")

    #Storing data into last_results

    query = """
        TRUNCATE TABLE last_results
    """

    with engine.begin() as connection:
        result = connection.execute(text(query))

    results_table = 'last_results'
    results_df.to_sql(results_table, engine, if_exists='append', index=False)

    print("last_results data inserted successfully.")

    # Fundamentals (all_data) to SQL
    alldata_temp_table = 'fundamentals_temp'
    alldata_df.to_sql(alldata_temp_table, engine, if_exists='replace', index=False)

    # Manage duplicates and insert data
    query = """
        INSERT INTO fundamentals (lastupdated, sector, industry, gicsector, gicindustry, marketcapmln, marketcapmlnusd, marketcapname, eps, beta, pe, ebitda, forwardpe, forwardeps, payoutratio, divstreak, divgrowth1y, divgrowth3y, divsafetyscore, divsafetyscorelocked, lastquaterepssurprise, lastquaterrevenuesurprise, nextquaterepsestimate, nextquaterrevenueestimate, nextearningsreportdate, lastearningsreportdate, nextearningsreportreportingdate, lastearningsreportreportingdate, revenuegrowthyoy, netincomegrowthyoy, cashflowgrowthyoy, companydescription, ticker, ignorecurrency, nominal, currentnominal, realnominal, maturitydate, offerdate, effectiveyield, yieldcoupon, currentyield, modifcurrentyield, yieldtomaturity, yieldtomaturityportfolio, effectiveyieldportfolio, nkd, couponssumm, duration, listinglevel, countryiso, isin, bondtype, issuedate, term, dividendtax, lotsize, expenseratio, assetinfoid, currency, currentprice, prevcloseprice, lastdaygainsamount, lastdaygainspercent, divcurrency, nextdividenddate, exdividenddate, nextdividendpershare, divyieldfwd, isdivyieldttm, divperyearfwd, divpaidttm, divgrowth5y, divgrowthstreak, divfrequency, financialscurrency, marketcapcurrency, type, title, status, traceid)
        SELECT lastupdated, sector, industry, gicsector, gicindustry, marketcapmln, marketcapmlnusd, marketcapname, eps, beta, pe, ebitda, forwardpe, forwardeps, payoutratio, divstreak, divgrowth1y, divgrowth3y, divsafetyscore, divsafetyscorelocked, lastquaterepssurprise, lastquaterrevenuesurprise, nextquaterepsestimate, nextquaterrevenueestimate, nextearningsreportdate, lastearningsreportdate, nextearningsreportreportingdate, lastearningsreportreportingdate, revenuegrowthyoy, netincomegrowthyoy, cashflowgrowthyoy, companydescription, ticker, ignorecurrency, nominal, currentnominal, realnominal, maturitydate, offerdate, effectiveyield, yieldcoupon, currentyield, modifcurrentyield, yieldtomaturity, yieldtomaturityportfolio, effectiveyieldportfolio, nkd, couponssumm, duration, listinglevel, countryiso, isin, bondtype, issuedate, term, dividendtax, lotsize, expenseratio, assetinfoid, currency, currentprice, prevcloseprice, lastdaygainsamount, lastdaygainspercent, divcurrency, nextdividenddate, exdividenddate, nextdividendpershare, divyieldfwd, isdivyieldttm, divperyearfwd, divpaidttm, divgrowth5y, divgrowthstreak, divfrequency, financialscurrency, marketcapcurrency, type, title, status, traceid
        FROM fundamentals_temp
        ON CONFLICT (ticker, lastupdated)
        DO NOTHING;
    """

    with engine.begin() as connection:
        result = connection.execute(text(query))

    # Get number of rows inserted
    print(f"Number of rows inserted into 'fundamentals': {result.rowcount}")

else:
    print('Could not find todays results OR alldata excel file.')
