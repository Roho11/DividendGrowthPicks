from config import db_url
import pandas as pd
from sqlalchemy import create_engine, text
import os

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

last_results_file = os.path.join(dividendStocksResults_filepath,valid_results_files[0])
last_alldata_file = os.path.join(dividendStocksAllData_filepath,valid_alldata_files[0])

results_df = pd.read_excel(last_results_file)
alldata_df = pd.read_excel(last_alldata_file, index_col='Unnamed: 0')

#Results data to SQL
engine        = create_engine(db_url)
results_table = 'results'
results_df.to_sql(results_table, engine, if_exists='append', index=False)

#Fundamentals (all_data) to SQL
alldata_temp_table = 'fundamentals_temp'
alldata_df.to_sql(alldata_temp_table, engine, if_exists='replace', index=False)

#Manage duplicates
query = """
    INSERT INTO fundamentals ("lastUpdated", sector, industry, "gicSector", "gicIndustry", "marketCapMln", "marketCapMlnUSD", "marketCapName", eps, beta, pe, ebitda, "forwardPE", "forwardEPS", "payoutRatio", "divStreak", "divGrowth1Y", "divGrowth3Y", "divSafetyScore", "divSafetyScoreLocked", "lastQuaterEPSSurprise", "lastQuaterRevenueSurprise", "nextQuaterEPSEstimate", "nextQuaterRevenueEstimate", "nextEarningsReportDate", "lastEarningsReportDate", "nextEarningsReportReportingDate", "lastEarningsReportReportingDate", "revenueGrowthYoY", "netIncomeGrowthYoY", "cashFlowGrowthYoY", "companyDescription", ticker, "ignoreCurrency", nominal, "currentNominal", "realNominal", "maturityDate", "offerDate", "effectiveYield", "yieldCoupon", "currentYield", "modifCurrentYield", "yieldToMaturity", "yieldToMaturityPortfolio", "effectiveYieldPortfolio", nkd, "couponsSumm", duration, "listingLevel", "countryISO", isin, "bondType", "issueDate", term, "dividendTax", "lotSize", "expenseRatio", "assetInfoId", currency, "currentPrice", "prevClosePrice", "lastDayGainsAmount", "lastDayGainsPercent", "divCurrency", "nextDividendDate", "exDividendDate", "nextDividendPerShare", "divYieldFWD", "isDivYieldTTM", "divPerYearFWD", "divPaidTTM", "divGrowth5Y", "divGrowthStreak", "divFrequency", "financialsCurrency", "marketCapCurrency", type, title, status, "traceId")
    SELECT "lastUpdated", sector, industry, "gicSector", "gicIndustry", "marketCapMln", "marketCapMlnUSD", "marketCapName", eps, beta, pe, ebitda, "forwardPE", "forwardEPS", "payoutRatio", "divStreak", "divGrowth1Y", "divGrowth3Y", "divSafetyScore", "divSafetyScoreLocked", "lastQuaterEPSSurprise", "lastQuaterRevenueSurprise", "nextQuaterEPSEstimate", "nextQuaterRevenueEstimate", "nextEarningsReportDate", "lastEarningsReportDate", "nextEarningsReportReportingDate", "lastEarningsReportReportingDate", "revenueGrowthYoY", "netIncomeGrowthYoY", "cashFlowGrowthYoY", "companyDescription", ticker, "ignoreCurrency", nominal, "currentNominal", "realNominal", "maturityDate", "offerDate", "effectiveYield", "yieldCoupon", "currentYield", "modifCurrentYield", "yieldToMaturity", "yieldToMaturityPortfolio", "effectiveYieldPortfolio", nkd, "couponsSumm", duration, "listingLevel", "countryISO", isin, "bondType", "issueDate", term, "dividendTax", "lotSize", "expenseRatio", "assetInfoId", currency, "currentPrice", "prevClosePrice", "lastDayGainsAmount", "lastDayGainsPercent", "divCurrency", "nextDividendDate", "exDividendDate", "nextDividendPerShare", "divYieldFWD", "isDivYieldTTM", "divPerYearFWD", "divPaidTTM", "divGrowth5Y", "divGrowthStreak", "divFrequency", "financialsCurrency", "marketCapCurrency", type, title, status, "traceId" 
    FROM fundamentals_temp
    ON CONFLICT (ticker, "lastUpdated")
    DO NOTHING;
"""

with engine.begin() as connection:
    connection.execute(text(query))

print("Data inserted successfully from the temp table.")



