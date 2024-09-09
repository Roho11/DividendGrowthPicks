#Dividend Growth Picks

Is A Python project that analyzes 2000+ U.S. dividend-paying stocks using five key financial metrics. The results are automatically posted to a dedicated Twitter (X) account.

The indicators data is retrieved from https://snowball-analytics.com for 2067 U.S. dividend paying stocks. 

Then each stock is filtered and valuated based on 5 fundamental metrics:

Payout Ratio
Dividend Growth Rate
Dividend Yield
Free Cash Flow Payout
Earnings per share/Free Cashflow per share

The best stocks that match the metrics are stored in a txt file and later posted on X account: https://x.com/DivGrowthPicks

Results also contain some visualizations. 

##File explanation

The files should be ran in this order:

0. create_tables.sql - creates the required tables

1. prepareJSONFIles.py - Gets all U.S. stock tickers, checks if the stock pays dividend or not (divides them into two JSON files) and lastly checks for the Snowball ID (The last part doesn't work currently so we don't run this file and just stick with the current 2067 IDs we got) -- NOT WORKING
2. inflationData2SQL.py - For the Dividend Yield metric we need to calculate the 1Y, 3Y and 5Y inflation rate and store it in our database.
3. dividendStocks.py - Analyses the data, returns the results, creates visualizations.
4. divResults2SQL.py - Stores the retrieved and results data into the database.

You would also need to have the database string connection stored in config.py

##Setup




