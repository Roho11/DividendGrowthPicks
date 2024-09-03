from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
from config import db_url

url = 'https://www.usinflationcalculator.com/inflation/consumer-price-index-and-annual-percent-changes-from-1913-to-2008/'
dfs = pd.read_html(url)
df = dfs[0]
df.drop(df.columns[[14, 15, 16]], axis=1, inplace=True)
df_edited = df
df_edited.columns = df_edited.iloc[1]
df_edited = df_edited.drop(0)
df_edited = df_edited.reset_index(drop=True)
df_edited = df_edited.drop(0)
df_edited.set_index('Year', inplace=True)

today = datetime.now()
this_month = today.month
this_year = today.year
obodbje = ''
#vedno gledamo za prejsni mesec
if this_month == 1: 
    this_month = 12
    this_year -= 1
else:
    this_month -= 1

def inflation_years(df, year, month, step):
    global obdobje
    month_names = {1: 'Jan', 2: 'Feb',3: 'Mar', 4: 'Apr', 5: 'May', 6: 'June', 7: 'July', 8: 'Aug', 9: 'Sep', 10: 'Oct', 11: 'Nov', 12: 'Dec'}
    year_str = str(year)
    current_cpi = df_edited.loc[year_str,month_names[month]]
    
    if 'Avail.' in current_cpi:
        #pomeni, da se podatkov za prejsnji mesec ni - gledamo predprejsnje obdobje
        if month == 1:
            month = 12
            year -= 1
            year_str = str(year)
            current_cpi = df_edited.loc[year_str,month_names[month]]
        else:
            month -= 1
            current_cpi = df_edited.loc[year_str,month_names[month]]
    prev_year     = year - step
    prev_year_str = str(prev_year)
    prev_cpi      =  (df_edited.loc[prev_year_str,month_names[month]])
    percentage_change = round(((float(current_cpi) - float(prev_cpi)) / float(prev_cpi)) * 100,2)
    print(f'Inflation data for {month_names[month]}: {percentage_change}%')
    if len(str(month)) == 1:
        month_dvomestno = f'0{month}'
    else:
        month_dvomestno = month
    obdobje = f'{year_str}-{month_dvomestno}'
    
    return percentage_change
 
inflation_1_year = inflation_years(df_edited, this_year, this_month, 1)
inflation_3_year = inflation_years(df_edited, this_year, this_month, 3)
inflation_5_year = inflation_years(df_edited, this_year, this_month, 5)

#zapisemo novo obdobje v tabelo inflation
inf_data_df = pd.DataFrame({
    # columns must be the same name as in the db
    'month': [obdobje], 
    'inflation1y': [inflation_1_year],
    'inflation3y': [inflation_3_year],
    'inflation5y': [inflation_5_year]
})

engine = create_engine(db_url)
result_db_df = pd.read_sql(f"SELECT * FROM inflation where month = '{obdobje}'", engine)

table_name = 'inflation'
if len(result_db_df) == 0:
    inf_data_df.to_sql(table_name, engine, if_exists='append', index=False)