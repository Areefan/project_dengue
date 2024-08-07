# %%
import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from urllib.parse import quote
import datetime
import seaborn as sns
from datetime import datetime, timedelta
import datetime
import matplotlib.pyplot as plt

import statsmodels.api as sm
from pylab import rcParams

import warnings; 
warnings.filterwarnings("ignore")

# %%
cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print("current times:", cur_time)

# %%
# Specify the directory containing the CSV files
directory = r'data/temp/'  # Use raw string to handle backslashes
# amp = [
#     'BUKT.csv',
#     'CIHO.csv',
#     'SAY001.csv',
#     'SAY002.csv',
#     'STH005.csv',
#     'STH007.csv',
#     'STH010.csv',
#     'STH011.csv',
#     'STH013.csv',
#     'STH014.csv',
#     'STH019.csv',
#     'STH021.csv',
#     'STH022.csv',
#     'STH023.csv',
#     'STH025.csv',
#     'STH026.csv'
# ]
amp = [
    'BUKT.csv',
    'STH005.csv',
    'STH007.csv',
    'STH010.csv',
    'STH011.csv',
    'STH014.csv',
    'STH019.csv',
    'STH021.csv',
    'STH022.csv',
    'STH023.csv',
    'STH025.csv',
    'STH026.csv'
]
# Read each CSV file into a DataFrame, add a new column, and store them in a list
dataframes = []
for dirpath, _, filenames in os.walk(directory):
    for file in filenames:
        if file.endswith('.csv'):
            if file in amp:
                # print(file)
                file_path = os.path.join(dirpath, file)
                try:
                    df = pd.read_csv(file_path)
                    # df['source_file'] = os.path.relpath(file_path, directory)  # Add a new column with the relative file path
                    df['source_file'] = file
                    dataframes.append(df)
                except FileNotFoundError:
                    print(f"File not found: {file_path}")
                except pd.errors.EmptyDataError:
                    print(f"File is empty: {file_path}")
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")

# Optionally, concatenate all DataFrames into a single DataFrame
if dataframes:
    all_data = pd.concat(dataframes, ignore_index=True)
    # Now 'all_data' contains all the data from the CSV files with an additional column 'source_file'
    # print(all_data)
else:
    print("No dataframes to concatenate.")

# %%
temp = pd.DataFrame(all_data)

# %%
# temp.info()

# %%
# # from datetime import datetime
# def calculate_week_number(str_date):
#     # Start of the custom week 1
#     # str_date = datetime.strptime(str_date, format).date()
#     start_of_week_1 = pd.Timestamp(year=str_date.year, month=1, day=7)
#     # print(start_of_week_1)
#     # if str_date < start_of_week_1:
#     #     start_date = datetime.date(str_date.year, 1, 7)
#     if str_date < start_of_week_1:
#         return 52  # For dates before the start of week 1
#     return ((str_date - start_of_week_1).days // 7) + 1

# %%
# Strip any leading/trailing whitespace from the date strings
temp['date'] = temp['date'].str.strip()
# temp['date'] = datetime.strptime(temp['date'], '%Y-%m-%d')
# Convert 'DATESICK' column to datetime
temp['date'] = pd.to_datetime(temp['date'], format='%Y-%m-%d')

# %%
# temp.info()

# %%
# Apply the function to get the week number
# temp['week_number'] = temp['date'].apply(calculate_week_number)

# %%
# temp.tail()

# %%
# Replace -999.0 with NaN
temp['temp'] = temp['temp'].replace(-999.0, np.nan)

# %%
# Convert 'temp' column to numeric, coercing errors to NaN
temp['temp'] = pd.to_numeric(temp['temp'], errors='coerce')

# %% [markdown]
# หาค่าเฉลี่ยของวัน

# %%
# temp.info()

# %%
temp.drop(['time'], axis=1, inplace=True)

# %%
# temp.info()

# %%
# print('Series has {} missing values'.format(temp.isna().sum()))
date_missed = temp[temp.isna()].index

# %%
# temp.info()

# %%
# Group by the relevant column(s), ensure that 'date' is part of the index

temp.set_index(['source_file', 'date'], inplace=True)

# %%
# Resetting just the date index to work with it directly
df_resampled = temp.groupby(level='source_file').apply(
    lambda x: x.droplevel('source_file').resample('W-mon').mean().interpolate(method='linear')
)

# Reassigning the group level back to the resampled data
# df_resampled['source_file'] = df_resampled.index.get_level_values(0)
# df_resampled.set_index(['source_file', df_resampled.index], inplace=True)

# %%
# Optional: Reset the index if needed
df_resampled = df_resampled.reset_index()

# %%
df_resampled.info()

# %%
# print('Series has {} missing values'.format(df_resampled.isna().sum()))
date_missed = df_resampled[df_resampled.isna()].index

# %%
# Reset the MultiIndex to work with 'date' as a regular column
df_reset = temp.reset_index()

# Set 'date' as the index for resampling
df_reset.set_index('date', inplace=True)

# Resample the DataFrame based on the 'date' index
df_resampled = df_reset.groupby('source_file').resample('W-mon').mean().interpolate(method='linear')

# Reset index to reintroduce 'group' as a column
df_resampled = df_resampled.reset_index(level=0)

# %%
df_resampled = df_resampled.reset_index()

# %%
# df_resampled[df_resampled['temp'].isna()]

# %%
newdf = df_resampled[df_resampled['date'].dt.year != 2018]

# %%
# newdf.info()

# %%
# print('Series has {} missing values'.format(newdf.isna().sum()))
date_missed = newdf[newdf.isna()].index

# %%
# newdf.info()

# %% [markdown]
# สถานีเมือง

# %%
ST9601 =  newdf.groupby('date')["temp"].mean().reset_index()

# %%
# ST9601.info()

# %%
ST9601['source_file'] = 'ST9601'

# %%
# ST9601.head()

# %%
newdf3 = pd.concat([newdf, ST9601], ignore_index=True)

# %%
# newdf3.info()

# %%
# newdf3.head()

# %%
newdf3['source_file'] = newdf3['source_file'].replace('ST9601', '9601')
newdf3['source_file'] = newdf3['source_file'].replace('STH007.csv', '9602')
newdf3['source_file'] = newdf3['source_file'].replace('STH023.csv', '9603')
newdf3['source_file'] = newdf3['source_file'].replace('STH011.csv', '9604')
newdf3['source_file'] = newdf3['source_file'].replace('STH005.csv', '9605')
newdf3['source_file'] = newdf3['source_file'].replace('STH022.csv', '9606')
newdf3['source_file'] = newdf3['source_file'].replace('STH019.csv', '9607')
newdf3['source_file'] = newdf3['source_file'].replace('STH021.csv', '9608')
newdf3['source_file'] = newdf3['source_file'].replace('STH014.csv', '9609')
newdf3['source_file'] = newdf3['source_file'].replace('STH010.csv', '9610')
newdf3['source_file'] = newdf3['source_file'].replace('BUKT.csv', '9611')
newdf3['source_file'] = newdf3['source_file'].replace('STH026.csv', '9612')
newdf3['source_file'] = newdf3['source_file'].replace('STH025.csv', '9613')

# %%
newdf3.rename(columns={'source_file': "station"}, inplace=True)

# %%
# newdf3.info()

# %%
# newdf3.head()

# %% [markdown]
# หาค่าเฉลี่ยของ week รายอำเภอ

# %%
# temp_mean_week = temp.groupby(['source_file','week_number'])["temp"].mean().reset_index(name='temp_mean')
# temp_mean_week.tail(30)

# %% [markdown]
# หาค่าเฉลี่ยของ week ทั้งจังหวัด

# %%
# temp_mean_week = temp.groupby(['date'])["temp"].mean().reset_index(name='temp_mean')
# temp_mean_week.head(10)

# %%
# # Apply the function to get the week number
# temp_mean_week['week_number'] = temp_mean_week['date'].apply(calculate_week_number)

# %%
# temp_mean_week.head()

# %%
# temp_mean_week.info()

# %%
# temp_mean_week['YEAR'] = temp_mean_week['date'].dt.year
# temp_mean_week.head()

# %%
# # Group by week number and sum cases
# #weekly_cases = df.groupby('week_number','NADDRCODE')['DATESICK'].count().reset_index()
# temp_on_week_in_year = temp_mean_week.groupby(['week_number','YEAR'])["temp_mean"].mean().reset_index(name='temp_mean')
# print(temp_on_week_in_year)

# %%
# def find_week_number(date):
#     """
#     Calculate the week number of a given date, where week 1 starts on January 7.

#     Args:
#     date (datetime.date): The date to calculate the week number for.

#     Returns:
#     int: The week number of the date.
#     """
#     # Define the starting date of week 1
#     start_date = datetime.date(date.year, 1, 7)
    
#     # If the given date is before January 7 of the same year, adjust the start date to the previous year's January 7
#     if date < start_date:
#         start_date = datetime.date(date.year - 1, 1, 7)
    
#     # Calculate the difference in days
#     days_diff = (date - start_date).days
    
#     # Calculate the week number
#     week_number = (days_diff // 7) + 1
    
#     return week_number

# def get_start_of_week(week_number, year):
#     """
#     Calculate the start date of a given week number, where week 1 starts on January 7.

#     Args:
#     week_number (int): The week number.
#     year (int): The year.

#     Returns:
#     datetime.date: The start date of the week.
#     """
#     start_date = datetime.date(year, 1, 7)
#     start_of_week = start_date + datetime.timedelta(weeks=week_number - 1)
#     return start_of_week

# def create_weeks_dataframe(start_date_str, end_date_str):
#     """
#     Create a DataFrame with weeks starting from January 7, along with their start and end dates.

#     Args:
#     start_date_str (str): The start date string for the DataFrame in 'YYYY-MM-DD' format.
#     end_date_str (str): The end date string for the DataFrame in 'YYYY-MM-DD' format.

#     Returns:
#     pd.DataFrame: A DataFrame with week numbers and their start and end dates.
#     """
#     start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date()
#     end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date()
    
#     weeks = []
#     current_date = start_date
    
#     while current_date <= end_date:
#         week_number = find_week_number(current_date)
#         start_of_week = get_start_of_week(week_number, current_date.year)
#         end_of_week = start_of_week + datetime.timedelta(days=6)
#         weeks.append({
#             'week_number': week_number,
#             'start_date': start_of_week,
#             'end_date': end_of_week
#         })
#         current_date = end_of_week + datetime.timedelta(days=1)
    
#     weeks_df = pd.DataFrame(weeks).drop_duplicates(subset=['week_number'])
#     return weeks_df

# %%
# # Example usage:
# start_date = '2024-01-07'
# end_date = '2025-01-04'
# weeks_df = create_weeks_dataframe(start_date, end_date)
# weeks_df

# %%
# temp_on_week_in_year.info()

# %%
# ndf = temp_on_week_in_year.merge(weeks_df, on='week_number', how='left')
# ndf.head()

# %%
# new_df2 = pd.merge(temp_on_week_in_year, weeks_df , on=['week_number'])
# new_df2 = new_df2[['week_number','YEAR', 'start_date', 'end_date', 'temp_mean']]
# new_df2.head()

# %%
# # Strip any leading/trailing whitespace from the date strings
# # new_df2['start_date'] = new_df2['start_date'].str.strip()

# # Convert 'DATESICK' column to datetime
# new_df2['start_date'] = pd.to_datetime(new_df2['start_date'], format='%d/%m/%Y')
# new_df2['MONTH'] = new_df2['start_date'].dt.month
# new_df2['DAY'] = new_df2['start_date'].dt.day
# new_df2.head()

# %%
# # new_df2['DATE'] = pd.to_datetime(new_df2[['YEARSICK','MONTHSICK','DAYSICK']])
# new_df2['date'] = pd.to_datetime(new_df2.YEAR.astype(str) + '/' + new_df2.MONTH.astype(str) +'/' + new_df2.DAY.astype(str))

# %%
# df2 = new_df2[['date','temp_mean']].sort_values('date')

# %%
# df2.rename(columns={'date': "date", 'temp_mean' : "temp"}, inplace=True)
# df2.info()

# %%
DIALECT = "mysql"
SQL_DRIVER = "pymysql"
USERNAME = "user"
PASSWORD = "user"
HOST = "dengue-db"
PORT = 3306
DBNAME = "dengue"

conn_str = DIALECT + "+" + SQL_DRIVER + "://" + USERNAME + ":" +quote(PASSWORD) + "@" + HOST + ":" +str(PORT) + "/" + DBNAME

# %%
with sa.create_engine(conn_str).connect() as con:
  newdf3.to_sql("temp",con,index=None, if_exists='replace')

# %%
print('uploaded temp success...')

# %%
newdf3.to_csv(r"data\dataset\temp_all.csv", index=False)


