# %%
import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from urllib.parse import quote
import datetime
from datetime import datetime, timedelta
import datetime

from pylab import rcParams

import warnings; 
warnings.filterwarnings("ignore")

# %%
# Specify the directory containing the CSV files
directory = r'data/'  # Use raw string to handle backslashes
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
press = pd.DataFrame(all_data)

# %%
# press.info()

# %%
# press.tail()

# %%
# Strip any leading/trailing whitespace from the date strings
press['date'] = press['date'].str.strip()
# temp['date'] = datetime.strptime(temp['date'], '%Y-%m-%d')
# Convert 'DATESICK' column to datetime
press['date'] = pd.to_datetime(press['date'], format='%Y-%m-%d')

# %%
# Replace -999.0 with NaN
press['press'] = press['press'].replace(-999.0, np.nan)

# %%
# Convert 'temp' column to numeric, coercing errors to NaN
press['press'] = pd.to_numeric(press['press'], errors='coerce')

# %%
press.drop(['time'], axis=1, inplace=True)

# %%
# press.info()

# %%
# press.head()

# %%
# print('Series has {} missing values'.format(press.isna().sum()))
date_missed = press[press.isna()].index

# %%
# Group by the relevant column(s), ensure that 'date' is part of the index
press.set_index(['source_file', 'date'], inplace=True)

# %%
# Resetting just the date index to work with it directly
df_resampled = press.groupby(level='source_file').apply(
    lambda x: x.droplevel('source_file').resample('W-mon').mean().interpolate(method='linear')
)

# Reassigning the group level back to the resampled data
# df_resampled['source_file'] = df_resampled.index.get_level_values(0)
# df_resampled.set_index(['source_file', df_resampled.index], inplace=True)

# %%
# Optional: Reset the index if needed
df_resampled = df_resampled.reset_index()

# %%
# print('Series has {} missing values'.format(df_resampled.isna().sum()))
date_missed = df_resampled[df_resampled.isna()].index

# %%
# Reset the MultiIndex to work with 'date' as a regular column
df_reset = press.reset_index()

# Set 'date' as the index for resampling
df_reset.set_index('date', inplace=True)

# Resample the DataFrame based on the 'date' index
df_resampled = df_reset.groupby('source_file').resample('W-mon').mean().interpolate(method='linear')

# Reset index to reintroduce 'group' as a column
df_resampled = df_resampled.reset_index(level=0)

# %%
# print('Series has {} missing values'.format(df_resampled.isna().sum()))
date_missed = df_resampled[df_resampled.isna()].index

# %%
newdf = df_resampled

# %%
# newdf.info()

# %%
newdf = newdf.reset_index()

# %%
ST9601 =  newdf.groupby('date')["press"].mean().reset_index()

# %%
ST9601.info()

# %%
ST9601['source_file'] = 'ST9601'

# %%
ST9601.head()

# %%
newdf3 = pd.concat([newdf, ST9601], ignore_index=True)

# %%
# newdf3.info()

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
  newdf3.to_sql("pressure",con,index=None, if_exists='replace')

# %%
print('uploaded pressure success...')

# %%
newdf3.to_csv(r"data/dataset/press_all.csv", index=False)


