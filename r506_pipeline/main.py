# %%
import pandas as pd
import numpy as np
import sqlalchemy as sa
from urllib.parse import quote
import datetime
import seaborn as sns
from datetime import datetime, timedelta
import datetime
import matplotlib.pyplot as plt
import itertools
# import statsmodels.api as sm
# from pylab import rcParams

import warnings; 
warnings.filterwarnings("ignore")
warnings.simplefilter(action="ignore",category=UserWarning)
warnings.simplefilter(action="ignore",category=FutureWarning)
warnings.filterwarnings("ignore")

plt.rcParams['figure.figsize'] = (10, 10)
plt.rcParams['grid.linestyle'] = ':'   
plt.rcParams['axes.grid'] = True

sns.set_style("whitegrid", {'axes.grid' : False})
#sns.color_palette("RdBu", n_colors=10)

np.float_ = np.float64

# %matplotlib inline
# %config InlineBackend.figure_formats = {'png', 'retina'}


# from IPython.core.interactiveshell import InteractiveShell
# InteractiveShell.ast_node_interactivity = "all"

import math
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error

from prophet import Prophet
from prophet.plot import add_changepoints_to_plot

from statsmodels.tsa.statespace.varmax import VARMAX
from statsmodels.tsa.vector_ar.var_model import VAR
from statsmodels.tsa.vector_ar.vecm import coint_johansen


print('Numpy version', np.__version__)
print('Pandas version', pd.__version__)
print('Seaborn version', sns.__version__)

# %%
cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print("current times:", cur_time)

# %%
df = pd.read_csv("./data/dataset.csv", encoding="cp874", )

# %%
df.loc[df['RACE'] == 'ไทย', 'RACE'] = '1'
df.loc[df['RACE'] == 'จีน', 'RACE'] = '2'
df.loc[df['RACE'] == 'ฮ๋องกง', 'RACE'] = '2'
df.loc[df['RACE'] == 'ไต้หวัน', 'RACE'] = '2'
df.loc[df['RACE'] == 'พม่า', 'RACE'] = '3'
df.loc[df['RACE'] == 'มาเลเซีย', 'RACE'] = '4'
df.loc[df['RACE'] == 'กัมพูชา', 'RACE'] = '5'
df.loc[df['RACE'] == 'ลาว', 'RACE'] = '6'
df.loc[df['RACE'] == 'เวียดนาม', 'RACE'] = '7'
df.loc[df['RACE'] == 'อื่นๆ', 'RACE'] = '8'

# %%
df['NADDRCODE'] = df.ADDRCODE.apply(lambda x: x[:4])
# df

# %%
# Strip any leading/trailing whitespace from the date strings
df['DATESICK'] = df['DATESICK'].str.strip()

# Convert 'DATESICK' column to datetime
df['DATESICK'] = pd.to_datetime(df['DATESICK'], format='%d/%m/%Y')

# %%
new_df = df[~df["NADDRCODE"].str.contains('96')]
# new_df

# %%
df = df[df["NADDRCODE"].str.contains('96')]

# %%
# df.head()

# %%
df['case'] = 1

# %%
# df.head()

# %%
ndf = df[['NADDRCODE','DATESICK','case']]

# %%
# Group by the relevant column(s), ensure that 'date' is part of the index

ndf.set_index(['NADDRCODE', 'DATESICK'], inplace=True)

# %%
# Resetting just the date index to work with it directly
df_case = ndf.groupby(level='NADDRCODE').apply(
    lambda x: x.droplevel('NADDRCODE').resample('W-mon').sum().interpolate(method='linear')
)

# %%
df_case = df_case.reset_index()

# %%
# df_case.info()

# %%
df_case.rename(columns={'NADDRCODE': "station","DATESICK":"date", "case" : "total_case"}, inplace=True)

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
  df_case.to_sql("cases",con,index=None, if_exists='replace')

# %%
print('uploaded cases success...')

# %%
df_case.to_csv(r"./data/new_case_p.csv", index=False)


