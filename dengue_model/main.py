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
# import itertools
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
def get_forecast_accuracy(y, y_hat):
    # ME
    me = (y - y_hat).sum()/len(y)

    # RMSE
    rmse = math.sqrt(mean_squared_error(y, y_hat))

    # MAPE
    mape = mean_absolute_percentage_error(y, y_hat) * 100
    
    # WAPE
    wape = (y - y_hat).__abs__().sum() / y.__abs__().sum() * 100

    print("ME: %.2f, RMSE: %.2f, MAPE: %.2f%%, WAPE: %.2f%%" % (me,rmse,mape,wape))

# %%
cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
print("current times:", cur_time)

# %%
# case = pd.read_csv("./r506_pipeline/data/df2.csv", encoding="cp874" )
# temp = pd.read_csv("./temp_pipeline/data/dataset/temp.csv", encoding="cp874")
# rain = pd.read_csv("./rain_pipeline/data/dataset/rain.csv", encoding="cp874")
# press = pd.read_csv("./pressure_pipeline/data/dataset/press.csv", encoding="cp874")
# humidity = pd.read_csv("./humidity_pipeline/data/dataset/humidity.csv", encoding="cp874")

# %%
# df = case.merge(temp,on='date',how='left')
# df = df.merge(press,on='date',how='left')
# df = df.merge(humidity,on='date',how='left')
# df = df.merge(rain,on='date',how='left')
# df.info()

# %%
uscon = pd.read_csv('./data/weekly_summary.csv',header=0)
uscon.head()

# %%
uscon['date'] = pd.to_datetime(uscon['date'])
uscon = uscon.set_index('date') 
weekly_summary = uscon.resample('W-MON').mean().interpolate(method='linear')
# weekly_summary = uscon.resample('D').sum()
# weekly_summary = weekly_summary[weekly_summary['total_case'] != 0]
# weekly_summary.head()

# %%
uscon = weekly_summary

# %%
model = VAR(uscon)
print(model.select_order())

# %%
from statsmodels.tsa.stattools import adfuller

def Augmented_Dickey_Fuller_Test_func(series , column_name):
    print (f'Results of Dickey-Fuller Test for column: {column_name}')
    dftest = adfuller(series, autolag='AIC')
    dfoutput = pd.Series(dftest[0:4], index=['Test Statistic','p-value','No Lags Used','Number of Observations Used'])
    print (dfoutput)
    if dftest[1] <= 0.05:
        print("Conclusion:====>")
        print("Reject the null hypothesis")
        print("Data is trend stationary")
    else:
        print("Conclusion:====>")
        print("Fail to reject the null hypothesis")
        print("Data is stostochastic trend")

# %%
for name, column in uscon.items():
    Augmented_Dickey_Fuller_Test_func(uscon[name],name)
    print('\n')

# %%
def cointegration_test(df): 
    res = coint_johansen(df,0,3)
    traces = res.lr1        # statistical test values
    cvts = res.cvt[:,1]     # critical value at 95% CI
    print('Column Name > Test Stat > C(95%) => Significant')
    print('----------------------------------------------')
    for col, trace, cvt in zip(df.columns, traces, cvts):
        print(col, '>', round(trace,2), ">", round(cvt,2), '=>' , trace > cvt)

# %%
cointegration_test(uscon)

# %%
split_ratio = 0.7
train_size = int(len(uscon)*split_ratio)
Y_train, Y_test = uscon[:train_size], uscon[train_size:]

# %%
best_pq = (1,0)

model = VARMAX(Y_train, order=best_pq, trend='c').fit(disp=False)
model.summary()

# %%
model.forecast(4)

# %%
plt.figure(figsize=(10,4))
plt.plot(uscon['total_case'], alpha=0.5, color='blue', label='Original')
plt.plot(model.predict()['total_case'], marker='o', linestyle='--', alpha=0.5, color='green', label='Predicted')
plt.legend()
plt.show()

get_forecast_accuracy(Y_train['total_case'], model.predict()['total_case'])

# %%
history = [y for y in Y_train.values]
train = history
predictions = list()
upper_ci = list()
lower_ci = list()


h = 4
for t in range(len(Y_test)-(h-1)):
      
  model_fit = VARMAX(train, order=best_pq).fit(disp=False)
    
  output = model_fit.get_forecast(h)
  predictions.append(output.predicted_mean[h-1])
  lower_ci.append(output.conf_int()[h-1, 0])
  upper_ci.append(output.conf_int()[h-1, 5])

  history.append(Y_test.iloc[t])
  train = history[t+1:]

# %%
plt.figure(figsize=(8,4))
plt.plot(Y_train['total_case'], label='Train set', color='red', alpha=0.6); 
plt.plot(Y_test['total_case'], label='Test set', color='blue', alpha=0.6); 

predictions_con = pd.Series([i[0] for i in predictions], index=Y_test.index[h-1:])
plt.plot(predictions_con, 'go:', label='Predicted', alpha=0.6, ms=4) 
plt.fill_between(Y_test.index[h-1:], upper_ci, lower_ci, color='#ff0066', alpha=.25)
plt.legend(loc='best')
plt.tight_layout();

get_forecast_accuracy(Y_test['total_case'][h-1:], predictions_con)

# %%
results = model.forecast(h)
results['total_case'].info()

# %%

# Define the total number of days to forecast (4 steps of 7 days each)
n_forecast_days = 4 

# Generate the forecast
forecast = model.get_forecast(steps=n_forecast_days)

# Create a date range starting from the day after the last date of the training data for 28 days
last_date = Y_test.index[-1]

# Generate the start date for each step
forecast_dates = [last_date + timedelta(weeks=1 * i) for i in range(n_forecast_days)]
# forecast_dates = pd.date_range(start=last_date + timedelta(days=1), periods=n_forecast_days)

# Convert the forecasted mean to a DataFrame and assign the date range as the index
forecast_mean = forecast.predicted_mean
forecast_mean.index = forecast_dates

# Print the forecast with dates
print(forecast_mean)
forecast_mean.info()

# %%
# Convert 'total_case' column to a Series
total_case_series = forecast_mean['total_case']
total_case_series = forecast_mean.loc[:, 'total_case']
# total_case_series = forecast_mean.loc[:, 'total_case', 'temp']
total_case_series = forecast_mean['total_case']

# %%
# Convert Series to DataFrame
total_case_series = total_case_series.reset_index()
total_case_series.columns = ['date', 'total_case']
# Set the 'date' column as the index
total_case_series.set_index('date', inplace=True)
total_case_series

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
old_case = uscon['total_case']
old_case = old_case.reset_index()

# %%
with sa.create_engine(conn_str).connect() as con:
  old_case.to_sql("allcase",con,index=None, if_exists='replace')

# %%
forecast_case = total_case_series['total_case'].astype(int)
forecast_case = forecast_case.reset_index()

# %%
with sa.create_engine(conn_str).connect() as con:
  forecast_case.to_sql("allcase",con,index=None, if_exists='append')

# %%
print('uploaded all case success...')


