# %%
from fastapi import FastAPI
import pandas as pd 
import sqlalchemy as sa
import requests

# %%
app = FastAPI()

def get_all_case_data():

	conn_str = 'mysql+pymysql://user:user@dengue-db:3306/dengue'
	engine = sa.create_engine(conn_str)
	conn = engine.connect()
	cases = pd.read_sql("cases", conn)
	conn.close()

	return cases

@app.get("/")
async def root():
	return {'status': 'Online'}

@app.get("/cases")
async def root():
	cases = get_all_case_data()
	return cases.to_dict("records")

# @app.get("/with-district/{code}")
# async def with_district():
# 	ncd_screen = get_all_case_data()
# 	r = requests.get("http://hospital")
# 	data = r.json()
# 	hospital = pd.DataFrame(data)
# 	new_ncd = pd.merge(ncd_screen, hospital, on='hospcode', how='inner')
# 	return new_ncd.to_dict("records")


