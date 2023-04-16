"""

    Date: 29/01/2023
    Author: James Stanley
    Description: Main file for production run of arma_ma model.

"""


import os
import sys

import yaml
from yaml.loader import BaseLoader

import awswrangler as wr

import numpy as np
import pandas as pd

from datetime import date

from ProjectLibrary.cacheYF import cacheTicker
from ProjectLibrary.trainModels import trainARMAModel
from ProjectLibrary.positionTable import cachePositions

todays_date = str(date.today())

master_trade_df = []

with open('settings/model_settings.yaml', 'r') as stream:
    input_data = yaml.load(stream,BaseLoader)

stream.close()

#input_data = wr.s3.read_json(path="""s3://jamsyd-model-metadata/arma-ma/model-definition/arma_ma_model_definitions.json""")

for ticker in input_data:

    payload = {
        'ticker':ticker,
        'dataframe':'output',
        'order':tuple(map(int,input_data[ticker]['order'].split(","))),
        'trainDFLength':252,
        'column':'Close',
        'forecastHorizon':5,
    }

    print(payload)
    wr.s3.to_csv(cacheTicker(ticker),f"""s3://jamsyd-market-data/marketdata/yfinance/{ticker}/{todays_date}.csv""")    
    cacheTicker(ticker).to_csv(os.path.join(r'output',r'inputdata',ticker+'.csv'))
    master_trade_df.append(trainARMAModel(ticker,todays_date,payload))

# dates 
dates_df = wr.s3.read_csv(r"s3://jamsyd-model-metadata/arma-ma/training-dates/trading_dates.csv")
dates_df_lst = dates_df['train_dates'].to_list().append(todays_date)

# Saving reaining dates to s3
#wr.s3.to_csv(pd.DataFrame(np.array(dates_df['train_dates'].to_list().append(todays_date)),columns=["train_dates"]),f"""s3://jamsyd-model-metadata/arma-ma/training-dates/trading_dates.csv""")

# saving the forecast data
wr.s3.to_csv(pd.concat(master_trade_df,axis=0),f"""s3://jamsyd-forecasts/arma_ma/{todays_date}.csv""")

# saving the trading dates 
wr.s3.to_csv(pd.DataFrame(np.array([todays_date]),columns=["train_dates"]),f"""s3://jamsyd-model-metadata/arma-ma/training-dates/trading_dates.csv""")

# saving positions to s3
wr.s3.to_csv(cachePositions(todays_date,pd.read_csv(r'/home/ec2-user/docker_images/ETL/arma_ma_etl/test.csv')),f"""s3://jamsyd-positions/positions/arma_ma/{todays_date}.csv""")


