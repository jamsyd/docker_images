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
from ProjectLibrary.trainarma import train_arma
from ProjectLibrary.arma_threshold_ma import spread_threshold

from ProjectLibrary.backtest_portfolio import test_pnl

master_trade_df = []

with open('settings/model_settings.yaml', 'r') as stream:
    input_data = yaml.load(stream,BaseLoader)

stream.close()

print(input_data)
# Need to add which column for the input data
for ticker in input_data:

    print(ticker,input_data[ticker])

    timeseries = cacheTicker(ticker)

    payload = {

        'dataframe':f"""s3://jamsyd-backtests/arma_ma/{ticker}.csv""",
        'forecastHorizon':5,
        'trainDFLength':252,
        'order':tuple(map(int,input_data[ticker]['order'].split(","))),
        'num_models':len(timeseries) - 252 - 100,
        'diff':True,
        'product':ticker,
        'column':'Close',

    }
    
    # Get the data
    wr.s3.to_csv(timeseries,f"""s3://jamsyd-backtests/arma_ma/{ticker}.csv""")    

    model = train_arma(payload)
    model.arma_model()
    
    pnl_payload = {

        'product_name':ticker,
        'forecastHorizon':5,
        'dataframe':f"""s3://jamsyd-backtests/arma_ma/{ticker}_forecasts_{str(payload['order'])}.csv""",
        'threshold':0.01,
        'reinvest':True,
        'strategy':'arma_ma',

    }
    print(pnl_payload)

    spread_threshold(pnl_payload)

#test_pnl(product_list=list(input_data.keys()), directory=f"""s3://jamsyd-backtests/arma_ma/""" )

