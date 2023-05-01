"""
    Date:
    Author:
    Description:
"""

def trainARMAModel(ticker,todays_date,payload):

    import os
    import sys

    import numpy as np
    import pandas as pd

    import awswrangler as wr
    from datetime import datetime
    from statsmodels.tsa.arima.model import ARIMA

    import matplotlib.pyplot as plt

    cacheForecasts = {
        'entry_date':[],
        'entryPrice':[],
        'pointForecast':[],
        'positionType':[],
        'product_name':[ticker]*payload['forecastHorizon'],
        'strategy_name':['arma_ma']*payload['forecastHorizon'],
        'forecastDay':[i for i in range(1,1+payload['forecastHorizon'])],
        'forecastDiff':[],
        'maDiff':[],
        'percDiff':[],
        'retScore':[],

    }

    cacheMetadata = {

        'asofdate':[],
        'aic':[],
        'bic':[],
        'hqic':[],
        'mae':[],
        'mse':[],
        'forecastHorizon':[],
        'trainDFLength':[],
        'order_p':[],
        'order_q':[],
        'product_name':[],

    }

    # reading in the dataframe
    dataframe = wr.s3.read_csv(f"""s3://jamsyd-market-data/marketdata/yfinance/{ticker}/{todays_date}.csv""")

    # calculating the log difference
    diff = np.log(dataframe[payload['column']]).diff(1)

    # training a single model
    mod = ARIMA(diff[-252:], order=payload['order'])
    res = mod.fit()

    # generating forecasts
    fcast          = res.forecast(payload['forecastHorizon'])
    fcast          = (np.exp(fcast.reset_index()['predicted_mean']) - 1).cumsum() + 1
    rescaled_fcast = fcast*dataframe[payload['column']].iloc[-1]

    # store the results
    cacheForecasts['pointForecast'] = rescaled_fcast.to_list()

    # diffs
    ma_diff = dataframe[payload['column']].rolling(50).mean().diff(1).reset_index()[payload['column']].iloc[-1]
    po_diff = (rescaled_fcast.iloc[-1] - dataframe[payload['column']].iloc[-1])

    perc_return = 100*po_diff/dataframe[payload['column']].iloc[-payload['forecastHorizon']]
    ret_score   = np.abs(po_diff/np.std(np.exp(np.log(dataframe[payload['column']]).diff(1))))

    if (po_diff) > 0 and (ma_diff > 0):
        for i in range(1,1+payload['forecastHorizon']):
            cacheForecasts['positionType'].append('long')

    elif (po_diff <= 0) and (ma_diff <= 0):
        for i in range(1,1+payload['forecastHorizon']):
            cacheForecasts['positionType'].append('short')

    else:
        for i in range(1,1+payload['forecastHorizon']):
            cacheForecasts['positionType'].append('no_position')

    for i in range(1,1+payload['forecastHorizon']):
        cacheForecasts['forecastDiff'].append(po_diff)
        cacheForecasts['maDiff'].append(ma_diff)
        cacheForecasts['percDiff'].append(perc_return)
        cacheForecasts['retScore'].append(ret_score)
        cacheForecasts['entryPrice'].append(dataframe[payload['column']].iloc[-1])
        cacheForecasts['entry_date'].append(todays_date)

    cacheMetadata['asofdate'].append(todays_date)
    cacheMetadata['aic'].append(res.aic)
    cacheMetadata['bic'].append(res.bic)
    cacheMetadata['hqic'].append(res.hqic)
    cacheMetadata['mae'].append((np.mean(np.abs(res.resid))))
    cacheMetadata['mse'].append((np.mean(np.square(res.resid))))
    cacheMetadata['forecastHorizon'].append(payload['forecastHorizon'])
    cacheMetadata['trainDFLength'].append(252)
    cacheMetadata['order_p'].append(payload['order'][0])
    cacheMetadata['order_q'].append(payload['order'][1])
    cacheMetadata['product_name'].append(ticker)
    
    print(cacheMetadata)
    print(pd.DataFrame(cacheMetadata))

    wr.s3.to_csv(pd.DataFrame(cacheMetadata),f"""s3://jamsyd-model-metadata/arma-ma/metadata/{ticker}/{todays_date}.csv""")

    return pd.DataFrame(cacheForecasts)


