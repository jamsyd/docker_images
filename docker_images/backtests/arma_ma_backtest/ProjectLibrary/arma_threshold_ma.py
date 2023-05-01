import numpy as np
import pandas as pd

import awswrangler as wr

def spread_threshold(pnl_event):

    cachePnL = {

        'asofdate':[],
        'pnl':[],
        'strategy':[],
        'positionType':[],
        'position_identifier':[]
    }

    # reading in dataframe
    pnl_event['dataframe'] = wr.s3.read_csv(pnl_event['dataframe'])

    pnl_event['dataframe']['pnl']          = pnl_event['dataframe']['Close'].diff(1)
    pnl_event['dataframe']['forecast_pnl'] = pnl_event['dataframe']['pointForecast'].diff(1)

    pnl_event['dataframe'] = pnl_event['dataframe'].iloc[pnl_event['forecastHorizon']-1:].reset_index()

    i = 0
    while i < len(pnl_event['dataframe']) - pnl_event['forecastHorizon']:

        fc_diff = pnl_event['dataframe']['forecast_pnl'].iloc[i:i+pnl_event['forecastHorizon']].sum()
        ma_diff  = pnl_event['dataframe']['MA_diff_50'][i]
    
        if np.abs(fc_diff) > pnl_event['threshold'] and ((fc_diff > 0 and ma_diff > 0) or (fc_diff <= 0 and ma_diff <= 0)):

            if fc_diff > 0 and ma_diff > 0:
                for j in range(0,pnl_event['forecastHorizon']):
                    cachePnL['pnl'].append(pnl_event['dataframe']['pnl'][i + j])
                    cachePnL['positionType'].append('long')
                    cachePnL['position_identifier'].append(f"""{pnl_event['product_name']}_long_{i}""")

            if fc_diff <= 0 and ma_diff <= 0:
                for j in range(0,pnl_event['forecastHorizon']):
                    cachePnL['pnl'].append(-pnl_event['dataframe']['pnl'][i + j])
                    cachePnL['positionType'].append('short')
                    cachePnL['position_identifier'].append(f"""{pnl_event['product_name']}_short_{i}""")
        else:

            for j in range(0,pnl_event['forecastHorizon']):
                cachePnL['pnl'].append(0)
                cachePnL['positionType'].append('no_position')
                cachePnL['position_identifier'].append(f"""{pnl_event['product_name']}_no_position_{i}""")    


        i+=pnl_event['forecastHorizon']

    cachePnL['asofdate'] = pnl_event['dataframe']['asofdate'][:len(cachePnL['pnl'])].to_list()
    cachePnL['strategy'] = [pnl_event['strategy']]*len(cachePnL['pnl'])

    # # saving dataframes   
    wr.s3.to_csv(pd.DataFrame(cachePnL),f"""s3://jamsyd-backtests/arma_ma/{pnl_event['product_name']}_pnl.csv""")

