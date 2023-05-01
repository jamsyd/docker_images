

import os
import glob
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import awswrangler as wr

def test_pnl(product_list,directory):

    # loop over the list of csv files
#    product_list  = ["DAX","ECH","EEM","EPOL","EPU","EWJ","EWM","EWT","EWZ","MCHI","SPY","THD"]
    merge_lst = []

    csv_files = read_objects(bucket_name='jamsyd-backtests',prefix='arma_ma')    

    for product in product_list:
    
        for f in csv_files:

            if product+"_pnl" in f:
                print(f)
                #print(f.split("/")[-1])
                f = str("s3://jamsyd-backtests/arma_ma/"+f.split("/")[-1])
            
                print(f)
                pnl_df = wr.s3.read_csv(f)      
                pnl_df['product_name'] = [product]*len(pnl_df)     

                close_df = wr.s3.read_csv(directory+r"/"+product+'.csv')

                result = pd.merge(close_df[['Date','Close','Dividends','Stock Splits']], 
                                pnl_df[['asofdate', 'pnl']],
                                left_on='Date',
                                right_on='asofdate',
                                how='inner')

                merge_lst.append(result)

    merge_df = pd.concat(merge_lst,axis=0)

    portfolio_value = 100000
    position_size   = 0.083

    scale = []

    merge_df = merge_df.reset_index()

    i = 0
    while i < len(merge_df):

        scale.append(np.floor(portfolio_value*position_size/merge_df['Close'][i]))

        i+=1

    merge_df['scale']        = scale
    merge_df['rescaled_pnl'] = merge_df['scale']*merge_df['pnl']

    merge_df['cumulative_pnl'] = merge_df.groupby(by='Date')['rescaled_pnl'].sum()

    wr.s3.to_csv(merge_df,f"""s3://jamsyd-backtests/arma_ma/arma_ma_constant_portfolio.csv""")

def read_objects(bucket_name,prefix):

    import boto3
    import pandas as pd
    import io

    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    prefix_objs = bucket.objects.filter(Prefix=prefix)

    prefix_df = []
    key_list  = []
    for obj in prefix_objs:
        key = obj.key
        key_list.append(key)

    return key_list



