import os
import yaml
from yaml.loader import BaseLoader

import numpy as np
import awswrangler as wr

def getScale(product_name,todays_date):

    df = wr.s3.read_csv(f"""s3://jamsyd-market-data/marketdata/yfinance/{product_name}/{todays_date}.csv""") 

    with open('/home/ec2-user/docker_images/ETL/arma_ma_etl/settings/position_settings.yaml', 'r') as stream:
        input_data = yaml.load(stream,BaseLoader)

    stream.close()

    return np.floor(float(input_data['portfolio_size'])*float(input_data['position_size'])/df['Close'].iloc[-1])
    


