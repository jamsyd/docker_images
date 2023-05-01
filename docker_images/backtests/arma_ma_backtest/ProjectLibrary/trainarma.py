
import numpy as np
import pandas as pd

import awswrangler as wr

import statsmodels.api as sm
from datetime import datetime

class train_arma:
    
    def __init__(self, event):

        # Read in dataframe
        self.dataframe = wr.s3.read_csv(event['dataframe'])
        self.dataframe.set_index('Date',inplace=True)


        # Training information
        self.forecastHorizon = event['forecastHorizon']
        self.trainDFLength   = event['trainDFLength']
        self.order           = event['order']
        self.diff            = event['diff']
        self.product         = event['product']
        self.column          = event['column']

        # number of mdoels to be trained
        self.num_models      = event['num_models']

        # store l of forecast
        self.forecastList    = []

    def arma_model(self):

        cacheForecasts = {

            'asofdate':[],
            'pointForecast':[],
            self.column:[],
            'forecastday':[],
            'product_name':[],
            'MA_50':[],
            'MA_diff_50':[],

        }

        cacheMetadata = {

            'asofdate':[],
            'aic':[],
            'bic':[],
            'hqic':[],
            'mae':[],
            'mse':[],
            'forecastHorizon':[],
            'diff':[],
            'trainDFLength':[],
            'num_models':[],
            'order_p':[],
            'order_q':[],
            'time':[],
            'product_name':[],
            'acf_0':[],
            'acf_1':[],
            'acf_2':[],
            'acf_3':[],
            'acf_4':[],
            'pacf_0':[],
            'pacf_1':[],
            'pacf_2':[],
            'pacf_3':[],
            'pacf_4':[],
        }

        if self.diff:
            diff   = np.log(self.dataframe[self.column]).diff(1).dropna(axis=0)
    
            for j in range(1,self.forecastHorizon+1):
                self.forecastList.append(j)

            import warnings
            warnings.filterwarnings("ignore")

            i = 0
            while i < self.num_models:

   
                try:
                    start = datetime.now()

                    mod = sm.tsa.arima.ARIMA(diff[i:self.trainDFLength+i], order=self.order)

                    res = mod.fit()
                    # converting log forecast
                    fcast = res.forecast(self.forecastHorizon)

                    # Calculating forecasts
                    cacheForecasts['pointForecast'].append(np.exp(fcast.reset_index()['predicted_mean'])*self.dataframe[self.column][self.trainDFLength+i])
                    cacheForecasts[self.column].append(self.dataframe[self.column][self.trainDFLength+i+1:self.trainDFLength+self.forecastHorizon+i+1])

                    # print(cacheForecasts)

                    # store model metadata
                    cacheMetadata['asofdate'].append(self.dataframe.index[self.trainDFLength+i])
                    cacheMetadata['aic'].append(res.aic)
                    cacheMetadata['bic'].append(res.bic)
                    cacheMetadata['hqic'].append(res.hqic)
                    cacheMetadata['mae'].append(np.mean(np.abs(res.resid)))
                    cacheMetadata['mse'].append(np.mean(np.square(res.resid)))

                    # event_data
                    cacheMetadata['forecastHorizon'].append(self.forecastHorizon)
                    cacheMetadata['diff'].append(self.diff)
                    cacheMetadata['trainDFLength'].append(self.trainDFLength)
                    cacheMetadata['num_models'].append(self.num_models)
                    cacheMetadata['order_p'].append(self.order[0])
                    cacheMetadata['order_q'].append(self.order[1])
                    cacheMetadata['product_name'].append(self.product)

                    cacheMetadata['time'].append(datetime.now() - start)


                    acf, ci = sm.tsa.acf(diff[i:self.trainDFLength+i],nlags=5, alpha=0.05)
                    pacf, ci = sm.tsa.pacf(diff[i:self.trainDFLength+i],nlags=5, alpha=0.05)

                    cacheMetadata['acf_0'].append(acf[0])
                    cacheMetadata['acf_1'].append(acf[1])
                    cacheMetadata['acf_2'].append(acf[2])
                    cacheMetadata['acf_3'].append(acf[3])
                    cacheMetadata['acf_4'].append(acf[4])

                    cacheMetadata['pacf_0'].append(pacf[0])
                    cacheMetadata['pacf_1'].append(pacf[1])
                    cacheMetadata['pacf_2'].append(pacf[2])
                    cacheMetadata['pacf_3'].append(pacf[3])
                    cacheMetadata['pacf_4'].append(pacf[4])        

                except:
                    print("fail")

                i+=self.forecastHorizon

            # Fixing data formats
            cacheForecasts['pointForecast'] = np.concatenate(cacheForecasts['pointForecast'])
            cacheForecasts['forecastday']   = np.array(self.forecastList*(int(len(cacheForecasts['pointForecast'])/int(self.forecastHorizon))))
            cacheForecasts[self.column]     = np.array(self.dataframe[self.column][self.trainDFLength:self.trainDFLength+len(cacheForecasts['pointForecast'])])
            cacheForecasts['asofdate']      = np.array(self.dataframe[self.column][1+self.trainDFLength:1+self.trainDFLength+len(cacheForecasts['pointForecast'])].index)
            cacheForecasts['product_name']  = np.array([self.product]*int(len(cacheForecasts['pointForecast'])))
            cacheForecasts['MA_50']         = self.dataframe[self.column].rolling(50).mean()[self.trainDFLength:self.trainDFLength+len(cacheForecasts['pointForecast'])]
            cacheForecasts['MA_diff_50']    = self.dataframe[self.column].rolling(50).mean().diff(1)[self.trainDFLength:self.trainDFLength+len(cacheForecasts['pointForecast'])]

            wr.s3.to_csv(pd.DataFrame(cacheForecasts),f"""s3://jamsyd-backtests/arma_ma/{self.product}_forecasts_{self.order}.csv""")
            wr.s3.to_csv(pd.DataFrame(cacheMetadata),f"""s3://jamsyd-backtests/arma_ma/{self.product}_metadata_{self.order}.csv""")


