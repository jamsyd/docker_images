import pandas as pd
import awswrangler as wr

from ProjectLibrary.scale import getScale

def cachePositions(todays_date, dataframe):

    cachePositionData = {

        'product_name':[],
        'positionType':[],
        'entryPrice':[],
        'entry_date':[],
        'scale':[],
        'position_value':[],

    }

    for product_name in dataframe['product_name'].unique():

        position_type = dataframe[dataframe['product_name'] == product_name]['positionType'].iloc[-1]

        if position_type == 'short' or position_type == 'long':
        
            scale         = getScale(product_name,todays_date)  

            cachePositionData['position_value'].append(scale*dataframe[dataframe['product_name'] == product_name]['entryPrice'].iloc[-1])
            cachePositionData['product_name'].append(product_name)
            cachePositionData['positionType'].append(position_type)
            cachePositionData['entry_date'].append(todays_date)
            cachePositionData['entryPrice'].append(dataframe[dataframe['product_name'] == product_name]['entryPrice'].iloc[-1])
            cachePositionData['scale'].append(scale)

        if position_type == 'no_position':

            scale = 0

            cachePositionData['position_value'].append(scale*dataframe[dataframe['product_name'] == product_name]['entryPrice'].iloc[-1])
            cachePositionData['product_name'].append(product_name)
            cachePositionData['positionType'].append(position_type)
            cachePositionData['entry_date'].append(todays_date)
            cachePositionData['entryPrice'].append(dataframe[dataframe['product_name'] == product_name]['entryPrice'].iloc[-1])
            cachePositionData['scale'].append(scale)


    return pd.DataFrame(cachePositionData)

