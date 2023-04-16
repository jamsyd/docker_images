import pandas as pd
from datetime import date

from dash import html
from dash import Dash, dash_table, dcc, html, Input, Output
from collections import OrderedDict

import awswrangler as wr

app = Dash(__name__)

todays_date = wr.s3.read_csv(r"""s3://jamsyd-model-metadata/arma-ma/training-dates/trading_dates.csv""")
todays_date = todays_date['train_dates'].iloc[-1]


df = wr.s3.read_csv(f"""s3://jamsyd-forecasts/arma_ma/{todays_date}.csv""")
df = df[['product_name','positionType','forecastDay','maDiff','forecastDiff','percDiff','retScore','pointForecast']]


#df = pd.read_csv(r'/home/jam/prd_arma_ma/arma_prd/output/results/2023-04-13.csv')[['product_name','positionType','forecastDay','maDiff','forecastDiff','percDiff','retScore','pointForecast']]

df_filter = df[(df['positionType'] != 'no_position')]

df2 = df_filter[df_filter['forecastDay']==5][['product_name','positionType','forecastDiff','percDiff','retScore','pointForecast']]

dashtable_1 = dash_table.DataTable(id='dashtable1',
            data=df2.to_dict('records'),
            columns=[{'id': c, 'name': c} for c in df.columns],
            fixed_rows={'headers': True},
            style_table={'height': "40%"},
            style_header={
                'backgroundColor':'rgb(50, 50, 50)',
                'color':'white'})

dashtable_2 = dash_table.DataTable(id='dashtable2',
              data=df.to_dict('records'),
              columns=[{'id': c, 'name': c} for c in df.columns],
              fixed_rows={'headers': True},
              style_table={'height': "10%"},
              style_header={'backgroundColor':'rgb(50, 50, 50)','color':'white'},
              style_data_conditional = [{'if': {'filter_query': '{positionType} contains "long"'},'backgroundColor': '#AAFF00','color': 'black'},
                      {'if': {'filter_query': '{positionType} contains "short"'},'backgroundColor': '#EE4B2B','color': 'black'}])

# Setting our
app.layout = html.Div([html.H1('Trade Summary',style={"font-weight":"bold"}),html.Br(),
                       html.Div(dashtable_1),
                       html.Br(),html.Br(),html.Br(),
                       html.Div(dashtable_2)])

if __name__ == '__main__':
        app.run_server(debug=True)
