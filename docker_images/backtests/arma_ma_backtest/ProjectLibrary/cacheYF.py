import yfinance as yf


def cacheTicker(ticker):
    ticker = yf.Ticker(ticker)
    ticker_df = ticker.history(period="10y")
    return ticker_df
