import yfinance as yf


def cacheTicker(ticker):
    ticker = yf.Ticker(ticker)
    ticker_df = ticker.history(period="5y")
    return ticker_df
