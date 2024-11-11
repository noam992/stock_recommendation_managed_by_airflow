import logging
from datetime import datetime, timedelta
import yfinance as yf
from backtesting import Backtest, Strategy


logger = logging.getLogger(__name__)


def get_stock_data(symbol, start_date, end_date):
    data = yf.download(symbol, start=start_date, end=end_date)
    if data.empty:
        raise ValueError(f"No data available for {symbol} between {start_date} and {end_date}")
    data = data.reset_index()
    data = data.set_index('Date')
    data.columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    data = data.drop('Adj Close', axis=1)
    return data


def calculate_sma(data, window):
    return data['Close'].rolling(window=window).mean()


def calculate_rsi(data, window):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


class StockStrategy(Strategy):
    def init(self):
        self.sma_short = self.I(calculate_sma, self.data.df, 20)
        self.sma_long = self.I(calculate_sma, self.data.df, 50)
        self.rsi = self.I(calculate_rsi, self.data.df, 14)

    def next(self):
        if self.sma_short[-1] > self.sma_long[-1] and self.rsi[-1] < 70:
            self.buy()
        elif self.sma_short[-1] < self.sma_long[-1] and self.rsi[-1] > 30:
            self.sell()


def backtest_strategy(symbol, start_date, end_date):
    data = get_stock_data(symbol, start_date, end_date)
    bt = Backtest(data, StockStrategy, cash=1000, commission=.0000)
    results = bt.run()
    trades = results['_trades']
    equity_curve = results['_equity_curve']
    return results, data, trades, equity_curve


def main(symbol, start_date, end_date):
    start_time = datetime.now()
    logger.info(f"Starting backtest analysis for {symbol} at {start_time}")

    recommendation = None
    try:
        results, data, trades, equity_curve = backtest_strategy(symbol, start_date, end_date)

        # Calculate the indicator values
        sma_short = calculate_sma(data, 20)
        sma_long = calculate_sma(data, 50)
        rsi = calculate_rsi(data, 14)

        recommendation = {
            'symbol': symbol,
            'EntryTime': trades['EntryTime'],
            'ExitTime': trades['ExitTime'],
            'return': results['Return [%]'],
            'equity_final': results['Equity Final [$]'],
            'sharpe_ratio': results['Sharpe Ratio'],
            'max_drawdown': results['Max. Drawdown [%]'],
            'last_price': data['Close'].iloc[-1],
            'strategy': 'Buy' if results['Return [%]'] > 0 else 'Sell',
            'sma_short_20': sma_short.iloc[-1],
            'sma_long_50': sma_long.iloc[-1],
            'rsi_14': rsi.iloc[-1],
            'data': data
        }
        
    except ValueError as e:
        logger.error(f"Error processing {symbol}: {str(e)}")

    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Completed backtest analysis for {symbol} at {end_time}. Duration: {duration}")
    
    return recommendation


# if __name__ == "__main__":
#     end_date = datetime.now()
#     start_date = end_date - timedelta(days=365)
#     symbol = 'NVDA'
    
#     recommendation = main(symbol, start_date, end_date)
#     print(recommendation)