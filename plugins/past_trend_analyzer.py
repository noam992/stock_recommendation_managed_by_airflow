import numpy as np
import pandas as pd
import yfinance as yf
import logging
from datetime import datetime, timedelta
from backtesting import Backtest, Strategy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def read_stocks_from_csv(filename: str) -> pd.DataFrame:
    try:
        stocks_df = pd.read_csv(filename)
        logging.info(f"Successfully read data from {filename}")
        return stocks_df
    except Exception as e:
        logging.error(f"Error reading CSV file {filename}: {str(e)}")
        return pd.DataFrame()


def save_to_csv(df, filename):
    logging.info("Saving data to CSV")
    df.to_csv(filename, index=False)
    logging.info(f"Data saved to {filename}")


def get_stock_data(symbol, start_date, end_date):
    data = yf.download(symbol, start=start_date, end=end_date)
    if data.empty:
        raise ValueError(f"No data available for {symbol} between {start_date} and {end_date}")
    data = data.reset_index()
    data = data.set_index('Date')
    data.columns = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    data = data.drop('Adj Close', axis=1)
    return data


def calculate_single_previous_rising_trade_stock(symbol, start_date, end_date, min_gap):
    start_time = datetime.now()
    logging.info(f"Starting gap analysis for {symbol} at {start_time}")

    analysis_result = None
    try:
        data = get_stock_data(symbol, start_date, end_date)
        marked_data, trend_metrics = identify_price_trends(data, min_gap, fluctuation_pct=0.02)
        
        analysis_result = {
            'symbol': symbol,
            'last_up_trade_resistence_date': trend_metrics['last_up_trade_resistence_date'].iloc[0],
            'last_up_trade_resistence_price': trend_metrics['last_up_trade_resistence_price'].iloc[0],
            'last_up_trade_support_date': trend_metrics['last_up_trade_support_date'].iloc[0],
            'last_up_trade_support_price': trend_metrics['last_up_trade_support_price'].iloc[0],
            'last_up_trade_range': trend_metrics['last_up_trade_range'].iloc[0],
            'last_up_trade_days': trend_metrics['last_up_trade_days'].iloc[0],
            'last_up_trade_avg_days': trend_metrics['last_up_trade_avg_days'].iloc[0],
            'data': marked_data
        }
        
    except ValueError as e:
        logging.error(f"Error processing {symbol}: {str(e)}")

    end_time = datetime.now()
    duration = end_time - start_time
    logging.info(f"Completed gap analysis for {symbol} at {end_time}. Duration: {duration}")
    
    return analysis_result


def identify_price_trends(data, min_gap, fluctuation_pct=0.02):
    price_data = data.copy().sort_index()
    
    price_data['gap_marker'] = 0
    price_data['gap_number'] = 0
    
    gap_count = 1
    i = 0
    
    while i < len(price_data) - 1:
        start_price = float(price_data['Close'].iloc[i])
        start_idx = i
        found_gap = False
        current_min = start_price
        min_idx = i
        
        for j in range(i + 1, len(price_data)):
            current_price = float(price_data['Close'].iloc[j])
            
            if current_price < current_min:
                current_min = current_price
                min_idx = j
                continue
            
            if current_price - current_min >= min_gap:
                found_gap = True
                max_price = current_price
                
                price_data.iloc[min_idx:j+1, price_data.columns.get_loc('gap_number')] = gap_count
                price_data.iloc[min_idx:j+1, price_data.columns.get_loc('gap_marker')] = 1
                
                continue_idx = j
                for k in range(j + 1, len(price_data)):
                    next_price = float(price_data['Close'].iloc[k])
                    
                    if next_price >= max_price * (1 - fluctuation_pct):
                        price_data.iloc[k, price_data.columns.get_loc('gap_number')] = gap_count
                        price_data.iloc[k, price_data.columns.get_loc('gap_marker')] = 1
                        max_price = max(max_price, next_price)
                        continue_idx = k
                    else:
                        break
                
                i = continue_idx + 1
                gap_count += 1
                break
        
        if not found_gap:
            i += 1

    trend_metrics = compute_trend_metrics(price_data)

    return price_data, trend_metrics


def compute_trend_metrics(price_data):
    summary = pd.DataFrame()
    
    latest_gap = price_data[price_data['gap_number'] > 0]['gap_number'].max()
    
    if pd.isna(latest_gap):
        return pd.DataFrame({
            'last_up_trade_resistence_date': [None],
            'last_up_trade_resistence_price': [None],
            'last_up_trade_support_date': [None],
            'last_up_trade_support_price': [None],
            'last_up_trade_range': [None],
            'last_up_trade_days': [None],
            'last_up_trade_avg_days': [None]
        })
    
    latest_gap_data = price_data[price_data['gap_number'] == latest_gap]
    
    summary['last_up_trade_resistence_date'] = [latest_gap_data.index[-1].strftime('%Y-%m-%d')]
    summary['last_up_trade_resistence_price'] = [latest_gap_data['Close'].iloc[-1]]
    summary['last_up_trade_support_date'] = [latest_gap_data.index[0].strftime('%Y-%m-%d')]
    summary['last_up_trade_support_price'] = [latest_gap_data['Close'].iloc[0]]
    summary['last_up_trade_range'] = [
        latest_gap_data['Close'].iloc[-1] - latest_gap_data['Close'].iloc[0]
    ]
    
    summary['last_up_trade_days'] = [len(latest_gap_data)]
    
    all_gaps_trading_days = []
    for gap_num in price_data[price_data['gap_number'] > 0]['gap_number'].unique():
        gap_data = price_data[price_data['gap_number'] == gap_num]
        trading_days = len(gap_data)
        all_gaps_trading_days.append(trading_days)
    
    summary['last_up_trade_avg_days'] = [np.mean(all_gaps_trading_days)]
    
    return summary


def main(filename: str):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    stocks_df = read_stocks_from_csv(filename)
        
    stocks_df['last_up_trade_resistence_date'] = pd.Series(dtype='object')
    stocks_df['last_up_trade_resistence_price'] = pd.Series(dtype='float64')
    stocks_df['last_up_trade_support_date'] = pd.Series(dtype='object')
    stocks_df['last_up_trade_support_price'] = pd.Series(dtype='float64')
    stocks_df['last_up_trade_range'] = pd.Series(dtype='float64')
    stocks_df['last_up_trade_days'] = pd.Series(dtype='float64')
    stocks_df['last_up_trade_avg_days'] = pd.Series(dtype='float64')

    for index, row in stocks_df.iterrows():
        symbol = row['Ticker']
        min_gap = float(row['channel_range'])
        analysis_response = calculate_single_previous_rising_trade_stock(symbol, start_date, end_date, min_gap=min_gap)
        
        if analysis_response:
            for col in ['last_up_trade_resistence_date', 'last_up_trade_resistence_price',
                        'last_up_trade_support_date', 'last_up_trade_support_price',
                        'last_up_trade_range', 'last_up_trade_days', 'last_up_trade_avg_days']:
                stocks_df.at[index, col] = analysis_response[col]
    
    save_to_csv(stocks_df, filename)


# if __name__ == "__main__":
#     main('../assets/stocks_list.csv')
    # symbol = 'ON'
    # end_date = datetime.now()
    # start_date = end_date - timedelta(days=365)

    # # Get stock data and create DataFrame
    # stock_data = get_stock_data(symbol, start_date, end_date)
    # stocks_df = read_stocks_from_csv('../assets/stocks_list.csv')
    # min_gap = float(stocks_df['channel_range'])
    # if min_gap is None:
    #     logging.error("No channel_range found in input file")

    # print(min_gap)
    # stocks_df['last_up_trade_resistence_date'] = pd.Series(dtype='object')
    # stocks_df['last_up_trade_resistence_price'] = pd.Series(dtype='float64')
    # stocks_df['last_up_trade_support_date'] = pd.Series(dtype='object')
    # stocks_df['last_up_trade_support_price'] = pd.Series(dtype='float64')
    # stocks_df['last_up_trade_range'] = pd.Series(dtype='float64')
    # stocks_df['last_up_trade_days'] = pd.Series(dtype='float64')
    # stocks_df['last_up_trade_avg_days'] = pd.Series(dtype='float64')

    # analysis_response = calculate_single_previous_rising_trade_stock(symbol, start_date, end_date, min_gap)
        
    # if analysis_response:
    #     for col in ['last_up_trade_resistence_date', 'last_up_trade_resistence_price',
    #                 'last_up_trade_support_date', 'last_up_trade_support_price',
    #                 'last_up_trade_range', 'last_up_trade_days', 'last_up_trade_avg_days']:
    #         stocks_df.loc[0, col] = analysis_response[col]
            
    # save_to_csv(stocks_df, '../assets/stocks_list.csv')
    # print("\nAnalysis Results:")
    # print(stocks_df)
