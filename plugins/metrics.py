import logging
import pandas as pd

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


def channel_range(lower_bound: float, upper_bound: float):
    if lower_bound > upper_bound:
        raise ValueError("Lower bound is greater than upper bound")
    return upper_bound - lower_bound


def ratio_of_current_price_to_channel_range(current_price: float, lower_bound: float, upper_bound: float):
    try:
        channel = channel_range(lower_bound, upper_bound)
        if channel == 0:
            logging.warning(f"Channel range is 0 for price {current_price}, bounds {lower_bound}-{upper_bound}")
            return 0
        return (current_price - lower_bound) / channel
    except Exception as e:
        logging.error(f"Error calculating price ratio: {str(e)}")
        return 0


def calculate_single_stock_metrics(ticker_name: str, ticker_price: float, support: float, resistance: float):

        if pd.notna(support) and pd.notna(resistance) and resistance >= support:
            try:
                channel_range_value = channel_range(support, resistance)
                price_ratio = ratio_of_current_price_to_channel_range(ticker_price, support, resistance)
                return channel_range_value, price_ratio
            except ValueError as e:
                logging.error(f"Error calculating metrics for {ticker_name}: {str(e)}")
                return 0, 0
        else:
            logging.warning(f"Skipping {ticker_name}: Invalid support/resistance values or resistance < support")
            return 0, 0


def main(filename: str):
    stocks_df = read_stocks_from_csv(filename)
    stocks_df['channel_range'] = float('nan')
    stocks_df['current_price_ratio_channel'] = float('nan')

    for index, row in stocks_df.iterrows():
        logging.info(f"# Processing stock: {row['Ticker']}")

        ticker_name = row['Ticker']
        ticker_price = row['Price']
        support = row['support']
        resistance = row['resistance']

        channel_range_value, price_ratio = calculate_single_stock_metrics(ticker_name, ticker_price, support, resistance)

        stocks_df.at[index, 'channel_range'] = channel_range_value
        stocks_df.at[index, 'current_price_ratio_channel'] = price_ratio
        logging.info(f"Channel Range: {channel_range_value}, Price Ratio: {price_ratio}")
    
    save_to_csv(stocks_df, filename)
