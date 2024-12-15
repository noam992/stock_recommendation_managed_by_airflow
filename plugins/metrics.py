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


def filter_to_relevant_stocks(filename: str):
    stocks_df = read_stocks_from_csv(filename)

    filtered_stocks_df = stocks_df[
        (stocks_df['channel_range'] >= 5) & 
        (
            (stocks_df['current_price_ratio_channel'] * 100 <= 20) & (stocks_df['current_price_ratio_channel'] * 100 >= -10) | 
            (stocks_df['current_price_ratio_support'] * 100 <= 2) & (stocks_df['current_price_ratio_support'] * 100 >= -1)
        )
    ]

    # Create new filename by adding _filter before extension
    filename_parts = filename.rsplit('.', 1)
    new_filename = f"{filename_parts[0]}_filter.{filename_parts[1]}"
    
    save_to_csv(filtered_stocks_df, new_filename)


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


def ratio_of_current_price_to_support(current_price: float, support: float) -> float:
    if pd.notna(support) and support != 0:
        return ((current_price - support) / support)
    return float('nan')


def calculate_single_stock_metrics(ticker_name: str, ticker_price: float, support: float, resistance: float):
    channel_range_value = 0
    price_ratio = 0
    lower_bound_distance = float('nan')
    potential_profit_value = float('nan')
    
    if pd.notna(support):
        lower_bound_distance = ratio_of_current_price_to_support(ticker_price, support)
        
        if pd.notna(resistance) and resistance >= support:
            try:
                channel_range_value = channel_range(support, resistance)
                price_ratio = ratio_of_current_price_to_channel_range(ticker_price, support, resistance)
                potential_profit_value = calculate_potential_profit(ticker_price, resistance)
            except ValueError as e:
                logging.error(f"Error calculating channel metrics for {ticker_name}: {str(e)}")
        else:
            logging.warning(f"Skipping channel calculations for {ticker_name}: Invalid resistance value or resistance < support")
    else:
        logging.warning(f"Skipping {ticker_name}: Invalid support value")
    
    return channel_range_value, price_ratio, lower_bound_distance, potential_profit_value


def calculate_potential_profit(current_price: float, resistance: float) -> float:
    if pd.notna(resistance) and resistance > 0:
        return (resistance - current_price) / current_price
    return float('nan')


def main(filename: str):
    stocks_df = read_stocks_from_csv(filename)
    stocks_df['channel_range'] = float('nan')
    stocks_df['current_price_ratio_channel'] = float('nan')
    stocks_df['current_price_ratio_support'] = float('nan')
    stocks_df['potential_profit_%'] = float('nan')

    for index, row in stocks_df.iterrows():
        logging.info(f"# Processing stock: {row['Ticker']}")

        ticker_name = row['Ticker']
        ticker_price = row['Price']
        support = row['support']
        resistance = row['resistance']

        channel_range_value, price_ratio, lower_bound_distance, potential_profit = calculate_single_stock_metrics(
            ticker_name, ticker_price, support, resistance)

        stocks_df.at[index, 'channel_range'] = channel_range_value
        stocks_df.at[index, 'current_price_ratio_channel'] = price_ratio
        stocks_df.at[index, 'current_price_ratio_support'] = lower_bound_distance
        stocks_df.at[index, 'potential_profit_%'] = potential_profit
        logging.info(f"Channel Range: {channel_range_value}, Price Ratio: {price_ratio}, "
                    f"Distance to Support: {lower_bound_distance:.2f}%, Potential Profit: {potential_profit:.2%}")
    
    save_to_csv(stocks_df, filename)


# if __name__ == "__main__":
#     main('../assets/stocks_list.csv')

