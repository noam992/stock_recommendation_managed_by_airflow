from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# Add the parent directory to sys.path to import the main function
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from plugins.finviz_pattern_list import main as get_finviz_pattern_list, filter_by_market_cap
from plugins.finviz_capture_graph import main as get_finviz_capture_graph
from plugins.finviz_line_values import main as get_finviz_line_values, detect_straight_line_by_color, detect_non_straight_line_by_color
from plugins.measure_calculations import channel_range, ratio_of_current_price_to_channel_range

# Global variables
screener_url = 'https://finviz.com/screener.ashx?v=110&s='
page_size = 20
objects = 0
covered_line_rgb = (0, 165, 255)
line_colors = [
    ((37, 111, 149), 'support_rgb'), # blue
    ((142, 73, 156), 'resistance_rgb') # purple
]


def read_stocks_from_csv(stocks_list_filename: str) -> pd.DataFrame:
    try:
        stocks_df = pd.read_csv(stocks_list_filename)
        logging.info(f"Successfully read data from {stocks_list_filename}")
        return stocks_df
    except Exception as e:
        logging.error(f"Error reading CSV file {stocks_list_filename}: {str(e)}")
        return pd.DataFrame()


def save_to_csv(df, path_to_save):
    logging.info("Saving data to CSV")
    df.to_csv(path_to_save, index=False)
    logging.info(f"Data saved to {path_to_save}")


def extract_line_values_by_pattern(pattern_name: str, detected_line: str):

    image_folder = f'assets/{pattern_name}_images'
    stocks_list_filename = f'assets/stocks_pattern_{pattern_name}.csv'

    stocks_df_full = get_finviz_pattern_list(screener_url, pattern_name, page_size, objects, stocks_list_filename)
    stocks_df = filter_by_market_cap(stocks_df_full, 'large') # large: cap stocks >= 10B, medium: cap stocks >= 2B and < 10B, small: cap stocks < 2B
    save_to_csv(stocks_df, stocks_list_filename)
    # stocks_df = read_stocks_from_csv(stocks_list_filename)
    
    # Initialize new columns with NaN values
    stocks_df['average_resistance'] = float('nan')
    stocks_df['average_support'] = float('nan')
    stocks_df['channel_range'] = float('nan')
    stocks_df['current_price_ratio_channel'] = float('nan')

    for index, row in stocks_df.iterrows():
        logger.info(f"# Processing stock: {row['Ticker']}")

        ticker_name  = row['Ticker']
        ticker_price = float(row['Price'])

        graph_img_path = get_finviz_capture_graph(image_folder, ticker_name)
        # graph_img_path = f'assets/ta_p_channel_images/{ticker_name}_chart.png'
        
        results = {'resistance_rgb': [], 'support_rgb': []}
        for color_rgb, color_name in line_colors:

            if detected_line == 'straight':

                img_path, img = detect_straight_line_by_color(
                    ticker=ticker_name,
                    img_path=graph_img_path,
                    color_rgb=color_rgb,
                    line_frame=60,
                    covered_line_rgb=covered_line_rgb,
                    detect_minLineLength=50
                )

            elif detected_line == 'non_straight':

                img_path, img = detect_non_straight_line_by_color(
                    ticker=ticker_name,
                    img_path=graph_img_path,
                    color_rgb=color_rgb,
                    line_frame=80,
                    covered_line_rgb=covered_line_rgb,
                    detect_minLineLength=50
                )

            if img_path == [] and img == []:
                logging.info(f"No valid image found for {ticker_name} with {color_name} color - continuing to next iteration")
                continue

            color_result = get_finviz_line_values(img_path, img, ticker_name, ticker_price)

            results[color_name] = color_result

        if len(results['resistance_rgb']) > 0 and len(results['support_rgb']) > 0:
            logger.info(f"Resistance: {results['resistance_rgb']}, Dupport: {results['support_rgb']}")
            average_resistance = sum(results['resistance_rgb']) / len(results['resistance_rgb'])
            average_support = sum(results['support_rgb']) / len(results['support_rgb'])

            if average_resistance >= average_support:
                logger.info(f"Average Resistance: {average_resistance}, Average Support: {average_support}")
                stocks_df.at[index, 'average_resistance'] = average_resistance
                stocks_df.at[index, 'average_support'] = average_support
                
                # Calculate and store channel range and price ratio
                try:
                    channel_range_value = channel_range(average_support, average_resistance)
                    price_ratio = ratio_of_current_price_to_channel_range(ticker_price, average_support, average_resistance)
                    
                    stocks_df.at[index, 'channel_range'] = channel_range_value
                    stocks_df.at[index, 'current_price_ratio_channel'] = price_ratio
                    logger.info(f"Channel Range: {channel_range_value}, Price Ratio: {price_ratio}")
                except ValueError as e:
                    logger.error(f"Error calculating metrics for {ticker_name}: {str(e)}")
        
        plus_avg_line_path = stocks_list_filename.replace('.csv', '_with_avg.csv')
        save_to_csv(stocks_df, plus_avg_line_path)


# Define the DAG
with DAG('stock_recommendation',
    description='Stock recommendation DAG using FinViz data',
    schedule_interval=None,  # No schedule - manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False) as dag:


    get_finviz_graph_pattern_ta_p_channel_task = PythonOperator(
        task_id='get_finviz_graph_pattern_ta_p_channel_task',
        python_callable=extract_line_values_by_pattern,
        op_kwargs={
            'pattern_name': 'ta_p_channel',
            'detected_line': 'straight'
        }
    )

    get_finviz_graph_pattern_ta_p_channelup_task = PythonOperator(
        task_id='get_finviz_graph_pattern_ta_p_channelup_task',
        python_callable=extract_line_values_by_pattern,
        op_kwargs={
            'pattern_name': 'ta_p_channelup',
            'detected_line': 'non_straight'
        }
    )

    get_finviz_graph_pattern_ta_p_channeldown_task = PythonOperator(
        task_id='get_finviz_graph_pattern_ta_p_channeldown_task',
        python_callable=extract_line_values_by_pattern,
        op_kwargs={
            'pattern_name': 'ta_p_channeldown',
            'detected_line': 'non_straight'
        }
    )

get_finviz_graph_pattern_ta_p_channel_task >> get_finviz_graph_pattern_ta_p_channelup_task >> get_finviz_graph_pattern_ta_p_channeldown_task
