import os
import io
import time
import logging
import pandas as pd
from typing import List
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from PIL import Image
from airflow.exceptions import AirflowException
from subprocess import Popen


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


html_tags = {
    'theme_tag': {
        'tagDataTestID': 'a[data-testid="chart-layout-theme"]',
        'WebDriverWait': 2
    },
    'close_popup_tag': {
        'tagID': 'aymStickyFooterClose',
        'sleep_before': 3,
        'WebDriverWait': 3
    },
    'chart_tag': {
        'tag': 'canvas',
        'sleep_before': 5,
        'WebDriverWait': 5
    },
    'stock_data_tag': {
        'tagDataTestID': 'div[data-testid="quote-data-content"]',
        'WebDriverWait': 1
    }
}


def read_stocks_from_csv(filename: str) -> pd.DataFrame:
    try:
        stocks_df = pd.read_csv(filename)
        logging.info(f"Successfully read data from {filename}")
        return stocks_df
    except Exception as e:
        logging.error(f"Error reading CSV file {filename}: {str(e)}")
        return pd.DataFrame()


def close_popup_privacy(driver):
    logging.info("Attempting to close privacy popup")
    try:
        time.sleep(html_tags['close_popup_tag']['sleep_before'])
        close_button = WebDriverWait(driver, html_tags['close_popup_tag']['WebDriverWait']).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[mode="primary"]'))
        )
        close_button.click()
        logging.info("Privacy popup closed successfully")
    except Exception as e:
        logging.warning(f"No privacy popup found or unable to close: {str(e)}")


def apply_white_theme(driver):
    logging.info("Attempting to apply white theme")
    try:
        theme_button = WebDriverWait(driver, html_tags['theme_tag']['WebDriverWait']).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, html_tags['theme_tag']['tagDataTestID']))
        )
        theme_button.click()
        logging.info("White theme applied successfully")
    except Exception as e:
        logging.error(f"Unable to apply white theme: {str(e)}")


def close_popup_ad(driver):
    logging.info("Attempting to close popup ad")
    try:
        time.sleep(html_tags['close_popup_tag']['sleep_before'])
        close_button = WebDriverWait(driver, html_tags['close_popup_tag']['WebDriverWait']).until( EC.element_to_be_clickable((By.ID, html_tags['close_popup_tag']['tagID'])) )
        close_button.click()

        logging.info("Popup ad closed successfully")
    except Exception as e:
        logging.warning(f"No popup ad found or unable to close: {str(e)}")


def scroll_to_bottom(driver, scroll_amount: int):
    driver.execute_script(f"window.scrollBy(0, {scroll_amount});")


def save_chart_img(driver, ticker, image_folder: str, scroll_amount: int):
    logging.info(f"Processing chart image for {ticker}")
    try:
        time.sleep(html_tags['chart_tag']['sleep_before'])
        canvas = WebDriverWait(driver, html_tags['chart_tag']['WebDriverWait']).until(
            EC.presence_of_element_located((By.TAG_NAME, html_tags['chart_tag']['tag']))
        )

        screenshot = driver.get_screenshot_as_png()
        screenshot = Image.open(io.BytesIO(screenshot))

        device_pixel_ratio = driver.execute_script('return window.devicePixelRatio;')

        canvas_location = canvas.location
        canvas_size = canvas.size

        left = int(canvas_location['x'] * device_pixel_ratio)
        top = int((canvas_location['y'] - scroll_amount) * device_pixel_ratio)
        right = int((canvas_location['x'] + canvas_size['width']) * device_pixel_ratio)
        bottom = int((canvas_location['y'] - scroll_amount + canvas_size['height']) * device_pixel_ratio)

        chart_image = screenshot.crop((left, top, right, bottom))

        img_path = f"{image_folder}/{ticker}_original_img.png"
        chart_image.save(img_path)

        logging.info(f"Successfully saved chart image for {ticker}")
        return img_path
    
    except Exception as e:
        logging.error(f"Failed to process chart image for {ticker}. Error: {str(e)}")
        return False


def get_stock_info(driver) -> pd.DataFrame:
    logging.info("Extracting stock information from page")
    try:
        time.sleep(2)  # Small delay to ensure data is loaded
        stock_data_div = WebDriverWait(driver, html_tags['stock_data_tag']['WebDriverWait']).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, html_tags['stock_data_tag']['tagDataTestID']))
        )
        
        # Create a dictionary to store the data
        stock_info = {}
        
        # Find all table cells
        cells = stock_data_div.find_elements(By.TAG_NAME, "td")
        
        # Process cells in pairs (label and value)
        for i in range(0, len(cells), 2):
            if i + 1 < len(cells):
                label = cells[i].text.strip()
                value = cells[i + 1].text.strip()
                stock_info[label] = value
        
        # Create DataFrame with specific columns
        df = pd.DataFrame([stock_info])
        
        logging.info("Successfully extracted stock information")
        return df
    
    except Exception as e:
        logging.error(f"Failed to extract stock information. Error: {str(e)}")
        return pd.DataFrame()


def capture_single_finviz_graph(ticker: str, image_folder: str):
    scroll_amount = 500
    chrome_zoom = 1.75

    logging.info(f"Starting chart scan for {ticker}")
    
    # Set default display if not set
    if 'DISPLAY' not in os.environ:
        os.environ['DISPLAY'] = ':99'
    if 'DISPLAY_WIDTH' not in os.environ:
        os.environ['DISPLAY_WIDTH'] = '1920'
    if 'DISPLAY_HEIGHT' not in os.environ:
        os.environ['DISPLAY_HEIGHT'] = '1080'

    # Start Xvfb before browser
    try:
        logging.info("Starting Xvfb...")
        display = os.environ['DISPLAY']
        width = os.environ['DISPLAY_WIDTH']
        height = os.environ['DISPLAY_HEIGHT']
        xvfb_process = Popen(['/usr/bin/Xvfb', display, '-screen', '0', f'{width}x{height}x24'])
        time.sleep(3)  # Give Xvfb time to start
    except Exception as e:
        logging.error(f"Failed to start Xvfb: {str(e)}")
        raise

    chrome_options = Options()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-software-rasterizer")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument(f"--force-device-scale-factor={chrome_zoom}")
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument(f'--display={os.environ["DISPLAY"]}')

    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(f"https://finviz.com/quote.ashx?t={ticker}&ty=c&p=d&b=1")

        # close_popup_privacy(driver)

        apply_white_theme(driver)
        close_popup_ad(driver)
        scroll_to_bottom(driver, scroll_amount)

        chart_img_path = save_chart_img(driver, ticker, image_folder, scroll_amount)
        if not chart_img_path:
            raise AirflowException(f"Failed to save chart image for {ticker}")
        
        # Get stock information
        stock_info_df = get_stock_info(driver)
        
        logging.info(f"Chart image saved for {ticker} at {chart_img_path}")
        return chart_img_path, stock_info_df

    except Exception as e:
        error_msg = f"Error processing chart for {ticker}: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)
    finally:
        if driver:
            driver.quit()
        # Clean up Xvfb
        if 'xvfb_process' in locals():
            xvfb_process.terminate()
            xvfb_process.wait()


def main(filename: str, image_folder: str):
    try:

        if not os.path.exists(image_folder):
            os.makedirs(image_folder)

        stocks_df = read_stocks_from_csv(filename)
        stocks_df['original_img_path'] = None
        
        # Add new columns
        new_columns = ['Market Cap', 'Volume', 'Earnings', 'Avg Volume', 'Rel Volume', 'Prev Close', 'Price', 'Change']
        for col in new_columns:
            stocks_df[col] = None

        for index, row in stocks_df.iterrows():
            ticker_name = row['Ticker']
            graph_img_path, stock_info_df = capture_single_finviz_graph(ticker=ticker_name, image_folder=image_folder)
            
            if not graph_img_path:
                raise AirflowException(f"Failed to capture graph for {ticker_name}")
                
            # Store the image path and stock info in the DataFrame
            stocks_df.at[index, 'original_img_path'] = graph_img_path
            
            # Update stock information columns
            if not stock_info_df.empty:
                for col in new_columns:
                    if col in stock_info_df.columns:
                        stocks_df.at[index, col] = stock_info_df.iloc[0][col]

        # Save the updated DataFrame back to the CSV file
        stocks_df.to_csv(filename, index=False)
        
    except Exception as e:
        error_msg = f"Failed to process finviz capture for {ticker_name}: {str(e)}"
        logging.error(error_msg)
        raise AirflowException(error_msg)


# if __name__ == "__main__":
#     result = capture_single_finviz_graph(image_folder = 'assets/images', ticker_name = 'BP')
#     print(result)