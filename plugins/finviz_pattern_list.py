import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')


def convert_market_cap_to_billions(cap_str):
    """Convert market cap string to numeric billions value."""
    if cap_str == '-' or not cap_str:  # Handle ETFs and missing values
        return None
    
    # Remove B or M and convert to float
    value = float(cap_str.replace('B', '').replace('M', ''))
    
    # Convert to billions
    if 'M' in cap_str:
        return value / 1000  # Convert millions to billions
    return value  # Already in billions if B


def filter_by_market_cap(df, market_cap_text):
    """Filter DataFrame based on market cap threshold."""
    # Convert Market Cap column to numeric values in billions
    df['Market_Cap_Billions'] = df['Market Cap'].apply(convert_market_cap_to_billions)
    
    # Filter based on threshold
    if market_cap_text.lower() == 'large':
        return df[df['Market_Cap_Billions'] >= 10].copy()
    elif market_cap_text.lower() == 'medium':
        return df[(df['Market_Cap_Billions'] >= 2) & 
                 (df['Market_Cap_Billions'] < 10)].copy()
    elif market_cap_text.lower() == 'small':
        return df[df['Market_Cap_Billions'] < 2].copy()
    else:
        return df  # Return original dataframe if invalid threshold


def get_stocks_details_of_pattern(url, page_num):
    logging.info(f'Getting stocks from {url} page {page_num}')

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Try to find the table with different class names
    table = soup.find('table', class_='table-light')
    if table is None:
        table = soup.find('table', class_='screener_table')
    if table is None:
        table = soup.find('table', {'id': 'screener-content'})

    if table is None:
        logging.info("Could not find the table. The website structure might have changed.")
        return pd.DataFrame()

    rows = table.find_all('tr')[1:]  # Skip the header row

    data = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) > 10:
            ticker = cols[1].text.strip()
            company = cols[2].text.strip()
            sector = cols[3].text.strip()
            industry = cols[4].text.strip()
            country = cols[5].text.strip()
            market_cap = cols[6].text.strip()
            price = cols[8].text.strip()
            change = cols[9].text.strip()
            volume = cols[10].text.strip()

            data.append({
                'Ticker': ticker,
                'Company': company,
                'Sector': sector,
                'Industry': industry,
                'Country': country,
                'Market Cap': market_cap,
                'Price': price,
                'Change': change,
                'Volume': volume
            })

    df = pd.DataFrame(data)
    return df


def save_to_csv(df, filename):
    logging.info("Saving data to CSV")
    df.to_csv(filename, index=False)
    logging.info(f"Data saved to {filename}")


def get_total_pages(screener_url, pattern):

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }

    full_url = f'{screener_url}{pattern}'
    response = requests.get(full_url, headers=headers)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    pagination = soup.find('td', {'id': 'screener_pagination'})
    if pagination:
        links = pagination.find_all('a')
        # Filter out the 'NEXT' link and get the last number
        page_numbers = [int(link.text) for link in links if link.text.isdigit()]
        if page_numbers:
            return max(page_numbers)
    return 1  # Return 1 if no pagination found


def main(screener_url, pattern, page_size, objects, filename):

    pattern_df = pd.DataFrame()
    tmp_pattern_df = pd.DataFrame()

    total_pages = get_total_pages(screener_url, pattern)

    for i in range(total_pages):
        full_url = f'{screener_url}{pattern}&r={objects}'

        tmp_pattern_df = get_stocks_details_of_pattern(full_url, i)
        pattern_df = pd.concat([pattern_df, tmp_pattern_df], ignore_index=True)

        if i == 0:
            objects = page_size
        else:
            objects += page_size

    # save_to_csv(pattern_df, filename)

    return pattern_df


# if __name__ == "__main__":
#     screener_url = 'https://finviz.com/screener.ashx?v=110&s='
#     page_size = 20
#     objects = 0
#     pattern_name = 'ta_p_channelup'
#     image_folder = f'assets/{pattern_name}_images'
#     stocks_list_filename = f'assets/stocks_pattern_{pattern_name}.csv'

#     stocks_df_full = main(screener_url, pattern_name, page_size, objects, stocks_list_filename)
#     stocks_df = filter_by_market_cap(stocks_df_full, 'large')
#     save_to_csv(stocks_df, stocks_list_filename)
#     print(stocks_df)