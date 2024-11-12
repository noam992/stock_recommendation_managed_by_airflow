from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import sys
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
import smtplib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


# Add the parent directory to sys.path to import the main function
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from plugins.finviz_pattern_list import main as create_stock_list
from plugins.finviz_capture_graph import main as capture_finviz_graphs, capture_single_finviz_graph
from plugins.finviz_line_values import main as extract_finviz_line_values, extract_single_finviz_avg_line_values
from plugins.metrics import main as calculate_metrics, calculate_single_stock_metrics
from plugins.backtesting_analysis import main as calculate_backtest_strategy, calculate_single_backtest_strategy


def send_email(filename, sender_email, sender_password, email_recipients):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(email_recipients)
    msg['Subject'] = Header('Stock List Report', 'utf-8')

    # Attach the CSV file
    with open(filename, 'rb') as f:
        attachment = MIMEText(f.read().decode('utf-8'), 'csv', 'utf-8')
        attachment.add_header('Content-Disposition', 'attachment', filename='stocks_list.csv')
        msg.attach(attachment)

    try:
        with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:     
            smtp.ehlo()
            smtp.starttls() 
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
            logger.info(f"Email sent with CSV attachment to {email_recipients}")
    except Exception as e:
        logger.error(f"An error occurred while sending the email: {e}")
        raise


# Define the DAG
with DAG('stock_recommendation',
    description='Stock recommendation DAG using FinViz data',
    schedule_interval=None,  # No schedule - manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False) as dag:


    create_stock_list_task = PythonOperator(
        task_id='create_stock_list_task',
        python_callable=create_stock_list,
        op_kwargs={
            'patterns': ['ta_p_channel', 'ta_p_channelup', 'ta_p_channeldown'],
            'market_cap': 'large',
            'manual_tickers': ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'NVDA', 'JPM', 'V', 'JNJ', 'WMT', 'PG', 'DIS', 'NFLX', 'ADBE', 'SPY', 'QQQ', 'XOM', 'TLT', 'GLD', 'META', 'AMD', 'COIN', 'MARA', 'MU', 'SBUX', 'DVN', 'PLTR'],
            'filename': 'assets/stocks_list.csv'
        }
    )

    capture_finviz_graphs_task = PythonOperator(
        task_id='capture_finviz_graphs_task',
        python_callable=capture_finviz_graphs,
        op_kwargs={
            'filename': 'assets/stocks_list.csv',
            'image_folder': 'assets/images'
        }
    )

    extract_finviz_avg_support_line_value_task = PythonOperator(
        task_id='extract_finviz_avg_support_line_value_task',
        python_callable=extract_finviz_line_values,
        op_kwargs={
            'filename': 'assets/stocks_list.csv',
            'image_folder': 'assets/images',
            'line_name': 'support',
            'color_rgb': (37, 111, 149),
            'covered_line_rgb': (0, 165, 255)
        }
    )

    extract_finviz_avg_resistance_line_value_task = PythonOperator(
        task_id='extract_finviz_avg_resistance_line_value_task',
        python_callable=extract_finviz_line_values,
        op_kwargs={
            'filename': 'assets/stocks_list.csv',
            'image_folder': 'assets/images',
            'line_name': 'resistance',
            'color_rgb': (142, 73, 156),
            'covered_line_rgb': (0, 165, 255)
        }
    )

    calculate_metrics_task = PythonOperator(
        task_id='calculate_metrics_task',
        python_callable=calculate_metrics,
        op_kwargs={
            'filename': 'assets/stocks_list.csv'
        }
    )

    calculate_backtest_strategy_task = PythonOperator(
        task_id='calculate_backtest_strategy_task',
        python_callable=calculate_backtest_strategy,
        op_kwargs={
            'filename': 'assets/stocks_list.csv'
        }
    )

    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=send_email,
        op_kwargs={
            'filename': 'assets/stocks_list.csv',
            'sender_email': 'safe.trade.byai@gmail.com',
            'sender_password': '<insert password>',
            'email_recipients': ['noam.konja@gmail.com']
        }
    )

create_stock_list_task >> capture_finviz_graphs_task >> extract_finviz_avg_support_line_value_task >> extract_finviz_avg_resistance_line_value_task >> calculate_metrics_task >> calculate_backtest_strategy_task >> send_email_task
