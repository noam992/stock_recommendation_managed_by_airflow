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
from plugins.metrics import main as calculate_metrics, calculate_single_stock_metrics, filter_to_relevant_stocks
from plugins.backtesting_analysis import main as calculate_backtest_strategy, calculate_single_backtest_strategy
from plugins.graphs import main as create_graphs
from plugins.google_services import main as upload_files_to_drive
from plugins.past_trend_analyzer import main as calculate_past_trends


def clean_directories(directories):
    for directory in directories:
        if os.path.exists(directory):
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path):
                        os.unlink(file_path)
                        logger.info(f"Deleted file: {file_path}")
                except Exception as e:
                    logger.error(f"Error deleting {file_path}: {e}")


def send_email(filename, sender_email, sender_password, email_recipients):
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = ', '.join(email_recipients)
    msg['Subject'] = Header(f'Stock List Report - {datetime.now().strftime("%Y-%m-%d")}', 'utf-8')

    # Attach the CSV file
    with open(filename, 'rb') as f:
        attachment = MIMEText(f.read().decode('utf-8'), 'csv', 'utf-8')
        attachment.add_header('Content-Disposition', 'attachment', filename='stocks_list.csv')
        msg.attach(attachment)

    # Attach all images from the output folder
    output_folder = 'assets/output'
    for image_file in os.listdir(output_folder):
        if image_file.lower().endswith(('.png', '.jpg', '.jpeg')):
            image_path = os.path.join(output_folder, image_file)
            with open(image_path, 'rb') as f:
                img_attachment = MIMEText(f.read(), 'base64', 'utf-8')
                img_attachment.add_header('Content-Disposition', 'attachment', filename=image_file)
                img_attachment.add_header('Content-Type', 'image/png')
                msg.attach(img_attachment)

    try:
        with smtplib.SMTP(host='smtp.gmail.com', port=587) as smtp:     
            smtp.ehlo()
            smtp.starttls() 
            smtp.login(sender_email, sender_password)
            smtp.send_message(msg)
            logger.info(f"Email sent with CSV and image attachments to {email_recipients}")
    except Exception as e:
        logger.error(f"An error occurred while sending the email: {e}")
        raise


# Define the DAG
with DAG('stock_recommendation',
    description='Stock recommendation DAG using FinViz data',
    schedule_interval='0 13 * * *',  # Run at 4 PM daily (16:00 israel time)
    start_date=datetime(2024, 1, 1),
    catchup=False) as dag:


    clean_directories_task = PythonOperator(
        task_id='clean_directories_task',
        python_callable=clean_directories,
        op_kwargs={
            'directories': ['assets/images', 'assets/output']
        }
    )

    create_stock_list_task = PythonOperator(
        task_id='create_stock_list_task',
        python_callable=create_stock_list,
        op_kwargs={
            'patterns': [], # ['ta_p_channel', 'ta_p_channelup', 'ta_p_channeldown'],
            'market_cap': 'large',
            'manual_tickers': ["ON"], # ['AMZN', 'GOOGL', 'MSFT', 'NVDA', 'TSLA', 'JPM', 'V', 'JNJ', 'WMT', 'PG', 'DIS', 'NFLX', 'ADBE', 'SPY', 'QQQ', 'XOM', 'TLT', 'GLD', 'META', 'AMD', 'COIN', 'MARA', 'MU', 'SBUX', 'DVN', 'PLTR'],
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

    calculate_past_trends_task = PythonOperator(
        task_id='calculate_past_trends_task',
        python_callable=calculate_past_trends,
        op_kwargs={
            'filename': 'assets/stocks_list.csv'
        }
    )

    filter_to_relevant_stocks_task = PythonOperator(
        task_id='filter_to_relevant_stocks_task',
        python_callable=filter_to_relevant_stocks,
        op_kwargs={
            'filename': 'assets/stocks_list.csv'
        }
    )

    create_graphs_task = PythonOperator(
        task_id='create_graphs_task',
        python_callable=create_graphs,
        op_kwargs={
            'filename': 'assets/stocks_list_filter.csv',
            'image_folder': 'assets/output'
        }
    )


    # upload_csv_to_drive_task = PythonOperator(
    #     task_id='upload_csv_to_drive_task',
    #     python_callable=upload_files_to_drive,
    #     op_kwargs={
    #         'file_type': 'csv',
    #         'file_dir': 'assets/stocks_list_filter.csv',
    #         'drive_folder_id': 'https://docs.google.com/spreadsheets/d/1w1_v4Pc_joAh9ymCGri0jJVSSIg-_3NzBOpR62Mu37s/edit?gid=0#gid=0',
    #         'credential_dir': 'assets/credentials/safe-trade-byai-1ad3bbad3477.json'
    #     }
    # )

    # upload_images_to_drive_task = PythonOperator(
    #     task_id='upload_images_to_drive_task',
    #     python_callable=upload_files_to_drive,
    #     op_kwargs={
    #         'file_type': 'img',
    #         'file_dir': 'assets/output',
    #         'drive_folder_id': '1NyxrwJCL77pSmtyP_fIwAayt73KCIf_s',
    #         'credential_dir': 'assets/credentials/safe-trade-byai-1ad3bbad3477.json'
    #     }
    # )

    send_email_task = PythonOperator(
        task_id='send_email_task',
        python_callable=send_email,
        op_kwargs={
            'filename': 'assets/stocks_list_filter.csv',
            'sender_email': 'safe.trade.byai@gmail.com',
            'sender_password': 'qrix vafb obge ezbu',
            'email_recipients': ['noam.konja@gmail.com']
        }
    )

clean_directories_task >> create_stock_list_task >> capture_finviz_graphs_task >> extract_finviz_avg_support_line_value_task >> extract_finviz_avg_resistance_line_value_task >> calculate_metrics_task >> calculate_backtest_strategy_task >> calculate_past_trends_task >> filter_to_relevant_stocks_task >> create_graphs_task >> send_email_task
# clean_directories_task >> create_stock_list_task >> capture_finviz_graphs_task >> extract_finviz_avg_support_line_value_task >> extract_finviz_avg_resistance_line_value_task >> calculate_metrics_task >> calculate_backtest_strategy_task >> calculate_past_trends_task >> filter_to_relevant_stocks_task >> create_graphs_task >> upload_csv_to_drive_task >> upload_images_to_drive_task >> send_email_task

