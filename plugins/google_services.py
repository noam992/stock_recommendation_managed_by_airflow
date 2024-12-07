from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import os
import logging
import csv

# Configure logging to terminal with timestamp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def upload_img_to_drive(from_path, folder_id, credentials_path):
    logging.info(f"Starting upload process from {from_path} to Drive folder ID: {folder_id}")
    
    try:
        # Set up credentials
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        # Build the Drive API service
        service = build('drive', 'v3', credentials=credentials)
        
        # Verify folder exists and we have access
        try:
            service.files().get(fileId=folder_id).execute()
        except Exception as e:
            logging.error(f"Cannot access folder with ID {folder_id}. Error: {str(e)}")
            raise Exception("Folder access denied. Make sure the service account has been granted access to this folder.")
        
        # First, list and delete all files in the target folder
        try:
            results = service.files().list(
                q=f"'{folder_id}' in parents",
                fields="files(id, name)"
            ).execute()
            files = results.get('files', [])
            
            for file in files:
                logging.info(f"Deleting existing file: {file['name']}")
                service.files().delete(fileId=file['id']).execute()
            
            logging.info("Finished cleaning target folder")
        except Exception as e:
            logging.error(f"Error cleaning target folder: {str(e)}")
            raise
        
        # Upload all files in the from_path
        for filename in os.listdir(from_path):
            try:
                filepath = os.path.join(from_path, filename)
                
                if os.path.isdir(filepath):
                    logging.debug(f"Skipping directory: {filename}")
                    continue
                
                # Determine MIME type based on file extension
                mime_type = None
                if filename.lower().endswith(('.png', '.jpg', '.jpeg')):
                    mime_type = 'image/jpeg' if filename.lower().endswith('.jpg') or filename.lower().endswith('.jpeg') else 'image/png'
                elif filename.lower().endswith('.csv'):
                    mime_type = 'text/csv'
                else:
                    continue  # Skip unsupported file types
                
                if mime_type is None:
                    logging.debug(f"Skipping unsupported file type: {filename}")
                    continue
                
                logging.info(f"Uploading file: {filename}")
                # Prepare file metadata
                file_metadata = {
                    'name': filename,
                    'parents': [folder_id]
                }
                
                # Create media
                media = MediaFileUpload(
                    filepath,
                    mimetype=mime_type,
                    resumable=True
                )
                
                # Upload file
                service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                logging.info(f"Successfully uploaded: {filename}")
                
            except Exception as e:
                logging.error(f"Error uploading file {filename}: {str(e)}")
                continue
        
        logging.info("Upload process completed successfully")
        
    except Exception as e:
        logging.error(f"Upload process failed: {str(e)}")
        raise

def update_google_sheets(from_path, folder_id, credentials_path):    
    logging.info(f"Starting Google Sheets update process from {from_path}")
    
    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )
        
        sheets_service = build('sheets', 'v4', credentials=credentials)
        
        try:
            # Extract spreadsheet ID from the folder_id URL
            spreadsheet_id = folder_id.split('/')[5]
            
            # Read CSV file using csv module to properly handle quoted values
            with open(from_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file, quotechar='"', quoting=csv.QUOTE_MINIMAL)
                values = list(csv_reader)
            
            # Clear existing content - using the correct sheet name
            sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range='list!A:Z'  # Changed from Sheet1 to list
            ).execute()
            
            # Update with new content - using the correct sheet name
            body = {
                'values': values
            }
            
            result = sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range='list!A1',  # Changed from Sheet1 to list
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logging.info(f"Successfully updated Google Sheet. Updated {result.get('updatedCells')} cells.")
            
        except Exception as e:
            logging.error(f"Error updating Google Sheet: {str(e)}")
            raise
        
        logging.info("Update process completed successfully")
        
    except Exception as e:
        logging.error(f"Update process failed: {str(e)}")
        raise

def main(file_type, file_dir, drive_folder_id, credential_dir):

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    file_path = os.path.join(base_dir, file_dir)
    credential_path = os.path.join(base_dir, credential_dir)
    
    if file_type == "img":
        upload_img_to_drive(
            from_path=file_path,
            folder_id=drive_folder_id,
            credentials_path=credential_path
        )
    elif file_type == "csv":
        update_google_sheets(
            from_path=file_path,
            folder_id=drive_folder_id,
            credentials_path=credential_path
        )

# if __name__ == "__main__":

#     # main(file_type="img", file_dir="assets\output", drive_folder_id="1NyxrwJCL77pSmtyP_fIwAayt73KCIf_s", credential_dir="assets\credentials\safe-trade-byai-1ad3bbad3477.json")
#     main(file_type="csv", file_dir="assets\stocks_list_filter.csv", drive_folder_id="https://docs.google.com/spreadsheets/d/1w1_v4Pc_joAh9ymCGri0jJVSSIg-_3NzBOpR62Mu37s/edit?gid=0#gid=0", credential_dir="assets\credentials\safe-trade-byai-1ad3bbad3477.json")
