import pickle
import os
from google_auth_oauthlib.flow import Flow, InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
from google.auth.transport.requests import Request



def Create_Service(client_secret_file, api_name, api_version, *scopes):
    print(client_secret_file, api_name, api_version, scopes, sep='-')
    CLIENT_SECRET_FILE = client_secret_file
    API_SERVICE_NAME = api_name
    API_VERSION = api_version
    SCOPES = [scope for scope in scopes[0]]
    print(SCOPES)

    cred = None

    pickle_file = f'token_{API_SERVICE_NAME}_{API_VERSION}.pickle'

    if os.path.exists(pickle_file):
        with open(pickle_file, 'rb') as token:
            cred = pickle.load(token)

    if not cred or not cred.valid:
        if cred and cred.expired and cred.refresh_token:
            cred.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRET_FILE, SCOPES)
            cred = flow.run_local_server()

        with open(pickle_file, 'wb') as token:
            pickle.dump(cred, token)

    try:
        service = build(API_SERVICE_NAME, API_VERSION, credentials=cred)
        print(API_SERVICE_NAME, 'service created successfully')
        return service
    except Exception as e:
        print('Unable to connect.')
        print(e)
        return None

def convert_to_RFC_datetime(year=1900, month=1, day=1, hour=0, minute=0):
    dt = datetime.datetime(year, month, day, hour, minute, 0).isoformat() + 'Z'
    return dt


client_secret_file = 'C:/Users/User/Desktop/Moje dokumenty/client_secret.json'
api_name = 'drive'
api_version = 'v3'
scopes = ['https://www.googleapis.com/auth/drive']

service = Create_Service(client_secret_file,api_name,api_version,scopes)

def export_csv_file(file_path: str, parents: list=None):
    if not os.path.exists(file_path):
        print(f'{file_path} not found.')
        return
    try:
        file_metadata = {
            'name' : os.path.basename(file_path).replace('.csv',''),
            'mimetype': 'application/vdn.google-apps.spreadsheet',
            'parents': parents
        }
        
        media = MediaFileUpload(filename=file_path, mimetype='text/csv')

        response = service.files().create(
            media_body = media,
            body = file_metadata
        ).execute()

        print(response)
        return response

    except Exception as e:
        print(e)
        return

#Select file    
csv_files = os.listdir('C:/Users/User/Desktop/Moje dokumenty/test')

#Select google drive folder
for csv_file in csv_files:
    export_csv_file(os.path.join('C:/Users/User/Desktop/Moje dokumenty/test', csv_file), parents=['19iWVK54e8TuSnHEOfwvVt9l8UEpWDPNU'])