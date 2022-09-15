
from audioop import reverse
from fileinput import filename
import pandas as pd
import win32com.client
import os
from datetime import datetime as dt
from datetime import timedelta
import pyzipper
import glob
import shutil
import argparse
from pathlib import Path
import imaplib, email
import numpy as np
from sys import exit
import pyodbc as pyodbc
import logging
import time

from modules.process_data import save_data
from modules.download import download_attachment

import warnings
warnings.filterwarnings("ignore")

# %%

#Preparing the log file
logPath = os.path.join(os.getcwd(),'logs')

today = dt.today()
log_datetime = today.strftime("%d-%m-%Y %H-%M")

logging.basicConfig(filename=logPath + '\\' + log_datetime + '.log',level=logging.DEBUG)
logging.info("Start "+ dt.now().strftime("%H:%M:%S"))


# Read config file
config_path = os.path.join(os.getcwd(), 'Config\\Config.xlsx' )
print('Reading Config file..')
logging.info('Reading Config file..')

config_df_dict = pd.read_excel(config_path, sheet_name="config")
config_df_dict.set_index('Name',inplace=True)
email_sender = config_df_dict.loc["Sender email"][0]
email_subject = config_df_dict.loc["Email Subject"][0]
outputDir = config_df_dict.loc["Sync File Folder"][0]
password = config_df_dict.loc["Sync File Password"][0].encode('utf_8')
recipientAccount = config_df_dict.loc["Recipient email id"][0]
emailPassword = config_df_dict.loc["Email Password"][0]
emailUrl    = config_df_dict.loc["Email URL"][0]
employeeFileFolder = config_df_dict.loc["Employee File Folder"][0]
employeeFilePassword = config_df_dict.loc["Employee File Password"][0]
 
# Access email
print("Accessing Email..")
logging.info("Accessing Email..")

download_attachment(email_sender,email_subject,outputDir,recipientAccount,emailPassword,emailUrl)

# extract all files to a location

folder_list = glob.glob('SyncEmails/data_' + '*.zip')

if len(folder_list) == 0:
    print("No unseen emails found.. Terminating Program")
    logging.info("No unseen emails found.. Terminating Program")
    exit()

filename = max(folder_list, key=os.path.getctime)

logging.info("Extracting zipped files..")
print("Extracting zipped files..")
try:
    SyncZip = pyzipper.AESZipFile(filename, mode='r')
    SyncZip.setpassword(password)
    SyncZip.extractall(path=outputDir)
    SyncZip.close()
    filenamenew = filename.rsplit('.',1)[0] + '_' + dt.now().strftime('%d%m%Y%H%M%S') + '.zip'
    os.rename(filename,filenamenew)
    shutil.move(filenamenew,'SyncEmails/Archives')
except Exception as e:
    print("Error while extracting zip file :" + str(e))
    logging.info("Error while extracting zip file :" + str(e))
    exit()

#Read and save data from files
save_data(employeeFileFolder, employeeFilePassword)
print("Data saved to database!")
logging.info("Data saved to database!")

print("Cleaning up..")
logging.info("Cleaning up..")

list_of_files = glob.glob('SyncEmails/*') 
list_of_files.remove('SyncEmails\\Archives')

for file in list_of_files:
    try:
        os.remove(file)
    except Exception as e:
        print("Error while deleting file:" + str(e))
        logging.info("Error while deleting file:" + str(e))

logging.info("End "+ dt.now().strftime("%H:%M:%S"))




