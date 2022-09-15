import glob
import shutil
import os
from datetime import datetime as dt
from datetime import timedelta
import pandas as pd
import numpy as np
import logging
import io
import msoffcrypto

from modules.db_connection import write_data_database, read_contractor_data

def save_data(employeeFileFolder, employeeFilePassword):
    #initialize the dataframes
    ftedf = pd.DataFrame()
    contractordf = pd.DataFrame()
    vaccdf = pd.DataFrame()
    pcrdf = pd.DataFrame()
    artdf = pd.DataFrame()

    Emp_file = os.path.join(employeeFileFolder,'*employee*.xlsx' )

    #Read data from employee files
    emp_file_list = glob.glob(Emp_file)

    if(len(emp_file_list) != 0):
        decrypted_workbook = io.BytesIO()
        latest_emp_file = max(emp_file_list, key=os.path.getctime)

        with open(latest_emp_file,'rb') as latest_emp_file:
            emp_file = msoffcrypto.OfficeFile(latest_emp_file)
            emp_file.load_key(password=employeeFilePassword)
            emp_file.decrypt(decrypted_workbook)

        ftedf = pd.read_excel(decrypted_workbook,dtype={'MOBILE NUMBER': 'str'})
    else:
        print("Employee file not found.. Terminating Program")
        logging.info("Employee file not found.. Terminating Program")
        exit()
    
    #Read Contractor data
    contractordf = read_contractor_data()

    #Read data from sync files
    vacc_file_list = glob.glob('SyncEmails/vaccination*.csv')
    vacc_file = max(vacc_file_list, key=os.path.getctime)
    vaccdf = pd.read_csv(vacc_file, na_filter = False)

    pcr_file_list = glob.glob('SyncEmails/pcr*.csv')
    pcr_file = max(pcr_file_list, key=os.path.getctime)
    pcrdf = pd.read_csv(pcr_file)

    art_file_list = glob.glob('SyncEmails/art*.csv')
    art_file = max(art_file_list, key=os.path.getctime)
    artdf = pd.read_csv(art_file)

    vaccdf['retrieved_at'] = pd.to_datetime(vaccdf['retrieved_at'])
    vaccdf['expiry_date'] = pd.to_datetime(vaccdf['expiry_date'],errors='coerce')
    pcrdf['retrieved_at'] = pd.to_datetime(pcrdf['retrieved_at'])
    pcrdf['timestamp'] = pd.to_datetime(pcrdf['timestamp'])
    artdf['retrieved_at'] = pd.to_datetime(artdf['retrieved_at'])
    artdf['produced_at'] = pd.to_datetime(artdf['produced_at'])
    vaccdf['weekend'] = vaccdf.apply(calculate_weekend,axis = 1)

    employeedf = combine_employee_data(ftedf,contractordf)

    vaccdf = identify_employee(employeedf,vaccdf)
    pcrdf = identify_employee(employeedf,pcrdf)
    artdf = identify_employee(employeedf,artdf)

    write_data_database(employeedf,"EMPLOYEE", ["NID"],"insert")
    write_data_database(vaccdf,"VACCINATION_STATUS", ["uin"],"insert")
    write_data_database(pcrdf,"PCR_RESULTS", ["uin","timestamp"],"insert")
    write_data_database(artdf,"ART_RES", ["uin","produced_at"],"insert")

def combine_employee_data(ftedf,contractordf):
    employee_column_names = ["NID","EMPLOYEETYPE","EMPLOYEENAME","GID","BUSINESSUNIT","DEPARTMENT","EMAILADDRESS","MOBILENUMBER"]
    employeedf = pd.DataFrame(columns = employee_column_names)

    ftedf.columns = ftedf.columns.str.replace(" ", "")
    
    ftedf["EMPLOYEETYPE"] = "FTE"
    contractordf["EMPLOYEETYPE"] = "Contractor"
    contractordf["BUSINESSUNIT"] = ""
    contractordf["EMPLOYEENAME"] = contractordf['SgIdenEmpFname'] + ' ' + contractordf['SgIdenEmpLname']

    tempContractordf = contractordf[["SgNricFin", "EMPLOYEETYPE", "EMPLOYEENAME","GlobalId","BUSINESSUNIT", "Department","CompanyEmail", "Phone"]].copy()
    tempContractordf.rename(columns = {"SgNricFin":"NID",
                        "EMPLOYEETYPE":"EMPLOYEETYPE",
                        "EMPLOYEENAME":"EMPLOYEENAME",
                        "GlobalId":"GID",
                        "BUSINESSUNIT":"BUSINESSUNIT",
                        "Department":"DEPARTMENT",
                        "CompanyEmail":"EMAILADDRESS",
                        "Phone":"MOBILENUMBER"}, inplace = True)

    #convert Contractor NID column to upper case to look for duplicates
    tempContractordf['NID'] =tempContractordf['NID'].astype(str).str.upper()

    # Find duplicate data
    #duplicateContractor = tempContractordf[tempContractordf.duplicated(subset='NID', keep=False)].sort_values("NID")

    #print("%s Duplicate rows found..." % len(duplicateContractor))
    #logging.info("%s Duplicate rows found..." % len(duplicateContractor))
    
    #print(duplicateContractor[['NID']])
    #logging.info(duplicateContractor[['NID']])


    # if (duplicateContractor.shape[0] >0 ):
    #         print("Dropping Duplicate rows...")
    #         logging.info("Dropping Duplicate rows...")
    #         tempContractordf.drop_duplicates(subset='NID', keep=False, inplace=True)

    employeedf = pd.concat([employeedf,ftedf,tempContractordf], ignore_index = True)
    employeedf["EmpID"] = range(1,len(employeedf) + 1)
    return employeedf

def calculate_weekend(row):
    if row['expiry_date'] is not pd.NaT:
        return row['expiry_date'] + timedelta(days = 6 - row['expiry_date'].weekday())
    return pd.NaT


def identify_employee(employeedf, datadf):
    datadf = pd.merge(datadf,employeedf[["NID","EmpID"]],left_on='uin', right_on = 'NID', how="left")
    datadf["EmpID"] =  datadf["EmpID"].astype('Int64')
    
    del datadf['NID']

    Contractordf = datadf[datadf.EmpID.isnull()]
    ftedf = datadf[datadf.EmpID.notnull()]
    Contractordf['PartialUIN'] = Contractordf['uin'].str[5:]
    employeedf['NID'] = employeedf['NID'].astype("string")
    contractordatadf = pd.merge(Contractordf, employeedf[["NID","EMPLOYEENAME","EmpID"]], left_on = ['PartialUIN','name'], right_on = ['NID','EMPLOYEENAME'], how = "left")

    del contractordatadf['NID']
    del contractordatadf['EMPLOYEENAME']
    del contractordatadf['PartialUIN']
    del contractordatadf['EmpID_x']

    contractordatadf.rename(columns = {"EmpID_y":"EmpID"}, inplace = True)

    newdatadf = pd.concat([ftedf,contractordatadf])
    return newdatadf

