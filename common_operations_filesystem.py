'''Module to perform operations with files (create folder, save data to excel file, import data from excel file)'''

import json
import os
import re
import sys
import numpy as np
import pandas as pd
from common_operations_miscellaneous import status_info


def create_folder(path, max_title):
    """Function to create any folder with path"""

    info = f'Make directory {os.path.basename(path)}'
    print(info, end = ' ')
    # if folder not exist create 
    if not os.path.exists(path):        
        try:
            os.makedirs(path)
        except OSError:
            status_info('fail', max_title, len(info))
            print(f'Not possible create directory {path}.')
            print('Code execution finished')
            sys.exit()
        else:
            status_info('ok', max_title, len(info))
    # otherwise print 'SKIP' status
    else:
        status_info('skip', max_title, len(info))
        
     
def check_valid_path(path):
    """Function to check if folder exist"""

    path = os.path.normpath(path)
    if not os.path.isdir(path):
        print(f"{path} folder doesn't exist")
        print('Code execution exit')
        sys.exit()
        

def save_data(report_constant_lst, data_names, *args):
    """
    Function to export extracted configuration data to JSON or CSV file
    depending on data passed 
    """

    customer_name, _, json_data_dir, max_title = report_constant_lst    
    # data_names it's a list of names for data passed as args

    for data_name, data_exported in zip(data_names, args):
        empty_data = False
        file_name = customer_name + '_' + data_name
        # adding file extenson depending from type of data saved
        # csv for DataFrame
        if isinstance(data_exported, pd.DataFrame):
            file_name += '.csv'
        # for all other types is json
        else:
            # create file name and path for json file
            file_name += '.json'
        # full file path
        file_path = os.path.join(json_data_dir, file_name)
        info = f'Saving {data_name} to {file_name} file'
        try:
            print(info, end =" ")
            with open(file_path, 'w', encoding="utf-8") as file:
                # savng data for DataFrame
                if isinstance(data_exported, pd.DataFrame):
                    # check if DataFrame have MultiIndex
                    # reset index if True due to MultiIndex is not saved                    
                    if isinstance(data_exported.index, pd.MultiIndex):
                        data_exported_flat = data_exported.reset_index()
                    # keep indexing if False
                    else:
                        data_exported_flat = data_exported.copy()
                    # when DataFrame is empty fill first row values
                    # with information string
                    if data_exported_flat.empty:
                        empty_data = True
                        if len(data_exported_flat.columns) == 0:
                            data_exported_flat['EMPTY'] = np.nan
                        data_exported_flat.loc[0] = 'NO DATA FOUND'
                    # save single level Index DataFrame to csv
                    data_exported_flat.to_csv(file, index=False)

                # for all other types
                else:
                    # if data list is not empty
                    if not len(data_exported):
                        empty_data = True 
                        data_exported = 'NO DATA FOUND'
                    json.dump(data_exported, file)
        # display writing data to file status
        except FileNotFoundError:
            status_info('fail', max_title, len(info))
        else:
            if not empty_data:
                status_info('ok', max_title, len(info))
            else:
                status_info('empty', max_title, len(info))
                

def load_data(report_constant_lst, *args):
    """Function to load data from JSON or CSV file to data object
    Detects wich type of data required to be loaded automaticaly
    Returns list with imported data 
    """
        
    customer_name, _, json_data_dir, max_title = report_constant_lst
    # list to store loaded data
    data_imported = []
    
    # check real file path
    for data_name in args:
        # constructing filenames for json and csv files
        file_name_json = customer_name + '_' + data_name + '.json'
        file_path_json = os.path.join(json_data_dir, file_name_json)
        file_name_csv = customer_name + '_' + data_name + '.csv'
        file_path_csv = os.path.join(json_data_dir, file_name_csv)
        # flags file existance
        file_json = False
        file_csv = False
        # check if json file exist
        if os.path.isfile(file_path_json):
            file_path = file_path_json
            file_name = file_name_json
            file_json = True
        # check if csv file exist
        elif os.path.isfile(file_path_csv):
            file_name = file_name_csv
            file_path = file_path_csv
            file_csv = True
        # if no files exist then dislay info about no data availabilty
        # and add None to data_imported list
        else:
            info = f'Loading {data_name}'
            print(info, end =" ")
            status_info('no file', max_title, len(info))
            data_imported.append(None)
        
        # when saved file founded 
        if any([file_json, file_csv]):                   
            info = f'Loading {data_name} from {file_name} file'
            print(info, end =" ")
            # open file
            try:  
                with open(file_path, 'r', encoding="utf-8") as file:
                    # for json file use load method
                    if file_json:
                        data_imported.append(json.load(file))
                    # for csv file use read_csv method
                    elif file_csv:
                        data_imported.append(pd.read_csv(file, dtype='unicode'))
            # display file load status
            except:
                status_info('no data', max_title, len(info))
                data_imported.append(None)
            else:
                status_info('ok', max_title, len(info))
    return data_imported


def find_files(folder, max_title, filename_contains='', filename_extension=''):
    """
    Function to create list with files. Takes directory, regex_pattern to verify if filename
    contains that pattern (default empty string) and filename extension (default is empty string)
    as parameters. Returns list of files with the extension deteceted in root folder defined as
    folder parameter and it's nested folders. If both parameters are default functions returns
    list of all files in directory
    """

    info = f'Checking {os.path.basename(folder)} folder for configuration files'
    print(info, end =" ") 

    # check if ssave_path folder exist
    check_valid_path(folder)
   
    # list to save configuration data files
    files_lst = []

    # going through all directories inside ssave folder to find configuration data
    for root, _, files in os.walk(folder):
        for file in files:
            if file.endswith(filename_extension) and re.search(filename_contains, file):
                file_path = os.path.normpath(os.path.join(root, file))
                files_lst.append(file_path)

    if len(files_lst) == 0:
        status_info('no data', max_title, len(info))
    else:
        status_info('ok', max_title, len(info))        
    return files_lst


         
