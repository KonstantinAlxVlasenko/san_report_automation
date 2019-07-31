import sys
import os.path
import pandas as pd
from os import makedirs

'''Module to perform operations with files (create folder, save data to excel file, import data from excel file)'''


# function to create any folder with 'path' 
def create_folder(path):    
    if not os.path.exists(path):        
        try:
            os.makedirs(path)
        except OSError:
            print(f'Not possible create directory {path}.')
            print('Code execution finished')
            sys.exit()
        else:
            print(f'Successfully created directory {path}.')
    else:
        print(f'Directory {path} already exists.')
        
        
# function to check if folder exist
def check_valid_path(path):
    path = os.path.normpath(path)
    if not os.path.isdir(path):
        print(f"{path} folder doesn't exist")
        print('Code execution finished')
        sys.exit()
        

# saving data to excel file
def save_xlsx_file(data_frame, sheet_title, customer_name, report_type, report_path, max_title):
    
    # information string length in terminal
    str_length = max_title + 46
    # construct excel filename
    file_name = customer_name + '_' + report_type + '_' + 'report'+'.xlsx'
    # information string
    info = f'\nExporting {sheet_title} to {file_name}'
    print(info, end =" ")
    file_path = os.path.join(report_path, file_name)
    
    # pd.ExcelWriter has only two file open modes
    # if file doesn't exist it has be opened in "w" mode
    if os.path.isfile(file_path):
        file_mode = 'a'
    else:
        file_mode = 'w'
        
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl',  mode=file_mode) as writer:
            data_frame.to_excel(writer, sheet_name=sheet_title, index=False)
    except PermissionError:
        print('FAIL'.rjust(str_length-len(info), '.'))
        print('Permission denied. Close the file.')
        sys.exit()
    else:
        print('OK'.rjust(str_length-len(info), '.'))


# function to import corresponding columns from san_assessment_init_file.xlsx file  
def columns_import(column_name, switch_init_file):
    
    print(f'Importing {column_name} columns group for port_online_df DataFrame')
    columns = pd.read_excel(switch_init_file, sheet_name = 'Sheet2', usecols =[column_name], squeeze=True)
    columns = columns.dropna().tolist()
    
    return columns