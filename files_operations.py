import sys
import os.path
import openpyxl
import pandas as pd
from os import makedirs

'''Module to perform operations with files (create folder, save data to excel file, import data from excel file)'''


# function to create any folder with 'path' 
def create_folder(path):
    
    info = f'Make directory {os.path.basename(path)}'
    print(info, end= '')    
    if not os.path.exists(path):        
        try:
            os.makedirs(path)
        except OSError:
            print('FAIL'.rjust(125-len(info), '.'))
            print(f'Not possible create directory {path}.')
            print('Code execution finished')
            sys.exit()
        else:
            # print(f'Successfully created directory {path}.')
            print('OK'.rjust(125-len(info), '.'))
    else:
        # print(f'Directory {path} already exists.')
        print('SKIP'.rjust(125-len(info), '.'))
        
        
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
    string_length = str_length-len(info)
    file_path = os.path.join(report_path, file_name)
    
    # pd.ExcelWriter has only two file open modes
    # if file doesn't exist it has be opened in "w" mode
    if os.path.isfile(file_path):
        file_mode = 'a'
        workbook = openpyxl.load_workbook(file_path)
        if sheet_title in workbook.sheetnames:
            import_df = pd.read_excel(file_path, sheet_name=sheet_title)
            if not data_frame.equals(import_df):
                if len(workbook.sheetnames) != 1:
                    file_mode = 'a' 
                    del workbook[sheet_title]
                    workbook.save(file_path)
                else:
                    file_mode = 'w'                                                    
                export_dataframe(file_path, data_frame, sheet_title, file_mode,string_length)
            else:
                print('SKIP'.rjust(str_length-len(info), '.'))
        else:
            export_dataframe(file_path, data_frame, sheet_title, file_mode,string_length)                    
    else:
        file_mode = 'w'
        export_dataframe(file_path, data_frame, sheet_title, file_mode,string_length)
        


def export_dataframe(file_path, data_frame, sheet_title, file_mode, string_length):
    
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl',  mode=file_mode) as writer:
            data_frame.to_excel(writer, sheet_name=sheet_title, index=False)
    except PermissionError:
        print('FAIL'.rjust(string_length, '.'))
        print('Permission denied. Close the file.')
        sys.exit()
    else:
        print('OK'.rjust(string_length, '.'))
    


# function to import corresponding columns from san_automation_info.xlsx file  
def columns_import(column_name, max_title):
    
    init_file = 'san_automation_info.xlsx'
    # information string length in terminal
    str_length = max_title + 46
    
    info = f'\nImporting {column_name} columns group from {init_file}'
    print(info, end = ' ')
    try:
        columns = pd.read_excel(init_file, sheet_name = 'parameters', usecols =[column_name], squeeze=True)
        columns = columns.dropna().tolist()
    except:
        print('FAIL'.rjust(str_length-len(info), '.'))
    else:
        print('OK'.rjust(str_length-len(info), '.'))
    
    return columns