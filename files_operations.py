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
            status_info('fail', 82, len(info))
            print(f'Not possible create directory {path}.')
            print('Code execution finished')
            sys.exit()
        else:
            status_info('ok', 82, len(info))
    else:
        status_info('skip', 82, len(info))
        
        
# function to check if folder exist
def check_valid_path(path):
    path = os.path.normpath(path)
    if not os.path.isdir(path):
        print(f"{path} folder doesn't exist")
        print('Code execution exit')
        sys.exit()
        

# saving DataFrame to excel file
def save_xlsx_file(data_frame, sheet_title, customer_name, report_type, report_path, max_title):   
    """Check if excel file exists, check if dataframe sheet is in file, 
    if new dataframe is equal to one in excel file than skip,
    otherwise delete sheet with stored dataframe (if sheet tabs number > 1)
    and save new dataframe 
    """
    # construct excel filename
    file_name = customer_name + '_' + report_type + '_' + 'report'+'.xlsx'
    # information string
    info = f'\nExporting {sheet_title} to {file_name}'
    print(info, end =" ")
    file_path = os.path.join(report_path, file_name)
    
    # pd.ExcelWriter has only two file open modes
    # if file doesn't exist it has be opened in "w" mode otherwise in "a"    
    if os.path.isfile(file_path):
        file_mode = 'a'
        # open existing excel file
        workbook = openpyxl.load_workbook(file_path)
        if sheet_title in workbook.sheetnames:
            # import stored data in excel file do dataframe
            import_df = pd.read_excel(file_path, sheet_name=sheet_title)
            # check if new dataframe and stored in excelfile are equal
            if not data_frame.equals(import_df):
                # if dataframes are not equal delete sheet
                # after sheet removal excel file must contain at least one sheet
                if len(workbook.sheetnames) != 1:                    
                    del workbook[sheet_title]
                    workbook.save(file_path)
                else:
                    file_mode = 'w'
                # export new dataframe
                # export_dataframe(file_path, data_frame, sheet_title, file_mode,str_length_status)
            # if dataframes are equal then skip
            else:
                file_mode = None
                status_info('skip', max_title, len(info), 1)                  
    else:
        file_mode = 'w'
    
    # if required export DataFrame to the new sheet
    if file_mode:
        try:
            with pd.ExcelWriter(file_path, engine='openpyxl',  mode=file_mode) as writer:
                data_frame.to_excel(writer, sheet_name=sheet_title, index=False)
        except PermissionError:
            status_info('fail', max_title, len(info), 1)
            print('Permission denied. Close the file.')
            sys.exit()
        else:
            status_info('ok', max_title, len(info), 1)        
    
# print current operation status ('OK', 'SKIP', 'FAIL')
def status_info(status, max_title, len_info_string, shift=0):
    
    # information + operation status string length in terminal
    str_length = max_title + 45 + shift
    status = status.upper()
    print(status.rjust(str_length - len_info_string, '.'))
    

# function to import corresponding columns from san_automation_info.xlsx file  
def columns_import(sheet_title, column_name, max_title):
    
    init_file = 'san_automation_info.xlsx'
    
    info = f'\nImporting {column_name} columns group from {init_file}'
    print(info, end = ' ')
    try:
        columns = pd.read_excel(init_file, sheet_name = sheet_title, usecols =[column_name], squeeze=True)
        columns = columns.dropna().tolist()
    except:
        status_info('fail', max_title, len(info), 1)
    else:
        status_info('ok', max_title, len(info), 1)
        print('\n')
    
    return columns