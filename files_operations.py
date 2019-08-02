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
    
    """Check if excel file exists, check if dataframe sheet in file, 
    if new dataframe is equal to one in excel file than skip,
    otherwise delete sheetname with stored datadrame and save new dataframe 
    """
    
    # information string length in terminal
    str_length = max_title + 46
    # construct excel filename
    file_name = customer_name + '_' + report_type + '_' + 'report'+'.xlsx'
    # information string
    info = f'\nExporting {sheet_title} to {file_name}'
    print(info, end =" ")
    str_length_status = str_length-len(info)
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
                export_dataframe(file_path, data_frame, sheet_title, file_mode,str_length_status)
            # if dataframes are equal then skip
            else:
                print('SKIP'.rjust(str_length-len(info), '.'))
        else:
            export_dataframe(file_path, data_frame, sheet_title, file_mode,str_length_status)                    
    else:
        file_mode = 'w'
        export_dataframe(file_path, data_frame, sheet_title, file_mode,str_length_status)
        

# export DataFrame to excel file
def export_dataframe(file_path, data_frame, sheet_title, file_mode, str_length_status):
    
    try:
        with pd.ExcelWriter(file_path, engine='openpyxl',  mode=file_mode) as writer:
            data_frame.to_excel(writer, sheet_name=sheet_title, index=False)
    except PermissionError:
        print('FAIL'.rjust(str_length_status, '.'))
        print('Permission denied. Close the file.')
        sys.exit()
    else:
        print('OK'.rjust(str_length_status, '.'))
    


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