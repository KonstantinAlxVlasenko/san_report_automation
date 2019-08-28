import sys
import os.path
import openpyxl
import re
import json
import pandas as pd
from os import makedirs
from datetime import date

'''Module to perform operations with files (create folder, save data to excel file, import data from excel file)'''


def create_folder(path):
    """Function to create any folder with 'path' 
    """    
    info = f'Make directory {os.path.basename(path)}'
    print(info, end= '')
    # if folder not exist create 
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
    # otherwise print 'SKIP' status
    else:
        status_info('skip', 82, len(info))
        
        
def check_valid_path(path):
    """Function to check if folder exist
    """
    path = os.path.normpath(path)
    if not os.path.isdir(path):
        print(f"{path} folder doesn't exist")
        print('Code execution exit')
        sys.exit()
        
        
# saving DataFrame to excel file
def save_xlsx_file(data_frame, sheet_title, report_data_lst, report_type = 'service'):   
    """Check if excel file exists, check if dataframe sheet is in file, 
    if new dataframe is equal to one in excel file than skip,
    otherwise delete sheet with stored dataframe (if sheet tabs number > 1)
    and save new dataframe 
    """    
    customer_name, report_path, _, max_title = report_data_lst      
    current_date = str(date.today())
    # construct excel filename
    file_name = customer_name + '_' + report_type + '_' + 'report_' + current_date + '.xlsx'
    # information string
    info = f'Exporting {sheet_title} to {file_name}'
    print(info, end =" ")
    file_path = os.path.join(report_path, file_name)
    
    # pd.ExcelWriter has only two file open modes
    # if file doesn't exist it has be opened in "w" mode otherwise in "a"    
    if os.path.isfile(file_path):
        file_mode = 'a'
        # open existing excel file
        workbook = openpyxl.load_workbook(file_path)
        if sheet_title in workbook.sheetnames:
            # # import stored data in excel file do dataframe
            # import_df = pd.read_excel(file_path, sheet_name=sheet_title)
            # # check if new dataframe and stored in excelfile are equal
            # if not data_frame.equals(import_df):
            #     # if dataframes are not equal delete sheet
                # after sheet removal excel file must contain at least one sheet
            if len(workbook.sheetnames) != 1:                    
                del workbook[sheet_title]
                try:
                    workbook.save(file_path)
                except PermissionError:
                    status_info('fail', max_title, len(info))
                    print('Permission denied. Close the file.')
                    sys.exit()
            else:
                file_mode = 'w'
                # export new dataframe
                # export_dataframe(file_path, data_frame, sheet_title, file_mode,str_length_status)
            # # if dataframes are equal then skip
            # else:
            #     file_mode = None
            #     status_info('skip', max_title, len(info))                  
    else:
        file_mode = 'w'
    
    # if required export DataFrame to the new sheet
    if file_mode:
        try:
            with pd.ExcelWriter(file_path, engine='openpyxl',  mode=file_mode) as writer:
                data_frame.to_excel(writer, sheet_name=sheet_title, index=False)
        except PermissionError:
            status_info('fail', max_title, len(info))
            print('Permission denied. Close the file.')
            sys.exit()
        else:
            status_info('ok', max_title, len(info))        
    

def status_info(status, max_title, len_info_string, shift=0):
    """Function to print current operation status ('OK', 'SKIP', 'FAIL')
    """    
    # information + operation status string length in terminal
    str_length = max_title + 55 + shift
    status = status.upper()
    # status info aligned to the right side
    # space between current operation information and status of its execution filled with dots
    print(status.rjust(str_length - len_info_string, '.'))

 
def columns_import(sheet_title, max_title, *args):
    """Function to import corresponding columns from san_automation_info.xlsx file
    Can import several columns
    """
    # file to store all required data to process configuratin files
    init_file = 'san_automation_info.xlsx'   
    info = f'Importing {args} columns group from {init_file} file {sheet_title} tab'
    print(info, end = ' ')
    # try read data in excel
    try:
        columns = pd.read_excel(init_file, sheet_name = sheet_title, usecols =args, squeeze=True)
    except FileNotFoundError:
        status_info('fail', max_title, len(info))
        print(f'File not found. Check if file {init_file} exist.')
        sys.exit()
    else:
        # if number of columns to read > 1 than returns corresponding number of lists
        if len(args)>1:
            columns_names = [columns[arg].dropna().tolist() if not columns[arg].empty else None for arg in args]
        else:
            columns_names = columns.dropna().tolist()
        status_info('ok', max_title, len(info))
    
    return columns_names

def data_extract_objects(sheet_title, max_title):
    """Function imports parameters names and regex tepmplates
    to extract required data from configuration files   
    """
    # imports keys to extract switch parameters from tmp dictionary
    params_names, params_add_names = columns_import(sheet_title, max_title, 'params', 'params_add')
    # imports base names for compile and match templates and creates corresonding names
    keys = columns_import(sheet_title, max_title, 're_names')
    comp_keys = [key+'_comp' for key in keys]
    match_keys = [key + '_match' for key in keys]
    # imports string for regular expressions
    comp_values = columns_import(sheet_title,  max_title, 'comp_values')
    # creates regular expressions
    comp_values_re = [re.compile(element) for element in comp_values]
    # creates dictionary with regular expressions  
    comp_dct = dict(zip(comp_keys, comp_values_re))
    
    return params_names, params_add_names, comp_keys, match_keys, comp_dct


def export_lst_to_excel(data_lst, report_data_lst, sheet_title_export, sheet_title_import = None, 
                        columns = columns_import, columns_title_import = 'columns'):
    """Function to export list to DataFrame and then save it to excel report file
    returns DataFrame
    """    
    *_, max_title = report_data_lst 
    
    # checks if columns were passed to function as a list
    if isinstance(columns, list):
        columns_title = columns
    # if not (default) then import columns from excel file
    else:
        columns_title =columns(sheet_title_import, max_title, columns_title_import)
    data_df = pd.DataFrame(data_lst, columns= columns_title)
    save_xlsx_file(data_df, sheet_title_export, report_data_lst)
    
    return data_df


def data_to_json(report_data_list, data_names, *args):
    """Function to export extracted configuration data to JSON file 
    """
    
    customer_name, _, json_data_dir, max_title = report_data_list
    
    for data_name, data_exported in zip(data_names, args):
        file_name = customer_name + '_' + data_name + '.json'
        file_path = os.path.join(json_data_dir, file_name)
        info = f'Exporting {data_name} to {file_name} file'
        try:
            print(info, end =" ")
            with open(file_path, 'w') as file:
                json.dump(data_exported, file)
        except:
            status_info('fail', max_title, len(info))
        else:
            status_info('ok', max_title, len(info))
            
            
def json_to_data(report_data_list, *args):
    """Function to import data from JSON file to data object 
    """
    
    customer_name, _, json_data_dir, max_title = report_data_list
    data_imported = []
    
    for data_name in args:
        file_name = customer_name + '_' + data_name + '.json'
        file_path = os.path.join(json_data_dir, file_name)        
        info = f'Importing {data_name} from {file_name} file'
        print(info, end =" ")
        try:  
            with open(file_path, 'r') as file:
                data_imported.append(json.load(file))
        except:
            status_info('no data', max_title, len(info))
            data_imported.append(None)
        else:
            status_info('ok', max_title, len(info))
    
    return data_imported


def line_to_list(re_object, line, *args):
    """Function to extract values from line with regex object 
    and combine values with other optional data into list
    """
    values, = re_object.findall(line)
    values_lst = [value.rstrip() if value else None for value in values]
    return [*args, *values_lst]
        
    
         