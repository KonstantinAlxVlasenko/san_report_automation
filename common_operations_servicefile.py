'''Module to import data from service files (san_automation_info.xlsx and report_info.xlsx)'''

import os
import re
import sys
import xlrd
import pandas as pd
from common_operations_miscellaneous import status_info


def report_entry_values(max_title):
    """
    Function to import entry report values:
    customer_name, hardware configuration files, directory to save report 
    """

    report_entry_df = dataframe_import('report', max_title, 'report_info.xlsx', ['name', 'value'], 'name', display_status=False)

    customer_name = report_entry_df.loc['customer_name', 'value']
    project_folder = os.path.normpath(report_entry_df.loc['project_folder', 'value'])
    ssave_folder = os.path.normpath(report_entry_df.loc['supportsave_folder', 'value'])
    if not pd.isna(report_entry_df.loc['blade_showall_folder', 'value']):
        blade_folder = os.path.normpath(report_entry_df.loc['blade_showall_folder', 'value'])
    else:
        blade_folder = None

    return customer_name, project_folder, ssave_folder, blade_folder


def columns_import(sheet_title, max_title, *args, 
                    init_file = 'san_automation_info.xlsx', display_status=True):
    """Function to import corresponding columns from init file.
    Can import several columns.
    """

    # file to store all required data to process configuratin files
    # default init_file  is 'san_automation_info.xlsx'
    # columns titles string to display imported columns list without parenthesis
    columns_str = ""
    for arg in args:
        columns_str += "'" + arg + "', "
    columns_str = columns_str.rstrip(", ")

    if display_status:
        info = f'Importing {columns_str} from {sheet_title} tab'
        print(info, end = ' ')
    # try read data in excel
    try:
        columns = pd.read_excel(init_file, sheet_name = sheet_title, usecols = args, squeeze=True)
    except FileNotFoundError:
        if display_status:
            status_info('fail', max_title, len(info))
        print(f'File not found. Check if file {init_file} exist.')
        sys.exit()
    except ValueError:
        if display_status:
            status_info('fail', max_title, len(info))
        print(f'Column(s) {columns_str} not found. Check if column exist in {sheet_title}.')
        sys.exit()        
    else:
        # if number of columns to read > 1 than returns corresponding number of lists
        if len(args)>1:
            columns_names = [columns[arg].dropna().tolist() if not columns[arg].empty else None for arg in args]
        else:
            columns_names = columns.dropna().tolist()
        if display_status:
            status_info('ok', max_title, len(info))
    
    return columns_names


def dataframe_import(sheet_title, max_title, init_file = 'san_automation_info.xlsx', 
                        columns = None, index_name = None, header = 0, display_status=True):
    """Function to import dataframe from exel file"""

    init_file_base = os.path.basename(init_file)
    # file to store all required data to process configuratin files
    # init_file = 'san_automation_info.xlsx'
    if display_status:   
        info = f'Importing {sheet_title} dataframe from {init_file_base} file'
        print(info, end = ' ')
    # try read data in excel
    try:
        dataframe = pd.read_excel(init_file, sheet_name = sheet_title, usecols = columns, index_col = index_name, header = header)
    # if file is not found
    except FileNotFoundError:
        if display_status:
            status_info('fail', max_title, len(info))
        print(f'File not found. Check if file {init_file} exists.')
        sys.exit()
    # if sheet is not found
    except xlrd.biffh.XLRDError:
        if display_status:
            status_info('fail', max_title, len(info))
        print(f'Sheet {sheet_title} not found in {init_file}. Check if it exists.')
        sys.exit()
    else:
        if display_status:
            status_info('ok', max_title, len(info))
    
    return dataframe


def data_extract_objects(sheet_title, max_title, param_columns = True):
    """Function imports parameters names and regex tepmplates
    to extract required data from configuration files   
    """
    if param_columns:
        # imports keys to extract switch parameters from tmp dictionary
        params_names, params_add_names = columns_import(sheet_title, max_title, 'params', 'params_add')
    else:
        params_names, params_add_names = None, None
    # imports base names for compile and match templates and creates corresonding names
    keys = columns_import(sheet_title, max_title, 're_names')
    comp_keys = [key+'_comp' for key in keys]
    match_keys = [key + '_match' for key in keys]
    # imports string for regular expressions
    comp_values = columns_import(sheet_title,  max_title, 'comp_values')
    # creates regular expressions
    comp_values_re = [re.compile(fr"{element}") for element in comp_values]
    # creates dictionary with regular expressions  
    comp_dct = dict(zip(comp_keys, comp_values_re))
    
    return params_names, params_add_names, comp_keys, match_keys, comp_dct


def dct_from_columns(sheet_title, max_title, *args, init_file = 'report_info.xlsx', display_status=True):
    """Function imports columns and create dictionary
    If only one column imported then dictionary with keys and empty lists as values created
    If several columns imported then first column is keys of dictionary and others are values
    """

    # info string in case if not possible to create dictionary
    info = f'{args} columns have different length. Not able to create dictionary. Check data in {sheet_title} tab'

    # if one column is passed then create dictionary with keys and empty lists as values for each key
    if len(args) == 1:
        keys = columns_import(sheet_title, max_title, *args, init_file=init_file, display_status=display_status)
        dct = dict((key, []) for key in keys)
    # if two columns passed then create dictionary of keys with one value for each key
    elif len(args) == 2:
        keys, values = columns_import(sheet_title, max_title, *args, init_file=init_file, display_status=display_status)
        # if columns have different number of elements throw information string and exit
        if len(keys) != len(values):
            print(info)
            sys.exit()                    
        dct ={key: value for key, value in zip(keys, values)}
    # if morte than two columns passed then create dictionary of keys with list of values for each key
    elif len(args) > 2:
        # first column is keys rest columns are in values list of lists
        keys, *values = columns_import(sheet_title, max_title, *args, init_file=init_file, display_status=display_status)
        # check if all imported columns have equal length to create dictionary
        # create set of columns length with set comprehension method
        columns_len_set = {len(columns_title) for columns_title in [keys, *values]}
        # columns length set contains more than 1 element show information string
        if len(columns_len_set) != 1:
            print(info)
            sys.exit()
        # dictionary with key and value as list of lists 
        dct ={key: value for key, *value in zip(keys, *values)}
    else:
        print('Not sufficient data to create dictionary')
        sys.exit()

    return dct


