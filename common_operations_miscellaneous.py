"""
Module with auxiliary functions to collect data from configuration files
and  perform operations on data
"""

import re

import pandas as pd

# from common_operations_filesystem import save_xlsx_file
# from common_operations_servicefile import columns_import


def status_info(status, max_title, len_info_string, shift=0):
    """Function to print current operation status ('OK', 'SKIP', 'FAIL')"""

    # information + operation status string length in terminal
    str_length = max_title + 80 + shift
    status = status.upper()
    # status info aligned to the right side
    # space between current operation information and status of its execution filled with dots
    print(status.rjust(str_length - len_info_string, '.'))


def line_to_list(re_object, line, *args):
    """Function to extract values from line with regex object 
    and combine values with other optional data into list
    """

    values, = re_object.findall(line)
    if isinstance(values, tuple) or isinstance(values, list):
        values_lst = [value.rstrip() if value else None for value in values]
    else:
        values_lst = [values.rstrip()]
    return [*args, *values_lst]


def update_dct(keys, values, dct, char = ', '):
    """Function to add param_add:value pairs
    to the dictionary with discovered parameters
    """

    for key, value in zip(keys, values):
        if value:                
            if isinstance(value, set) or isinstance(value, list):
                value = f'{char}'.join(value)
            dct[key] = value
    return dct


def force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title):
    """
    Function to check if force data extract key is ON
    If all data from data list are present and True(non-zero) but extract key is ON
    Then function prints for which data force extraction has been initiated
    Returns list with data check results 
    """

    # list to store data check results
    data_check = []
    # check each data in data_lst
    for data in data_lst:
        # if data was collected or analyzed before 'load' function returns an object
        # even if object is empty True is added to data_check lst since there is no
        # necessity to recollect data 
        if data is not None:
            data_check.append(True)
        else:
            data_check.append(False)
    # when all data are not empty but force exctract is ON
    # print data names for which extraction is forced
    if all(data_check) and any(force_extract_keys_lst):
        # list for which data extraction is forced
        force_extract_names_lst = [data_name for data_name, force_extract_key in zip(data_names, force_extract_keys_lst) if force_extract_key]
        info = f'Force {", ".join(force_extract_names_lst)} data extract initialize'
        print(info, end =" ")
        status_info('ok', max_title, len(info))
        
    return data_check


def verify_data(report_data_list, data_names, *args):
    """
    Function to verify if loaded json file contains 'NO DATA FOUND' information string.
    If yes then data converted to empty list [] otherwise remains unchanged.
    Function implemented to avoid multiple collection of parametes not applicable
    for the current SAN (fcr, ag, porttrunkarea) 
    """

    *_, max_title, _ = report_data_list
    
    # list to store verified data
    verified_data_lst = []
    for data_name, data_verified in zip(data_names, args):
        info = f'Verifying {data_name}'
        print(info, end =" ")
        # if json file contains NO DATA information string
        if data_verified == 'NO DATA FOUND':
            data_verified = []
            status_info('no data', max_title, len(info))
        else:
            status_info('ok', max_title, len(info))
        verified_data_lst.append(data_verified)

    return verified_data_lst



