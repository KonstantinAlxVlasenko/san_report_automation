"""
Module with auxiliary functions to collect data from configuration files
and  perform operations on data
"""

import re

import pandas as pd
from functools import wraps


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

    return status


def display_status(info, max_title):
    def dec(fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            print(info, end =" ")
            result = fn(*args, **kwargs)
            status_info('ok', max_title, len(info))
            return result
        return inner
    return dec



def line_to_list(re_object, line, *args):
    """
    Function to extract values from line with regex object 
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
        force_extract_names_lst = \
            [data_name for data_name, force_extract_key in zip(data_names, force_extract_keys_lst) if force_extract_key]

        # force_extract_names_str add elenments of force_extract_names_lst until it's length 
        # exceeds max_title to avoid info string truncation 
        force_extract_names_str = ''
        item_count = 0
        for name in force_extract_names_lst:
            if not force_extract_names_str:
                force_extract_names_str = name
                item_count += 1
            else: 
                if len(force_extract_names_str) < max_title:
                    force_extract_names_str = force_extract_names_str + ', ' + name
                    item_count += 1
        if item_count < len(force_extract_names_lst):
            force_extract_names_str = force_extract_names_str + ', etc'

        info = f'Force {force_extract_names_str} invoke'
        print(info, end =" ")
        status_info('ok', max_title, len(info))
        
    return data_check


def verify_data(report_data_list, data_names, *args,  show_status=True):
    """
    Function to verify if loaded json file contains 'NO DATA FOUND' information string.
    If yes then data converted to empty list [] or DataFrame otherwise remains unchanged.
    Function implemented to avoid multiple collection and analysis of parametes not applicable
    for the current SAN (fcr, ag, porttrunkarea) 
    """

    *_, max_title, _ = report_data_list
    
    # list to store verified data
    verified_data_lst = []
    for data_name, data_verified in zip(data_names, args):
        if show_status:
            info = f'Verifying {data_name}'
            print(info, end =" ")
        # if data is DataFrame
        if isinstance(data_verified, pd.DataFrame):
            if data_verified.iloc[0, 0] == 'NO DATA FOUND':
                # reset DataFrame (leaves columns title only)
                data_verified = data_verified.iloc[0:0]
                if show_status:
                    status_info('empty', max_title, len(info))
            else:
                if show_status:
                    status_info('ok', max_title, len(info))
        # for other type of data
        else:
            # if json file contains NO DATA information string
            if data_verified == 'NO DATA FOUND':
                # transorm data to empty list
                data_verified = []
                if show_status:
                    status_info('empty', max_title, len(info))
            else:
                if show_status:
                    status_info('ok', max_title, len(info))
        verified_data_lst.append(data_verified)

    return verified_data_lst if len(args) > 1 else verified_data_lst[0]


def verify_force_run(data_names, data_lst, report_steps_dct, max_title, analyzed_data_names = []):
    """
    Function to check if it is required to run procedure for data collection from configuration files
    or collected data analysis. If data file doesn't exist (data_lst) or force run explicitly requested by user
    for the data function returns (data_names) or for data used during method execution (analyzed_data_names).
    Returns True or False.
    """

    force_run = False

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations 
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # list with True (if data loaded) and/or False (if data was not found and None returned)
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)

    # check force extract keys for data passed to main function as parameters and fabric labels
    # if analyzed data was re-extracted or re-analyzed on previous steps then data from data_lst
    # need to be re-checked regardless if it was analyzed on prev iterations
    analyzed_data_flags = [report_steps_dct[data_name][1] for data_name in analyzed_data_names]

    # information string if data used during execution have been forcibly changed
    if any(analyzed_data_flags) and not any(force_extract_keys_lst) and all(data_check):
        info = f'Force data processing due to change in collected or analyzed data'
        print(info, end =" ")
        status_info('ok', max_title, len(info))

    # when no data saved or force extract flag is on or data passed as parameters have been changed
    #  then analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or any(analyzed_data_flags):
        force_run = True

    return force_run


def reply_request(question: str, reply_options = ['y', 'yes', 'n', 'no'], show_reply = False):
    """Function to ask user for input until its in reply options"""

    reply = None                
    while not reply in reply_options:
        reply = input(question).lower()
    else:
        if show_reply:
            print(f'Your choice: {reply}')
    
    if reply in ['yes', 'no']:
        return reply[0]
    else:
        return reply






