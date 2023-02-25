"""Module with auxiliary functions to request input from user, display operation status and
checking if each step is required to run"""


import sys
from datetime import datetime
from functools import wraps

import utilities.data_structure_operations as dsop
from san_automation_constants import LEFT_INDENT, MIDDLE_SPACE


def status_info(status, max_title, len_info_string, shift=0):
    """Function to print current operation status ('OK', 'SKIP', 'FAIL')"""

    # information + operation status string length in terminal
    str_length = max_title + MIDDLE_SPACE + shift
    status = status.upper()
    # status info aligned to the right side
    # space between current operation information and status of its execution filled with dots
    print(status.rjust(str_length - len_info_string, '.'))
    return status


def show_collection_status(collected_lst, max_title, len_info_string):
    """Function to show collection status after data extraction to collected_lst"""

    if dsop.list_is_empty(collected_lst):
        status_info('no data', max_title, len_info_string)
    else:
        status_info('ok', max_title, len_info_string)


def show_module_info(project_steps_df, data_names, data_name_id=0):
    """Function to show module title from project_steps_df"""

    print(f'\n\n{project_steps_df.loc[data_names[data_name_id], "module_info"]}\n')


def show_step_info(project_steps_df, data_names, data_name_id=0, end='\n'):
    """Function to show step information of the module from project_steps_df"""

    print(f'\n{project_steps_df.loc[data_names[data_name_id], "step_info"]}\n', end=end)


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


def continue_request():
    """Function to show request to progrma execution. Used if some minor data is missing"""

    reply = reply_request('Do you want to continue? (y)es/(n)o: ')
    if reply == 'n':
        sys.exit()


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
        status_info('on', max_title, len(info))
    return data_check


def verify_force_run(data_names, data_lst, project_steps_df, max_title, analyzed_data_names = []):
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
    try: 
        # force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]

        force_extract_keys_lst = [project_steps_df.loc[data_name, 'force_run'] for data_name in data_names]
    except KeyError as keyerror:
        print('\n')
        print(f'Check if {keyerror} is present in report_info.xlsx.')
        print('\n')
        exit()
    # list with True (if data loaded) and/or False (if data was not found and None returned)
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)

    # check force extract keys for data passed to main function as parameters and fabric labels
    # if analyzed data was re-extracted or re-analyzed on previous steps then data from data_lst
    # need to be re-checked regardless if it was analyzed on prev iterations
    
    # analyzed_data_flags = [report_steps_dct[data_name][1] for data_name in analyzed_data_names]
    analyzed_data_flags = [project_steps_df.loc[data_name, 'force_run'] for data_name in analyzed_data_names]

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


def current_datetime(drop_seconds=False, join=False):
    """Function returns current datetime in 03/11/2022 11:37:45 format"""

    now = datetime.now()
    # w/o seconds
    if drop_seconds:
        if join:
            return now.strftime("%d%m%Y_%H%M")
        else:
            return now.strftime("%d/%m/%Y %H:%M")
    # with seconds
    elif not drop_seconds:
        if join:
            return now.strftime("%d%m%Y_%H%M%S")
        else:
            return now.strftime("%d/%m/%Y %H:%M:%S")


def display_continue_request():
    """Function displays continue request"""

    reply = reply_request(f'{" "*(LEFT_INDENT - 1)} Do you want to CONTINUE? (y)es/(n)o: ')
    if reply == 'n':
        sys.exit()












