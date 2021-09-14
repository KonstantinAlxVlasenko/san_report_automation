"""Module to extract portFcPortCmdShow information"""


import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (force_extract_check, line_to_list,
                                             status_info, update_dct,
                                             verify_data)
from common_operations_servicefile import columns_import, data_extract_objects


def logs_extract(chassis_params_fabric_lst, report_creation_info_lst):
    """Function to extract logs"""

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst
    # names to save data obtained after current module execution
    data_names = ['errdump']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_constant_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    errdump_lst, = data_lst

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files  
    if not all(data_lst) or any(force_extract_keys_lst):             
        print('\nEXTRACTING LOGS ...\n')
        
        # extract chassis parameters names from init file
        chassis_columns = columns_import('chassis', max_title, 'columns')
        # number of switches to check
        switch_num = len(chassis_params_fabric_lst)

        # data imported from init file to extract values from config file
        _, _, comp_keys, match_keys, comp_dct = data_extract_objects('log', max_title)

        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping       
        errdump_lst = []  

        # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
        # checking each chassis for switch level parameters
        for i, chassis_params_data in enumerate(chassis_params_fabric_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current chassis
            chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
            chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
            chassis_info_lst = [chassis_params_data_dct.get(key) for key in chassis_info_keys]

            sshow_file, chassis_name, _ = chassis_info_lst

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_name} switch logs'
            print(info, end =" ")
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            collected = {'errdump': False}
            
            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                # check file until all groups of parameters extracted
                while not all(collected.values()):
                    line = file.readline()
                    if not line:
                        break
                    # errdump section start
                    # errdump_start_comp
                    if re.search(comp_dct['errdump_start'], line) and not collected['errdump']:
                        # when section is found corresponding collected dict values changed to True
                        collected['errdump'] = True
                        # switchcmd_end_comp
                        while not re.search(comp_dct['switchcmd_end'], line):
                            line = file.readline()
                            if not line:
                                break
                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # errdump_message_match
                            if match_dct['errdump_message']:
                                error_message = line_to_list(comp_dct['errdump_message'], line, *chassis_info_lst)
                                # append current value to the list of values 
                                errdump_lst.append(error_message)
                            if not line:
                                break                                
                    # errdump section end
            status_info('ok', max_title, len(info))
        # create list with extracted data
        data_lst = [errdump_lst]      
        # save extracted data to json file
        save_data(report_constant_lst, data_names, errdump_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        errdump_lst = verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [errdump_lst] 
    
    return errdump_lst

