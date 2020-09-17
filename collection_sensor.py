"""Module to extract sensor information"""


import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects


def sensor_extract(switch_params_lst, report_data_lst):
    """Function to extract sensor information"""  

    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['sensor']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration    
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    sensor_lst, = data_lst
    
    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files  
    if not all(data_lst) or any(force_extract_keys_lst):    
        print('\nEXTRACTING ENVIRONMENT DATA ...\n')   
        
        # extract chassis parameters names from init file
        switch_columns = columns_import('switch', max_title, 'columns')
        # number of switches to check
        switch_num = len(switch_params_lst)   
     
        # data imported from init file to extract values from config file
        *_, comp_keys, match_keys, comp_dct = data_extract_objects('sensor', max_title)


        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        sensor_lst = []

        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_data in enumerate(switch_params_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current switch
            switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                                'SwitchName', 'switchWwn']
            switch_info_lst = [switch_params_data_dct.get(key) for key in switch_info_keys]
            
            sshow_file, *_, switch_index, switch_name, _ = switch_info_lst
                        
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} sensor readings'
            print(info, end =" ")           
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            collected = {'sensor': False}

            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                # check file until all groups of parameters extracted
                while not all(collected.values()):
                    line = file.readline()                        
                    if not line:
                        break
                    # sensor section start   
                    # switchcmd_sensorshow_comp
                    if re.search(comp_dct[comp_keys[0]], line) and not collected['sensor']:
                        collected['sensor'] = True                      
                        # switchcmd_end_comp
                        while not re.search(comp_dct[comp_keys[2]], line):
                            line = file.readline()
                            match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # islshow_match
                            if match_dct[match_keys[1]]:
                                sensor_reading = line_to_list(comp_dct[comp_keys[1]], line, *switch_info_lst)
                                # appending list with only REQUIRED port info for the current loop iteration 
                                # to the list with all ISL port info
                                sensor_lst.append(sensor_reading)
                            if not line:
                                break                                
                    # sensor section end
            status_info('ok', max_title, len(info))      
        # save extracted data to json file
        save_data(report_data_lst, data_names, sensor_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        sensor_lst = verify_data(report_data_lst, data_names, *data_lst)
    
    return sensor_lst
