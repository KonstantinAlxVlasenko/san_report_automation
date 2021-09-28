"""Module to extract maps parameters"""

import os.path
import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (force_extract_check, status_info,
                                             verify_data)
from common_operations_servicefile import data_extract_objects
from common_operations_miscellaneous import verify_force_run
from common_operations_dataframe import list_to_dataframe
from common_operations_table_report import dataframe_to_report


def maps_params_extract(all_config_data, report_creation_info_lst):
    """Function to extract maps parameters
    """

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['maps_parameters']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_constant_lst, *data_names)
    
    
    
    # # unpacking from the loaded list with data
    # # pylint: disable=unbalanced-tuple-unpacking
    # maps_params_fabric_lst, = data_lst
    
    # # data force extract check 
    # # list of keys for each data from data_lst representing if it is required 
    # # to re-collect or re-analyze data even they were obtained on previous iterations
    # force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # # print data which were loaded but for which force extract flag is on
    # force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, max_title)

    # # when any of data_lst was not saved or 
    # # force extract flag is on then re-extract data  from configueation files
    # if not all(data_lst) or any(force_extract_keys_lst):

    if force_run:
        print('\nEXTRACTING MAPS DATA FROM AMS_MAPS_LOG CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(all_config_data)    
        # list to store only REQUIRED parameters
        # collecting data for all switches during looping 
        maps_params_fabric_lst = []
        # data imported from init file to extract values from config file
        maps_params, maps_params_add, comp_keys, match_keys, comp_dct = data_extract_objects('maps', max_title)
        
        # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
        # checking each config set(supportshow file) for chassis level parameters
        for i, switch_config_data in enumerate(all_config_data):
            # data unpacking from iter param
            switch_name, sshow_file, ams_maps_files = switch_config_data
            # number of ams_maps configs
            num_maps = len(ams_maps_files) if ams_maps_files else 0    
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} MAPS parameters. Number of AMS_MAPS configs: {num_maps} ...'
            print(info)
            
            # checking ams_maps log file for each logical switch
            if ams_maps_files:
                for ams_maps_file in ams_maps_files:
                    # search control dictionary. continue to check sshow_file until all parameters groups are found
                    collected = {'switch_index': False, 'global_dash': False}
                    # dictionary to store all DISCOVERED switch parameters
                    # collecting data only for the logical switch in current loop
                    maps_params_dct = {}
                    
                    info = ' '*16+f'{os.path.basename(ams_maps_file)} processing'
                    print(info, end =" ")
                
                    with open(ams_maps_file, encoding='utf-8', errors='ignore') as file:
                        # check file until all groups of parameters extracted
                        while not all(collected.values()):
                            line = file.readline()
                            if not line:
                                break
                            # logical switch index section start
                            if re.search(r'^[= ]*AMS/MAPS *Data *Switch *(\d+)[= ]*$', line):
                                # when section is found corresponding collected dict values changed to True
                                collected['switch_index'] = True
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                switch_index = match_dct[match_keys[0]].group(1)
                            # logical switch index section end
                            # global dashboard section start
                            if re.search(r'^[- ]*MAPS +Global +Monitoring +Configuration[ -]*$', line):
                                collected['global_dash'] = True
                                while not re.search(r'^[- ]*NM +Data[- ]*$',line):
                                    line = file.readline()
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # match_keys ['switch_index_match', 'dashboard_match', 'report_match', 'no_lic_match'] 
                                    # 'dashboard_match'
                                    if match_dct[match_keys[1]]:
                                        maps_params_dct[match_dct[match_keys[1]].group(1).rstrip()] = match_dct[match_keys[1]].group(2)                            
                                    # 'report_match'
                                    if match_dct[match_keys[2]]:
                                        maps_params_dct[match_dct[match_keys[2]].group(1).rstrip()] = match_dct[match_keys[2]].group(2)
                                    # 'no Fabric lic match'
                                    if match_dct[match_keys[3]]:
                                        for maps_param in maps_params[6:23]:
                                            maps_params_dct[maps_param] = 'No FV lic'                                         
                                    if not line:
                                        break
                            # global dashboard section end
                                
                    # additional values which need to be added to the chassis params dictionary
                    # chassis_params_add order (configname, ams_maps_config, chassis_name, switch_index)
                    # values axtracted in manual mode. if change values order change keys order in init.xlsx "maps_params_add" column
                    maps_params_values = (sshow_file, ams_maps_file, switch_name, switch_index)
                    
                    # adding additional parameters and values to the chassis_params_switch_dct
                    for maps_param_add, maps_param_value in zip(maps_params_add,  maps_params_values):
                            maps_params_dct[maps_param_add] = maps_param_value

                    # creating list with REQUIRED maps parameters for the current switch
                    # if no value in the maps_params_dct for the parameter then None is added  
                    # and appending this list to the list of all switches maps_params_fabric_lst
                    maps_params_fabric_lst.append([maps_params_dct.get(maps_param, None) for maps_param in maps_params])
                
                    status_info('ok', max_title, len(info))
            else:
                info = ' '*16+'No AMS_MAPS configuration found.'
                print(info, end =" ")
                status_info('skip', max_title, len(info))
        
        # # save extracted data to json file
        # save_data(report_constant_lst, data_names, maps_params_fabric_lst)

        # convert list to DataFrame
        maps_params_fabric_df = list_to_dataframe(maps_params_fabric_lst, max_title, 'maps')
        # saving data to csv file
        data_lst = [maps_params_fabric_df]
        save_data(report_constant_lst, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        # maps_params_fabric_lst = verify_data(report_constant_lst, data_names, *data_lst)
        maps_params_fabric_df = verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [maps_params_fabric_df]

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)
        
    # return maps_params_fabric_lst

    return maps_params_fabric_df
