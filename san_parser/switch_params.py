"""Module to extract switch parameters"""


import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects
from common_operations_dataframe import list_to_dataframe
from common_operations_table_report import dataframe_to_report
from common_operations_miscellaneous import verify_force_run
from common_operations_database import read_db, write_db


def switch_params_extract(chassis_params_df, report_creation_info_lst):
    """Function to extract switch parameters"""

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['switch_parameters', 'switchshow_ports']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    data_lst = read_db(report_constant_lst, report_steps_dct, *data_names)

    # when any data from data_lst was not saved (file not found) or
    # force extract flag is on then re-extract data from configuration files
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run:    
        print('\nEXTRACTING SWITCH PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...\n')   
        
        # # extract chassis parameters names from init file
        # chassis_columns = columns_import('chassis', max_title, 'columns')
        
        # number of switches to check
        switch_num = len(chassis_params_df.index)   
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        switch_params_lst = []
        # list to store switch ports details 
        switchshow_ports_lst = []    
        # data imported from init file to extract values from config file
        switch_params, params_add, comp_keys, match_keys, comp_dct = data_extract_objects('switch', max_title)
        
        # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
        # checking each chassis for switch level parameters
        for i, chassis_params_sr in chassis_params_df.iterrows():       
                
            chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
            chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]
            sshow_file, chassis_name, chassis_wwn = chassis_info_lst

            # when num of logical switches is 0 or None than mode is Non-VF otherwise VF
            ls_mode_on = (True if not chassis_params_sr["Number_of_LS"] in ['0', None] else False)
            ls_mode = ('ON' if not chassis_params_sr["Number_of_LS"] in ['0', None] else 'OFF')
            # logical switches indexes. if switch is in Non-VF mode then ls_id is 0
            ls_ids = chassis_params_sr['LS_IDs'].split(', ') if chassis_params_sr['LS_IDs'] else ['0']               
            
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_name} switch parameters. Number of LS: {chassis_params_sr["Number_of_LS"]}'
            print(info, end =" ")

            # check each logical switch in chassis
            for i in ls_ids:
                # search control dictionary. continue to check sshow_file until all parameters groups are found
                collected = {'configshow': False, 'switchshow': False}
                # dictionary to store all DISCOVERED switch parameters
                # collecting data only for the logical switch in current loop
                switch_params_dct = {}      
                with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                    # check file until all groups of parameters extracted
                    while not all(collected.values()):
                        line = file.readline()                        
                        if not line:
                            break
                        # configshow section start
                        if re.search(fr'^\[Switch Configuration Begin *: *{i}\]$', line) and not collected['configshow']:
                            # when section is found corresponding collected dict values changed to True
                            collected['configshow'] = True
                            
                            while not re.search(fr'^\[Switch Configuration End : {i}\]$',line):
                                line = file.readline()
                                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}                           
                                # match_keys ['switch_configall_match', 'switch_switchshow_match']
                                if match_dct[match_keys[0]]:
                                    switch_params_dct[match_dct[match_keys[0]].group(1).rstrip()] = match_dct[match_keys[0]].group(3).rstrip()              
                                if not line:
                                    break
                        # config section end
                        # switchshow section start
                        if re.search(r'^(SWITCHCMD /fabos/bin/)?switchshow *:$', line) and not collected['switchshow']:
                            collected['switchshow'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {i} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break
                            while not re.search(r'^real [\w.]+$',line):
                                line = file.readline()
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # 'switch_switchshow_match'
                                if match_dct[match_keys[1]]:
                                    switch_params_dct[match_dct[match_keys[1]].group(1).rstrip()] = match_dct[match_keys[1]].group(2).rstrip()
                                # 'ls_attr_match'
                                if match_dct[match_keys[2]]:
                                    ls_attr = comp_dct[comp_keys[2]].findall(line)[0]
                                    for k, v in zip(ls_attr[::2], ls_attr[1::2]):
                                        switch_params_dct[k] = v
                                # 'switchshow_portinfo_match'
                                if match_dct[match_keys[3]]:
                                    switchinfo_lst = [sshow_file, chassis_name, chassis_wwn, str(i), 
                                                      switch_params_dct.get('switchName', None), 
                                                      switch_params_dct.get('switchWwn', None), 
                                                      switch_params_dct.get('switchState', None), 
                                                      switch_params_dct.get('switchMode', None)
                                                      ]
                                    switchshow_port_lst = line_to_list(comp_dct[comp_keys[3]], line, *switchinfo_lst)
                                    # if switch has no slots than slot number is 0
                                    if not switchshow_port_lst[9]:
                                        switchshow_port_lst[9] = str(0)
                                    
                                    switchshow_ports_lst.append(switchshow_port_lst)
                                                     
                                if not line:
                                    break                        
                        # switchshow section end
                        
                # additional values which need to be added to the switch params dictionary 
                # switch_params_add order ('configname', 'chassis_name', 'switch_index', 'ls_mode')
                # values axtracted in manual mode. if change values order change keys order in init.xlsx switch tab "params_add" column
                switch_params_values = (sshow_file, chassis_name, chassis_wwn, str(i), ls_mode)

                if switch_params_dct:
                    # adding additional parameters and values to the switch_params_switch_dct
                    update_dct(params_add, switch_params_values, switch_params_dct)                                                
                    # creating list with REQUIRED chassis parameters for the current switch.
                    # if no value in the switch_params_dct for the parameter then None is added 
                    # and appending this list to the list of all switches switch_params_fabric_lst            
                    switch_params_lst.append([switch_params_dct.get(switch_param, None) for switch_param in switch_params])
                                
            status_info('ok', max_title, len(info))

        # convert list to DataFrame
        switch_params_df = list_to_dataframe(switch_params_lst, max_title, sheet_title_import='switch')
        switchshow_ports_df = list_to_dataframe(switchshow_ports_lst, max_title, sheet_title_import='switch', columns_title_import = 'switchshow_portinfo_columns')
        # saving data to csv file
        data_lst = [switch_params_df, switchshow_ports_df]
        # save_data(report_constant_lst, data_names, *data_lst)
        # write data to sql db
        write_db(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        switch_params_df, switchshow_ports_df = verify_data(report_constant_lst, data_names, *data_lst)
        data_lst = [switch_params_df, switchshow_ports_df]

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dataframe_to_report(data_frame, data_name, report_creation_info_lst)
        
    return switch_params_df, switchshow_ports_df
