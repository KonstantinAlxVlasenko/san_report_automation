"""Module to extract port information (sfp transceivers, portcfg, trunk area settings)"""


import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data, verify_force_run)
from common_operations_servicefile import (columns_import,
                                           data_extract_objects,
                                           dct_from_columns)


def portinfo_extract(switch_params_lst, report_creation_info_lst):
    """Function to extract switch port information"""
    
    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['sfpshow', 'portcfgshow']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration    
    data_lst = load_data(report_constant_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    sfpshow_lst, portcfgshow_lst = data_lst

    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, max_title)

    # # data force extract check 
    # # list of keys for each data from data_lst representing if it is required 
    # # to re-collect or re-analyze data even they were obtained on previous iterations
    # force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # # print data which were loaded but for which force extract flag is on
    # force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # # when any of data_lst was not saved or 
    # # force extract flag is on then re-extract data  from configueation files  
    # if not all(data_lst) or any(force_extract_keys_lst):
    if force_run:    
        print('\nEXTRACTING SWITCH PORTS SFP, PORTCFG INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')   
        
        # extract chassis parameters names from init file
        switch_columns = columns_import('switch', max_title, 'columns')
        # number of switches to check
        switch_num = len(switch_params_lst)   
     
        # data imported from init file to extract values from config file
        params, params_add, comp_keys, match_keys, comp_dct = data_extract_objects('portinfo', max_title)
        portcfg_params = columns_import('portinfo', max_title, 'portcfg_params')
        # dictionary to save portcfg ALL information for all ports in fabric
        portcfgshow_dct = dict((key, []) for key in portcfg_params)
        # list to store only REQUIRED switch parameters
        # collecting sfpshow data for all switches ports during looping
        sfpshow_lst = []
        # list to save portcfg information for all ports in fabric
        portcfgshow_lst = []

        
        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_data in enumerate(switch_params_lst):       

            # data unpacking from iter param
            # dictionary with parameters for the current switch
            switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                                'SwitchName', 'switchWwn']
            switch_info_lst = [switch_params_data_dct.get(key) for key in switch_info_keys]
            ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False
            
            sshow_file, _, _, switch_index, switch_name, *_ = switch_info_lst
            
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} ports sfp and cfg'
            print(info, end =" ")           
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            collected = {'sfpshow': False, 'portcfgshow': False}
            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                # check file until all groups of parameters extracted
                while not all(collected.values()):
                    line = file.readline()                        
                    if not line:
                        break
                    # sfpshow section start
                    if re.search(r'^(SWITCHCMD )?(/fabos/cliexec/)?sfpshow +-all *: *$', line) and not collected['sfpshow']:
                        collected['sfpshow'] = True
                        if ls_mode_on:
                            while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                line = file.readline()
                                if not line:
                                    break
                        while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            if match_dct[match_keys[0]]:
                                # dictionary to store all DISCOVERED switch ports information
                                # collecting data only for the logical switch in current loop
                                sfpshow_dct = {}
                                _, slot_num, port_num = line_to_list(comp_dct[comp_keys[0]], line)
                                # if switch has no slots then all ports have slot 0
                                slot_num = '0' if not slot_num else slot_num
                                while not re.match('\r?\n', line):
                                    line = file.readline()
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # power_match
                                    if match_dct[match_keys[1]]:
                                        sfp_power_lst = line_to_list(comp_dct[comp_keys[1]], line)
                                        # cut off RX or TX Power
                                        sfp_power_value_unit = sfp_power_lst[1:]
                                        for v, k in zip(sfp_power_value_unit[::2], sfp_power_value_unit[1::2]):
                                            if k == 'uWatts':
                                                k = 'uW'
                                            key = sfp_power_lst[0] + '_' + k
                                            sfpshow_dct[key] = v
                                    # transceiver_match
                                    elif match_dct[match_keys[2]]:
                                        sfpshow_dct[match_dct[match_keys[2]].group(1).rstrip()] = match_dct[match_keys[2]].group(2).rstrip()
                                    # no_sfp_match
                                    elif match_dct[match_keys[3]]:
                                            sfpshow_dct['Vendor Name'] = 'No SFP module'
                                    # not_available_match
                                    elif match_dct[match_keys[4]]:
                                            sfpshow_dct[match_dct[match_keys[4]].group(1).rstrip()] = match_dct[match_keys[4]].group(2).rstrip()
                                    # sfp_info_match
                                    elif match_dct[match_keys[5]]:
                                        sfpshow_dct[match_dct[match_keys[5]].group(1).rstrip()] = match_dct[match_keys[5]].group(2).rstrip()                                        
                                    if not line:
                                        break
                                    
                                # additional values which need to be added to the dictionary with all DISCOVERED parameters during current loop iteration
                                # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column                                   
                                sfpshow_port_values = [*switch_info_lst, slot_num, port_num]                                       
                                # adding additional parameters and values to the sfpshow_dct
                                update_dct(params_add, sfpshow_port_values, sfpshow_dct)               
                                # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                                sfpshow_lst.append([sfpshow_dct.get(param, None) for param in params])
                    # sfpshow section end
                    # portcfgshow section start
                    if re.search(r'^(SWITCHCMD )?(/fabos/cliexec/)?portcfgshow *: *$', line) and not collected['portcfgshow']:
                        collected['portcfgshow'] = True
                        if ls_mode_on:
                            while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                line = file.readline()
                                if not line:
                                    break
                        while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$|No ports found in switch',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # 'slot_port_line_match'
                            if match_dct[match_keys[6]]:
                                # dictionary to store all DISCOVERED switch ports information
                                portcfgshow_tmp_dct = {}
                                # extract slot and port numbers
                                slot_num, port_nums_str = line_to_list(comp_dct[comp_keys[6]], line)
                                port_nums_lst = port_nums_str.split()
                                port_nums = len(port_nums_lst)
                                # list with switch and slot information
                                switch_info_slot_lst = switch_info_lst.copy()
                                switch_info_slot_lst.append(slot_num)
                                # adding switch and slot information for each port to dictionary
                                for portcfg_param, switch_info_value in zip(portcfg_params[:7], switch_info_slot_lst):
                                    portcfgshow_tmp_dct[portcfg_param] = [switch_info_value for i in range(port_nums)]
                                # adding port numbers to dictionary    
                                portcfgshow_tmp_dct[portcfg_params[7]] = port_nums_lst                                
                                while not re.match('\r?\n', line):
                                    line = file.readline()
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # portcfg_match
                                    if match_dct[match_keys[7]]:
                                        # extract param name and values for each port and adding to dictionary
                                        param_name, param_values_str = line_to_list(comp_dct[comp_keys[7]], line)
                                        portcfgshow_tmp_dct[param_name] = param_values_str.split()
                                    if not line:
                                        break
                                # saving portcfg information of REQUIRED parameters from dictionary with DISCOVERED parameters
                                for portcfg_param in portcfg_params:
                                    portcfgshow_dct[portcfg_param].extend(portcfgshow_tmp_dct.get(portcfg_param, [None for i in range(port_nums)]))              
                    # portcfgshow section end
                     
            status_info('ok', max_title, len(info))

        # after check all config files create list of lists from dictionary. each nested list contains portcfg information for one port
        for portcfg_param in portcfg_params:
            portcfgshow_lst.append(portcfgshow_dct.get(portcfg_param))            
        portcfgshow_lst = list(zip(*portcfgshow_lst))
        
        # save extracted data to json file
        save_data(report_constant_lst, data_names, sfpshow_lst, portcfgshow_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        sfpshow_lst, portcfgshow_lst = verify_data(report_constant_lst, data_names, *data_lst)
        
    
    return sfpshow_lst, portcfgshow_lst
