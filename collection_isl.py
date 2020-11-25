"""Module to extract interswitch connection information"""


import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects


def interswitch_connection_extract(switch_params_lst, report_data_lst):
    """Function to extract interswitch connection information"""  

    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['isl', 'trunk', 'porttrunkarea']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration    
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    isl_lst, trunk_lst, porttrunkarea_lst = data_lst
    
    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files  
    if not all(data_lst) or any(force_extract_keys_lst):    
        print('\nEXTRACTING INTERSWITCH CONNECTION INFORMATION (ISL, TRUNK, TRUNKAREA) ...\n')   
        
        # extract chassis parameters names from init file
        switch_columns = columns_import('switch', max_title, 'columns')
        # number of switches to check
        switch_num = len(switch_params_lst)   
     
        # data imported from init file to extract values from config file
        *_, comp_keys, match_keys, comp_dct = data_extract_objects('isl', max_title)


        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        isl_lst = []
        trunk_lst = []
        porttrunkarea_lst = []

        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_data in enumerate(switch_params_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current switch
            switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                                'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
            switch_info_lst = [switch_params_data_dct.get(key) for key in switch_info_keys]
            ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False
            
            sshow_file, _, _, switch_index, switch_name, *_, switch_mode = switch_info_lst
                        
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} isl, trunk and trunk area ports. Switch mode: {switch_mode}'
            print(info, end =" ")           
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            collected = {'isl': False, 'trunk': False, 'trunkarea': False}

            if switch_mode == 'Native':
                with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                    # check file until all groups of parameters extracted
                    while not all(collected.values()):
                        line = file.readline()                        
                        if not line:
                            break
                        # isl section start   
                        # switchcmd_islshow_comp
                        if re.search(comp_dct[comp_keys[0]], line) and not collected['isl']:
                            collected['isl'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break                        
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[2]], line):
                                line = file.readline()
                                match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # islshow_match
                                if match_dct[match_keys[1]]:
                                    isl_port = line_to_list(comp_dct[comp_keys[1]], line, *switch_info_lst[:-1])
                                    # portcfg parameters
                                    if isl_port[-1]:
                                        isl_port[-1] = isl_port[-1].replace(' ', ', ')
                                    # appending list with only REQUIRED port info for the current loop iteration 
                                    # to the list with all ISL port info
                                    isl_lst.append(isl_port)
                                if not line:
                                    break                                
                        # isl section end
                        # trunk section start   
                        # switchcmd_trunkshow_comp
                        if re.search(comp_dct[comp_keys[3]], line) and not collected['trunk']:
                            collected['trunk'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break                        
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[2]], line):                             
                                match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # trunkshow_match
                                if match_dct[match_keys[4]]:
                                    trunk_port = line_to_list(comp_dct[comp_keys[4]], line, *switch_info_lst[:-1])
                                    # if trunk line has trunk number then remove ":" from trunk number
                                    if trunk_port[9]:
                                        trunk_port[9] = trunk_port[9].strip(':')
                                        trunk_num = trunk_port[9]
                                    # if trunk line has no number then use number from previous line
                                    else:
                                        trunk_port[9] = trunk_num
                                    # appending list with only REQUIRED trunk info for the current loop iteration 
                                    # to the list with all trunk port info
                                    trunk_lst.append(trunk_port)
                                line = file.readline()
                                if not line:
                                    break                                
                        # trunk section end
                        # porttrunkarea section start
                        # switchcmd_trunkarea_comp
                        if re.search(comp_dct[comp_keys[5]], line) and not collected['trunkarea']:
                            collected['trunkarea'] = True
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[2]], line):
                                line = file.readline()
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # 'porttrunkarea_match'
                                if match_dct[match_keys[6]]:
                                    porttrunkarea_port_lst = line_to_list(comp_dct[comp_keys[6]], line, *switch_info_lst[:6])
                                    # due to regular expression master slot appears two times in line
                                    porttrunkarea_port_lst.pop(9)
                                    # for No_light ports port and slot numbers are '--'
                                    if porttrunkarea_port_lst[10] == '--':
                                        porttrunkarea_port_lst[9] = '--'
                                    # if switch has no slots than slot number is 0
                                    for i in [5, 9]:                                    
                                        if not porttrunkarea_port_lst[i]:
                                            porttrunkarea_port_lst[i] = 0
                                    porttrunkarea_lst.append(porttrunkarea_port_lst)                                                       
                                if not line:
                                    break                        
                        # porttrunkarea section end                    
                status_info('ok', max_title, len(info))
            # if switch in Access Gateway mode then skip
            else:
                status_info('skip', max_title, len(info))        
        # save extracted data to json file
        save_data(report_data_lst, data_names, isl_lst, trunk_lst, porttrunkarea_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        isl_lst, trunk_lst, porttrunkarea_lst = verify_data(report_data_lst, data_names, *data_lst)
    
    return isl_lst, trunk_lst, porttrunkarea_lst
