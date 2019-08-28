import re
import pandas as pd
from files_operations import columns_import, status_info, data_extract_objects, data_to_json, json_to_data, line_to_list

"""Module to extract switch parameters"""


def switch_params_configshow_extract(chassis_params_fabric_lst, report_data_lst):
    
    # report_data_lst = [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 5. SWITCH PARAMETERS ...\n')
    
    *_, max_title = report_data_lst
    # check if data already have been extracted
    data_names = ['switch_parameters', 'switchshow_ports']
    data_lst = json_to_data(report_data_lst, *data_names)
    switch_params_lst, switchshow_ports_lst = data_lst
    
    # if no data saved than extract data from configurtion files 
    if not all(data_lst):    
        print('\nEXTRACTING SWITCH PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...\n')   
        
        # extract chassis parameters names from init file
        chassis_columns = columns_import('chassis', max_title, 'columns')
        # number of switches to check
        switch_num = len(chassis_params_fabric_lst)   
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        switch_params_lst = []
        # list to store switch ports details 
        switchshow_ports_lst = []    
        # data imported from init file to extract values from config file
        switch_params, switch_params_add, comp_keys, match_keys, comp_dct = data_extract_objects('switch', max_title)
        
        # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
        # checking each chassis for switch level parameters
        for i, chassis_params_data in enumerate(chassis_params_fabric_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current chassis
            chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
            sshow_file = chassis_params_data_dct['configname']
            chassis_name = chassis_params_data_dct['chassis_name']
            num_ls = int(chassis_params_data_dct["Number_of_LS"]) if not chassis_params_data_dct["Number_of_LS"] in ['0', None] else 1
            # when num of logical switches is 0 or None than mode is Non-VF otherwise VF
            ls_mode_on = (True if not chassis_params_data_dct["Number_of_LS"] in ['0', None] else False)
            ls_mode = ('ON' if not chassis_params_data_dct["Number_of_LS"] in ['0', None] else 'OFF')       
            
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_data_dct["chassis_name"]} switch parameters check. Number of LS: {chassis_params_data_dct["Number_of_LS"]}'
            print(info, end =" ")
            
            # check each logical switch in chassis
            for i in range(num_ls):
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
                        if re.search(fr'^\[Switch Configuration Begin : {i}\]$', line):
                            # when section is found corresponding collected dict values changed to True
                            collected['configshow'] = True
                            
                            while not re.search(fr'^\[Switch Configuration End : {i}\]$',line):
                                line = file.readline()
                                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}                           
                                # match_keys ['switch_configall_match', 'switch_switchshow_match']
                                if match_dct[match_keys[0]]:
                                    switch_params_dct[match_dct[match_keys[0]].group(1).rstrip()] = match_dct[match_keys[0]].group(3)              
                                if not line:
                                    break
                        # config section end
                        # switchshow section start
                        if re.search(r'^(SWITCHCMD /fabos/bin/)?switchshow\s*:$', line):
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
                                    switch_params_dct[match_dct[match_keys[1]].group(1).rstrip()] = match_dct[match_keys[1]].group(2)
                                # 'ls_attr_match'
                                if match_dct[match_keys[2]]:
                                    ls_attr = comp_dct[comp_keys[2]].findall(line)[0]
                                    for k, v in zip(ls_attr[::2], ls_attr[1::2]):
                                        switch_params_dct[k] = v
                                # 'switchshow_portinfo_match'
                                if match_dct[match_keys[3]]:
                                    switchinfo_lst = [sshow_file, chassis_name, i, switch_params_dct.get('switchName', None)]
                                    switchshow_ports_lst.append(line_to_list(comp_dct[comp_keys[3]], line, *switchinfo_lst))                                                       
                                if not line:
                                    break                        
                        # switchshow section end
                        
                # additional values which need to be added to the switch params dictionary 
                # switch_params_add order ('configname', 'chassis_name', 'switch_index', 'ls_mode')
                # values axtracted in manual mode. if change values order change keys order in init.xlsx switch tab "params_add" column
                switch_params_values = (sshow_file, chassis_name, str(i), ls_mode)            

                # adding additional parameters and values to the chassis_params_switch_dct
                for switch_param_add, switch_param_value in zip(switch_params_add,  switch_params_values):
                    if switch_param_value:                
                        # if not isinstance(switch_param_value, str):
                        #     s = ':' if switch_param_add == 'timezone_h:m' else ', '
                        #     switch_param_value = f'{s}'.join(switch_param_value)
                        switch_params_dct[switch_param_add] = switch_param_value
                                            
                # creating list with REQUIRED chassis parameters for the current switch.
                # if no value in the switch_params_dct for the parameter then None is added 
                # and appending this list to the list of all switches switch_params_fabric_lst            
                switch_params_lst.append([switch_params_dct.get(switch_param, None) for switch_param in switch_params])
                                
            status_info('ok', max_title, len(info))
        # save extracted data to json file
        data_to_json(report_data_lst, data_names, switch_params_lst, switchshow_ports_lst)
        
    return switch_params_lst, switchshow_ports_lst