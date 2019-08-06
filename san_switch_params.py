import re
import pandas as pd
from files_operations import columns_import, status_info

"""Module to extract switch parameters"""


def switch_params_configshow_extract(chassis_params_fabric_lst, chassis_columns, max_title):
        
    print('\n\nEXTRACTING SWITCH PARAMETERS FROM CONFIGSHOW SECTION OF SUPPORTSHOW CONFIGURATION FILES ...')
    # extract chassis parameters values from init file
    switch_params_configshow = columns_import('parameters', 'switch_params_configshow', max_title)
    # chassis_params_keys  = columns_import('parameters', 'chassis_params_add', max_title)
    
    # number of switches to check
    switch_num = len(chassis_params_fabric_lst)
    
    # list to store REQUIRED chassis parameters
    switch_params_configshow_lst = []
    
    # lines example SwitchName = swDC_62r
    switch_param_comp = re.compile(r'^([\w .-]+) ?(=|:) ?([-\w. :/]+)$')    
    
    # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
    for i, chassis_params_data in enumerate(chassis_params_fabric_lst):
        
        chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
        sshow_file = chassis_params_data_dct['configname']
        
        info = f'[{i+1} of {switch_num}]: {chassis_params_data_dct["switchname"]} switches_configshow parameters check. Number of LS: {chassis_params_data_dct["Number_of_LS"]}'
        print(info, end =" ")
        
        num_ls = int(chassis_params_data_dct["Number_of_LS"]) if not chassis_params_data_dct["Number_of_LS"] in ['0', None] else 1
        
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            line = file.readline()

            for i in range(num_ls):
                # dictionary to store DISCOVERED chassis parameters
                switch_params_dct = {}
                while not re.search(fr'^\[Switch Configuration Begin : {i}\]$', line):
                    line = file.readline()
                    if not line:
                        break
                while not re.search(fr'^\[Switch Configuration End : {i}\]$',line):
                    line = file.readline()
                    switch_param_match = switch_param_comp.match(line)
                    if switch_param_match:
                        switch_params_dct[switch_param_match.group(1).rstrip()] = switch_param_match.group(3)              
                    if not line:
                        break
                    
                switch_params_dct['configname'] = sshow_file
                switch_params_dct['switch_index'] = i
                switch_params_configshow_lst.append([switch_params_dct.get(switch_param, None) for switch_param in switch_params_configshow])
                
        
        
        # # chassis_params_keys order ('configname', 'switchname', 'snmp_server', 'syslog_server', 'timezone_h:m')
        # chassis_params_values = (sshow_file, switch_name, snmp_target_set, syslog_set, tz_lst)
        
        # # adding additional parameters and values to the chassis_params_switch_dct
        # for chassis_param_key, chassis_param_value in zip(chassis_params_keys,  chassis_params_values):
        #     if chassis_param_value:                
        #         if not isinstance(chassis_param_value, str):
        #             s = ':' if chassis_param_key == 'timezone_h:m' else ', '
        #             chassis_param_value = f'{s}'.join(chassis_param_value)
        #         chassis_params_switch_dct[chassis_param_key] = chassis_param_value

        # # creating list with REQUIRED chassis parameters and adding this list to the chassis_params_fabric_lst
        # chassis_params_fabric_lst.append([chassis_params_switch_dct.get(chassis_param, None) for chassis_param in chassis_params])
            
        status_info('ok', max_title, len(info))
        
    return switch_params_configshow_lst