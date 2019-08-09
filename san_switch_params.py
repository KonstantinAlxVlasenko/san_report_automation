import re
import pandas as pd
from files_operations import columns_import, status_info, data_extract_objects

"""Module to extract switch parameters"""


def switch_params_configshow_extract(chassis_params_fabric_lst, chassis_columns, max_title):
        
    print('\n\nEXTRACTING SWITCH PARAMETERS FROM CONFIGSHOW SECTION OF SUPPORTSHOW CONFIGURATION FILES ...')
    # extract chassis parameters values from init file
    switch_params_configshow = columns_import('parameters', max_title, 'switch_params_configshow')
    # chassis_params_keys  = columns_import('parameters', 'chassis_params_add', max_title)
    
    # number of switches to check
    switch_num = len(chassis_params_fabric_lst)
    
    # list to store REQUIRED chassis parameters
    switch_params_configshow_lst = []
    
    switch_params, switch_params_add, comp_keys, match_keys, comp_dct = data_extract_objects('switch', max_title)
    
    # lines example SwitchName = swDC_62r
    switch_param_comp = re.compile(r'^([\w .-]+) ?(=|:) ?([-\w. :/]+)$')    
    
    # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
    for i, chassis_params_data in enumerate(chassis_params_fabric_lst):
        
        chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
        sshow_file = chassis_params_data_dct['configname']
        num_ls = int(chassis_params_data_dct["Number_of_LS"]) if not chassis_params_data_dct["Number_of_LS"] in ['0', None] else 1
        ls_mode = (True if not chassis_params_data_dct["Number_of_LS"] in ['0', None] else False)
        
        
        
        info = f'[{i+1} of {switch_num}]: {chassis_params_data_dct["switchname"]} switches_configshow parameters check. Number of LS: {chassis_params_data_dct["Number_of_LS"]}'
        print(info, end =" ")
        
        for i in range(num_ls):
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            collected = {'configshow': False}
            # dictionary to store DISCOVERED switch parameters
            switch_params_dct = {}      
            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                while not all(collected.values()):
                    line = file.readline()
                    if not line:
                        break
                    # configshow section
                    if re.search(fr'^\[Switch Configuration Begin : {i}\]$', line):
                        collected['configshow'] = True
                        while not re.search(fr'^\[Switch Configuration End : {i}\]$',line):
                            line = file.readline()
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # switch_param_match = switch_param_comp.match(line)
                            # if switch_param_match:
                            #     switch_params_dct[switch_param_match.group(1).rstrip()] = switch_param_match.group(3)
                            if match_dct[match_keys[0]]:
                                switch_params_dct[match_dct[match_keys[0]].group(1).rstrip()] = match_dct[match_keys[0]].group(3)              
                            if not line:
                                break
                    
            # switch_params_add order ('configname', 'switc_index')
            # values axtracted in manual mode. if change values order change keys order in init.xlsx switch tab "params_add" column
            switch_params_values = (sshow_file, str(i))            

            # adding additional parameters and values to the chassis_params_switch_dct
            for switch_param_add, switch_param_value in zip(switch_params_add,  switch_params_values):
                if switch_param_value:                
                    if not isinstance(switch_param_value, str):
                        s = ':' if switch_param_add == 'timezone_h:m' else ', '
                        switch_param_value = f'{s}'.join(switch_param_value)
                    switch_params_dct[switch_param_add] = switch_param_value
                            
                        # switch_params_dct['configname'] = sshow_file
                        # switch_params_dct['switch_index'] = i
            
            # creating list with REQUIRED chassis parameters for the current switch 
            # and appending this list to the list of all switches switch_params_fabric_lst            
            switch_params_configshow_lst.append([switch_params_dct.get(switch_param, None) for switch_param in switch_params])
                
            
        status_info('ok', max_title, len(info))
        
    return switch_params_configshow_lst