import re
import pandas as pd
from files_operations import columns_import, status_info, data_extract_objects

"""Module to extract fabrics information"""


def fabricshow_extract(switch_params_lst, max_title):
    """Function to extract fabrics information
    """
        
    print('\n\nEXTRACTING FABRICS INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...')
    
    # extract switch parameters names from init file
    switch_columns = columns_import('switch', max_title, 'columns')
    # number of switches to check
    switch_num = len(switch_params_lst)   
    # list to store only REQUIRED switch parameters
    # collecting data for all switches during looping
    fabricshow_lst = []
    fcrfabricshow_lst = []    
    # data imported from init file to extract values from config file
    fabricshow, fabricshow_add, comp_keys, match_keys, comp_dct = data_extract_objects('fabricshow', max_title)   


    # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
    # checking each chassis for switch level parameters
    for i, switch_params_data in enumerate(switch_params_lst):       
        # data unpacking from iter param
        # dictionary with parameters for the current chassis
        switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
        sshow_file = switch_params_data_dct['configname']
        chassis_name = switch_params_data_dct['chassis_name']
        switch_index = int(switch_params_data_dct['switch_index'])
        switch_name = switch_params_data_dct['SwitchName']
        ls_mode = switch_params_data_dct['LS_mode']
        switch_role = switch_params_data_dct['switchRole']
        switch_fid = switch_params_data_dct['Fabric_ID']
        switch_fcr = switch_params_data_dct['FC_Router']
        
        # current operation information string
        info = f'[{i+1} of {switch_num}]: {switch_name} fabric information check. Switch role: {switch_role}'
        print(info, end =" ")
        
        collected = {'fabricshow': False, 'fcrfabricshow': False} if switch_fcr == 'ON' else {'fabricshow': False}
        
        # check config of Principal switch only 
        if switch_role == 'Principal':
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            # collected = {'fabricshow': False}
            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                # check file until all groups of parameters extracted
                while not all(collected.values()):
                    line = file.readline()
                    if not line:
                        break
                    # fabricshow section start
                    if re.search(r'^(SWITCHCMD /fabos/cliexec/)?fabricshow\s*:$', line):
                        # when section is found corresponding collected dict values changed to True
                        collected['fabricshow'] = True
                        if ls_mode == 'ON':
                            while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                line = file.readline()
                                if not line:
                                    break
                        while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                            line = file.readline()
                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # match_keys ['fabricshow_match'] 
                            # 'fabricshow_match'
                            if match_dct[match_keys[0]]:
                                fabricshow_values, = comp_dct[comp_keys[0]].findall(line)
                                principal_switch_lst = [sshow_file, switch_name, switch_index, switch_fid]
                                current_switch_lst =  [value.rstrip() if value else None for value in fabricshow_values]
                                fabricshow_lst.append([*principal_switch_lst, *current_switch_lst])                                       
                            if not line:
                                break
                    # fabricshow section end
                    # fcrfabricshow section start
                    if re.search(r'^BASESWCMD fcrfabricshow:$', line):
                        collected['fcrfabricshow'] = True
                        while not re.search(r'^(real [\w.]+)|(\*\* SS CMD END \*\*)$',line):
                            line = file.readline()
                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # match_keys ['fabricshow_match', 'fcrfabricshow_match'] 
                            # 'fabricshow_match'
                            if match_dct[match_keys[1]]:                                    
                                fcrouter_info, = comp_dct[comp_keys[1]].findall(line)
                                fcrouter_info_lst = [value.rstrip() if value else None for value in fcrouter_info]
                            
                                
                                while not re.match('\r?\n', line):
                                    line = file.readline()
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    if match_dct[match_keys[2]]:
                                        fcrouter_name = match_dct[match_keys[2]].group(1)                                        
                                    if match_dct[match_keys[3]]:
                                        fcrouter_port_values, = comp_dct[comp_keys[3]].findall(line)
                                        fcrouter_port_values_lst = [value.rstrip() if value else None for value in fcrouter_port_values]
                                        principal_router_lst = [sshow_file, switch_name, switch_index, switch_fid]
                                        fcrfabricshow_lst.append([*principal_router_lst, fcrouter_name, *fcrouter_info_lst, *fcrouter_port_values_lst])
                                    if not line:
                                        break                                      
                            if not line:
                                break
                    # fcrfabricshow section end
                    
            status_info('ok', max_title, len(info))
        else:
            status_info('skip', max_title, len(info))
            
    return fabricshow_lst, fcrfabricshow_lst