import re
import pandas as pd
from files_operations import columns_import, status_info, data_extract_objects, load_data, save_data 
from files_operations import line_to_list, force_extract_check, update_dct

"""Module to extract zoning information"""


def zoning_extract(switch_params_lst, report_data_lst):
    """Function to extract zoning information
    """
    # report_data_lst contains [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 13. ZONING CONFIGURATION ...\n')
    
    *_, max_title, report_steps_dct = report_data_lst
    # check if data already have been extracted
    data_names = ['cfg', 'zone', 'alias', 'cfg_effective', 'zone_effective']
    data_lst = load_data(report_data_lst, *data_names)
    cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst = data_lst

    # data force extract check. 
    # if data have been extracted already but extract key is ON then data re-extracted
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # if no data saved than extract data from configurtion files  
    if not all(data_lst) or any(force_extract_keys_lst):             
        print('\nEXTRACTING ZONING INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # extract switch parameters names from init file
        switch_columns = columns_import('switch', max_title, 'columns')
        # number of switches to check
        switch_num = len(switch_params_lst)   
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        cfg_lst = []
        zone_lst = []
        alias_lst = []
        cfg_effective_lst = []
        zone_effective_lst = []
         
        # data imported from init file to extract values from config file
        *_, comp_keys, match_keys, comp_dct = data_extract_objects('zoning', max_title)
        # ag_params = columns_import('fabricshow', max_title, 'ag_params')  
        
        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_data in enumerate(switch_params_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current switch
            switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            switch_info_keys = ['configname', 'chassis_name', 'switch_index', 
                                'SwitchName', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
            switch_info_lst = [switch_params_data_dct[key] for key in switch_info_keys]
            ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False
            
            sshow_file, _, switch_index, switch_name, switch_role = switch_info_lst[:5]

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} zoning. Switch role: {switch_role}'
            print(info, end =" ")
            
            collected = {'cfgshow': False}
            
            # check config of Principal switch only 
            if switch_role == 'Principal':
                # principal_switch_lst contains sshow_file, chassis_name, switch_index, switch_name, switch_fid
                principal_switch_lst = [*switch_info_lst[:4], switch_info_lst[5]]                                                        
                # search control dictionary. continue to check sshow_file until all parameters groups are found
                with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                    # check file until all groups of parameters extracted
                    while not all(collected.values()):
                        line = file.readline()
                        if not line:
                            break
                        # cfgshow section start
                        if re.search(comp_dct[comp_keys[0]], line):
                            # when section is found corresponding collected dict values changed to True
                            collected['cfgshow'] = True
                            # control flag to check if Effective configuration line passed
                            effective = False
                            # set to collect Defined configuration names 
                            defined_configs_set = set()
                            if ls_mode_on:
                                while not re.search(fr'^CURRENT CONTEXT -- {switch_index} *, \d+$',line):
                                    line = file.readline()
                                    if not line:
                                        break
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[4]], line):                                
                                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # if Effective configuration line passed
                                if match_dct[match_keys[7]]:
                                    effective = True                                     
                                # 'cfg_match'
                                if match_dct[match_keys[1]]:
                                    cfg_line = line_to_list(comp_dct[comp_keys[1]], line)
                                    # zoning config name
                                    cfg_name = cfg_line[0]
                                    # add config name to the set
                                    defined_configs_set.add(cfg_name)
                                    # if except config name line contains zone names
                                    if cfg_line[1]:
                                        members_lst = cfg_line[1].strip().replace(';', '').split()
                                        for member in members_lst:
                                            cfg_lst.append([*principal_switch_lst, cfg_name, member])
                                    # if Effectice configuration checked then 
                                    # add Effective and Defined configuration names to the table 
                                    if effective:
                                        cfg_effective_lst.append([*principal_switch_lst, cfg_name, ', '.join(defined_configs_set)])
                                    # switch to the next line to enter the loop
                                    line = file.readline()
                                    # zoning_switchcmd_end_comp separates different configs, zones and aliases 
                                    while not re.search(comp_dct[comp_keys[3]], line):
                                        # find all zone names in line
                                        members_lst = re.findall(comp_dct[comp_keys[2]], line)
                                        for member in members_lst:
                                            cfg_lst.append([*principal_switch_lst, cfg_name, member.rstrip(';')])
                                        line = file.readline()
                                        if not line:
                                            break
                                # 'zone_match'
                                elif match_dct[match_keys[5]]:
                                    zone_line = line_to_list(comp_dct[comp_keys[5]], line)
                                    zone_name = zone_line[0]
                                    line = file.readline()
                                    # zoning_switchcmd_end_comp separates different configs, zones and aliases
                                    while not re.search(comp_dct[comp_keys[3]], line):
                                        members_lst = re.findall(comp_dct[comp_keys[2]], line)
                                        for member in members_lst:
                                            # for Defined configuration add zones to zone_lst
                                            if not effective:
                                                zone_lst.append([*principal_switch_lst, zone_name, member.rstrip(';')])
                                            # for Effective configuration add zones to zone_effective_lst
                                            elif effective:
                                                zone_effective_lst.append([*principal_switch_lst, zone_name, member.rstrip(';')])
                                        line = file.readline()
                                        if not line:
                                            break
                                # 'alias_match'
                                elif match_dct[match_keys[6]]:
                                    alias_line = line_to_list(comp_dct[comp_keys[6]], line)
                                    alias_name = alias_line[0]
                                    # if line contains alias name and alias member
                                    if alias_line[1]:
                                        member_lst = alias_line[1].strip().replace(';', '').split()
                                        for member in member_lst:
                                            alias_lst.append([*principal_switch_lst, alias_name, member])
                                    line = file.readline()
                                    # zoning_switchcmd_end_comp separates different configs, zones and aliases
                                    while not re.search(comp_dct[comp_keys[3]], line):
                                        member_lst = re.findall(comp_dct[comp_keys[2]], line)
                                        for member in member_lst:
                                            alias_lst.append([*principal_switch_lst, alias_name, member.rstrip(';')])
                                        line = file.readline()
                                        if not line:
                                            break
                                # if line doesn't coreesponds to any reg expression pattern then switch line
                                else:
                                    line = file.readline()                                           
                                if not line:
                                    break
                                                                                
                status_info('ok', max_title, len(info))
            else:
                status_info('skip', max_title, len(info))
        # save extracted data to json file
        save_data(report_data_lst, data_names, cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst)
            
    return cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst


