"""Module to extract zoning information"""


import re

import pandas as pd
from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects


def zoning_extract(switch_params_lst, report_creation_info_lst):
    """Function to extract zoning information"""

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['cfg', 'zone', 'alias', 'cfg_effective', 'zone_effective', 'peerzone' , 'peerzone_effective']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_constant_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    cfg_lst, zone_lst, alias_lst, cfg_effective_lst, \
        zone_effective_lst, peerzone_lst, peerzone_effective_lst = data_lst

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on    
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files   
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
        peerzone_effective_lst = []
        peerzone_lst = []
         
        # data imported from init file to extract values from config file
        *_, comp_keys, match_keys, comp_dct = data_extract_objects('zoning', max_title)
        # ag_params = columns_import('fabricshow', max_title, 'ag_params')  
        
        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_data in enumerate(switch_params_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current switch
            switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                                'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
            switch_info_lst = [switch_params_data_dct[key] for key in switch_info_keys]
            ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False
            
            sshow_file, *_, switch_index, switch_name, _, switch_role = switch_info_lst[:7]

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} zoning. Switch role: {switch_role}'
            print(info, end =" ")
            
            collected = {'cfgshow': False, 'peerzone': False}
            
            # check config of Principal switch only 
            if switch_role == 'Principal':
                # principal_switch_lst contains sshow_file, chassis_name, switch_index, switch_name, switch_fid
                principal_switch_lst = [*switch_info_lst[:6], switch_info_lst[7]]                                                        
                # search control dictionary. continue to check sshow_file until all parameters groups are found
                with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                    # check file until all groups of parameters extracted
                    while not all(collected.values()):
                        line = file.readline()
                        if not line:
                            break
                        # cfgshow section start
                        if re.search(comp_dct[comp_keys[0]], line) and not collected['cfgshow']:
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
                                    # if Effective configuration checked then 
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
                                    # if line contains zone name and zone member
                                    if zone_line[1]:
                                        member_lst = zone_line[1].strip().replace(';', '').split()
                                        for member in member_lst:
                                            # for Defined configuration add zones to zone_lst
                                            if not effective: 
                                                zone_lst.append([*principal_switch_lst, zone_name, member])
                                            # for Effective configuration add zones to zone_effective_lst
                                            elif effective:
                                                zone_effective_lst.append([*principal_switch_lst, zone_name, member])
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
                                # if line doesn't coreesponds to any reg expression pattern then next line
                                # until cfgshow command border reached
                                else:
                                    line = file.readline()                                           
                                if not line:
                                    break
                        # cfgshow section end
                        # peerzone section start
                        elif re.search(comp_dct[comp_keys[8]], line) and not collected['peerzone']:
                            # when section is found corresponding collected dict values changed to True
                            collected['peerzone'] = True
                            # control flag to check if Effective configuration line passed
                            peerzone_effective = False
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
                                    peerzone_effective = True                                     
                                # 'zone_match'
                                if match_dct[match_keys[5]]:
                                    zone_line = line_to_list(comp_dct[comp_keys[5]], line)
                                    zone_name = zone_line[0]
                                    line = file.readline()
                                    # zoning_switchcmd_end_comp separates different zones
                                    while not re.search(comp_dct[comp_keys[3]], line):
                                        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}  
                                        # peerzone_property_match
                                        if match_dct[match_keys[9]]:
                                            # peerzone_property is tuple. contains property member ot created by info
                                            peerzone_property = match_dct[match_keys[9]].groups()
                                            zonemember = [*principal_switch_lst, zone_name, *peerzone_property]
                                            # for Effective configuration add member to peerzone_effective_lst
                                            if peerzone_effective:
                                                peerzone_effective_lst.append(zonemember)
                                            # for Defined configuration add member to peerzone_lst
                                            else:
                                                peerzone_lst.append(zonemember)
                                            line = file.readline()
                                        # peerzone_member_type_match (principal or peer)
                                        elif match_dct[match_keys[10]]:
                                            member_type = match_dct[match_keys[10]].group(1)
                                            line = file.readline()
                                            # peerzone_member_end_comp separates peer and principals groups
                                            while not re.search(comp_dct[comp_keys[11]], line):
                                                # find zonemembers
                                                members_lst = re.findall(comp_dct[comp_keys[2]], line)
                                                for member in members_lst:
                                                    # for Defined configuration add zones to zone_lst
                                                    zonemember = [*principal_switch_lst, zone_name, member_type, member.rstrip(';')]
                                                    # for Effective configuration add member to peerzone_effective_lst
                                                    if peerzone_effective:
                                                        peerzone_effective_lst.append(zonemember)
                                                    # for Defined configuration add member to peerzone_lst
                                                    else:
                                                        peerzone_lst.append(zonemember)
                                                line = file.readline()
                                                if not line:
                                                    break
                                        # if line doesn't coreespond to any reg expression pattern then switch line
                                        # until zone description border reached
                                        else:
                                            line = file.readline()
                                            if not line:
                                                break
                                # next line until zoneshow --peer command border reached
                                else:
                                    line = file.readline()                                           
                                if not line:
                                    break
                        # peerzone section end
                status_info('ok', max_title, len(info))
            else:
                status_info('skip', max_title, len(info))
        # save extracted data to json file
        save_data(report_constant_lst, data_names, cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst, peerzone_lst, peerzone_effective_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst, peerzone_lst, peerzone_effective_lst = verify_data(report_constant_lst, data_names, *data_lst)
    return cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst, peerzone_lst, peerzone_effective_lst

