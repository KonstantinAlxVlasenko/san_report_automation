"""Module to extract zoning information"""


import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


def zoning_extract(switch_params_df, report_creation_info_lst):
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

    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)

    if force_run:              
        print('\nEXTRACTING ZONING INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(switch_params_df.index)   
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('zoning', max_title) 
        
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        cfg_lst = []
        zone_lst = []
        alias_lst = []
        cfg_effective_lst = []
        zone_effective_lst = []
        peerzone_effective_lst = []
        peerzone_lst = []

        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():       
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} zoning. Switch role: {switch_params_sr["switchRole"]}'
            print(info, end =" ")

            if switch_params_sr["switchRole"] == 'Principal':
                current_config_extract(cfg_lst, zone_lst, peerzone_lst, alias_lst, 
                                        cfg_effective_lst, zone_effective_lst, peerzone_effective_lst, 
                                        pattern_dct, switch_params_sr)
                meop.status_info('ok', max_title, len(info))
            else:
                meop.status_info('skip', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'cfg_columns', 'zone_columns', 'alias_columns',
                                                                'cfg_effective_columns', 'zone_effective_columns',
                                                                'peerzone_columns', 'peerzone_effective_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, cfg_lst, zone_lst, alias_lst, 
                                                        cfg_effective_lst, zone_effective_lst,
                                                        peerzone_lst, peerzone_effective_lst)
        cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df, *_ = data_lst
        # write data to sql db
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)
    return cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df


def current_config_extract(cfg_lst, zone_lst, peerzone_lst, alias_lst, 
                            cfg_effective_lst, zone_effective_lst, peerzone_effective_lst, pattern_dct, 
                            switch_params_sr):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False

    sshow_file, *_, switch_index, switch_name, _, switch_role = switch_info_lst[:7]
    
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
                if re.search(pattern_dct['switchcmd_cfgshow'], line) and not collected['cfgshow']:
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
                    while not re.search(pattern_dct['switchcmd_end'], line):                               
                        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # if Effective configuration line passed
                        if match_dct['effective']:
                            effective = True                                     
                        # 'cfg_match'
                        if match_dct['cfg']:
                            cfg_line = dsop.line_to_list(pattern_dct['cfg'], line)
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
                            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                                # find all zone names in line
                                members_lst = re.findall(pattern_dct['zones'], line)
                                for member in members_lst:
                                    cfg_lst.append([*principal_switch_lst, cfg_name, member.rstrip(';')])
                                line = file.readline()
                                if not line:
                                    break
                        # 'zone_match'
                        elif match_dct['zone']:
                            zone_line = dsop.line_to_list(pattern_dct['zone'], line)
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
                            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                                members_lst = re.findall(pattern_dct['zones'], line)
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
                        elif match_dct['alias']:
                            alias_line = dsop.line_to_list(pattern_dct['alias'], line)
                            alias_name = alias_line[0]
                            # if line contains alias name and alias member
                            if alias_line[1]:
                                member_lst = alias_line[1].strip().replace(';', '').split()
                                for member in member_lst:
                                    alias_lst.append([*principal_switch_lst, alias_name, member])
                            line = file.readline()
                            # zoning_switchcmd_end_comp separates different configs, zones and aliases
                            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                                member_lst = re.findall(pattern_dct['zones'], line)
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
                elif re.search(pattern_dct['switchcmd_zonehow'], line) and not collected['peerzone']:
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
                    while not re.search(pattern_dct['switchcmd_end'], line):
                        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}                              
                        # if Effective configuration line passed
                        if match_dct['effective']:
                            peerzone_effective = True                                     
                        # 'zone_match'
                        if match_dct['zone']:
                            zone_line = dsop.line_to_list(pattern_dct['zone'], line)
                            zone_name = zone_line[0]
                            line = file.readline()
                            # zoning_switchcmd_end_comp separates different zones
                            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}  
                                # peerzone_property_match
                                if match_dct['peerzone_property']:
                                    # peerzone_property is tuple. contains property member ot created by info
                                    peerzone_property = match_dct['peerzone_property'].groups()
                                    zonemember = [*principal_switch_lst, zone_name, *peerzone_property]
                                    # for Effective configuration add member to peerzone_effective_lst
                                    if peerzone_effective:
                                        peerzone_effective_lst.append(zonemember)
                                    # for Defined configuration add member to peerzone_lst
                                    else:
                                        peerzone_lst.append(zonemember)
                                    line = file.readline()
                                # peerzone_member_type_match (principal or peer)
                                elif match_dct['peerzone_member_type']:
                                    member_type = match_dct['peerzone_member_type'].group(1)
                                    line = file.readline()
                                    # peerzone_member_end_comp separates peer and principals groups
                                    while not re.search(pattern_dct['peerzone_member_end'], line):
                                        # find zonemembers
                                        members_lst = re.findall(pattern_dct['zones'], line)
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