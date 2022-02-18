"""Module to extract fabric routing information"""


import itertools
import re

import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


def fcr_membership_extract(switch_params_df, report_creation_info_lst):
    """Function to extract fabrics routing information"""

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['fcrfabric', 'fcrproxydev', 'fcrphydev', 'lsan', 'fcredge', 'fcrresource']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run:              
        print('\nEXTRACTING FABRICS ROUTING INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(switch_params_df.index)
           
        # lists to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        fcrfabric_lst = []
        fcrproxydev_lst = []
        fcrphydev_lst = []
        lsan_lst = []
        fcredge_lst = []
        fcrresource_lst = []
        
        # dictionary to collect fcr device data
        # first element of list is regular expression pattern name,
        # second - is the list to collect data, 
        # third - is the index in line to which slice extracted list 
        fcrdev_dct = {'fcrproxydev': ['switchcmd_fcrproxydevshow', fcrproxydev_lst, None], 'fcrphydev': ['switchcmd_fcrphydevshow', fcrphydev_lst, -3]}    

        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('fcr', max_title)
        fcrresource_params = dfop.list_from_dataframe(re_pattern_df, 'fcrresource_params')

        for i, switch_params_sr in switch_params_df.iterrows():       

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} fabric routing. FC Routing: {switch_params_sr["FC_Router"]}'
            print(info, end =" ")

            if switch_params_sr["FC_Router"] == 'ON':
                current_config_extract(fcrfabric_lst, lsan_lst, fcredge_lst, fcrresource_lst, fcrdev_dct, pattern_dct, 
                                        switch_params_sr, fcrresource_params)                                                            
                meop.status_info('ok', max_title, len(info))
            else:
                meop.status_info('skip', max_title, len(info))

        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'fcrfabric_columns', 'fcrproxydev_columns', 'fcrphydev_columns', 
                                                                'lsan_columns', 'fcredge_columns', 'fcrresource_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst,
                                                            lsan_lst, fcredge_lst, fcrresource_lst)
        fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df, *_ = data_lst    
        # write data to sql db
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)
    return fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df


def current_config_extract(fcrfabric_lst, lsan_lst, fcredge_lst, fcrresource_lst, fcrdev_dct, pattern_dct, 
                            switch_params_sr, fcrresource_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False

    # data unpacking from iter param
    sshow_file, *_, switch_name, _, switch_role, fid, fc_router = switch_info_lst

    # search control dictionary. continue to check sshow_file until all parameters groups are found                       
    collected = {'fcrfabric': False, 'fcrproxydev': False, 'fcrphydev': False, 
                    'lsanzone': False, 'fcredge': False, 'fcrresource': False} \
        if switch_role == 'Principal' else {'fcredge': False, 'fcrresource': False}
    
    # check config of FC routers only 
    if fc_router == 'ON':
        # fcrouter_info_lst contains sshow_file, chassis_name, switch_index, switch_name, switch_fid
        fcrouter_info_lst = [*switch_info_lst[:6], switch_info_lst[7]]                                        
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            # check file until all groups of parameters extracted
            while not all(collected.values()):
                line = file.readline()
                if not line:
                    break
                # check configs of Principal switches only                        
                if switch_role == 'Principal':
                    # fcrfabricshow section start
                    # switchcmd_fcrfabricshow
                    if re.search(pattern_dct['switchcmd_fcrfabricshow'], line) and not collected['fcrfabric']:
                        collected['fcrfabric'] = True
                        if ls_mode_on:
                            while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid}$',line):
                                line = file.readline()
                                if not line:
                                    break 
                        # switchcmd_end_comp
                        while not re.search(pattern_dct['switchcmd_end'], line):
                            line = file.readline()
                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                            # fc_router_match'
                            if match_dct['fc_router']:                                   
                                fcrouter_params_lst = dsop.line_to_list(pattern_dct['fc_router'], line)
                                # check if line is empty                                    
                                while not re.match('\r?\n', line):
                                    line = file.readline()
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # fcr_info_match
                                    if match_dct['fcr_info']:
                                        fcrouter_name = match_dct['fcr_info'].group(1)
                                    # fcr_exports_match                                        
                                    if match_dct['fcr_exports']:
                                        fcrfabric_lst.append(dsop.line_to_list(pattern_dct['fcr_exports'], line, 
                                                                            *fcrouter_info_lst, fcrouter_name, 
                                                                            *fcrouter_params_lst))                                            
                                    if not line:
                                        break                                      
                            if not line:
                                break
                    # fcrfabricshow section end
                    
                    # fcrproxydev and fcrphydev checked in a loop over dictionary keys coz data representation is similar
                    # fcrdevshow section start
                    for fcrdev_type in fcrdev_dct.keys():
                        fcrdev_pattern_name, fcrdev_lst, slice_index = fcrdev_dct[fcrdev_type]
                        # switchcmd_fcrproxydevshow_comp
                        if re.search(pattern_dct[fcrdev_pattern_name], line) and not collected[fcrdev_type]:
                            collected[fcrdev_type] = True                                    
                            if ls_mode_on:
                                while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                                    line = file.readline()
                                    if not line:
                                        break                                                            
                            # switchcmd_end_comp
                            while not re.search(pattern_dct['switchcmd_end'], line):
                                line = file.readline()
                                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                # fcrdevshow_match
                                if match_dct['fcrdevshow']:
                                    fcrdev_lst.append(dsop.line_to_list(pattern_dct['fcrdevshow'], line, *fcrouter_info_lst)[:slice_index])                                            
                                if not line:
                                    break                                
                    # fcrdevshow section end
                    # lsanzoneshow section start
                    if re.search(pattern_dct['switchcmd_lsanzoneshow'], line) and not collected['lsanzone']:
                        collected['lsanzone'] = True
                        if ls_mode_on:
                            while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                                line = file.readline()
                                if not line:
                                    break                      
                        # switchcmd_end_comp
                        while not re.search(pattern_dct['switchcmd_end'], line):
                            match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                            # lsan_name_match
                            if match_dct['lsan_name']:
                                # switch_info and current connected device wwnp
                                lsan_name = dsop.line_to_list(pattern_dct['lsan_name'], line)
                                # move cursor to one line down to get inside while loop
                                line = file.readline()
                                # lsan_switchcmd_end_comp
                                while not re.search(pattern_dct['lsan_switchcmd_end'], line):
                                    # line = file.readline()
                                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                    # lsan_members_match
                                    if match_dct['lsan_members']:
                                        lsan_member = dsop.line_to_list(pattern_dct['lsan_members'], line)
                                        lsan_lst.append([*fcrouter_info_lst, *lsan_name, *lsan_member])
                                    #     line = file.readline()
                                    # else:
                                    #     line = file.readline()
                                    line = file.readline()                                
                                    if not line:
                                        break
                            else:
                                line = file.readline()
                            if not line:
                                break  
                    # lsanzoneshow section end
                
                # fcredge and fcrresource checked for Principal and Subordinate routers
                # fcredgeshow section start
                if re.search(pattern_dct['switchcmd_fcredgeshow'], line) and not collected['fcredge']:
                    collected['fcredge'] = True
                    if ls_mode_on:
                        while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                            line = file.readline()
                            if not line:
                                break                      
                    # switchcmd_end_comp
                    while not re.search(pattern_dct['switchcmd_end'], line):
                        line = file.readline()
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # fcredgeshow_match
                        if match_dct['fcredgeshow']:
                            fcredge_lst.append(dsop.line_to_list(pattern_dct['fcredgeshow'], line, *fcrouter_info_lst))                                            
                        if not line:
                            break   
                # fcredgeshow section end
                # fcrresourceshow section start
                if re.search(pattern_dct['switchcmd_fcrresourceshow'], line) and not collected['fcrresource']:
                    collected['fcrresource'] = True
                    fcrresource_dct = {}
                    if ls_mode_on:
                        while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                            line = file.readline()
                            if not line:
                                break                      
                    # switchcmd_end_comp
                    while not re.search(pattern_dct['switchcmd_end'], line):
                        line = file.readline()
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # fcrresourceshow_match
                        if match_dct['fcrresourceshow']:
                            fcrresource_dct[match_dct['fcrresourceshow'].group(1).rstrip()] = \
                                [match_dct['fcrresourceshow'].group(2), match_dct['fcrresourceshow'].group(3)]                                             
                        if not line:
                            break
                    # each value of dictionary is list of two elements
                    # itertools.chain makes flat tmp_lst list from all lists in dictionary
                    tmp_lst = list(itertools.chain(*[fcrresource_dct.get(param) 
                                                        if fcrresource_dct.get(param) else [None, None] for param in fcrresource_params]))
                    fcrresource_lst.append([*fcrouter_info_lst, *tmp_lst]) 
                # fcrresourceshow section end