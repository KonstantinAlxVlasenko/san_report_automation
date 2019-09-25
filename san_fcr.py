import re
import pandas as pd
import itertools
from files_operations import columns_import, status_info, data_extract_objects, data_to_json 
from files_operations import json_to_data, line_to_list, force_extract_check, update_dct

"""Module to extract fabric routing information"""


def fcr_extract(switch_params_lst, report_data_lst):
    """Function to extract fabrics routing information
    """
    # report_data_lst contains [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 12. FC ROUTING PARAMETERS ...\n')
    
    *_, max_title, report_steps_dct = report_data_lst
    # check if data already have been extracted
    data_names = ['fcrfabric', 'fcrproxydev', 'fcrphydev', 'lsan', 'fcredge', 'fcrresource']
    data_lst = json_to_data(report_data_lst, *data_names)
    fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst = data_lst

    # data force extract check. 
    # if data have been extracted already but extract key is ON then data re-extracted
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # if no data saved than extract data from configurtion files  
    if not all(data_lst) or any(force_extract_keys_lst):             
        print('\nEXTRACTING FABRICS ROUTING INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # extract switch parameters names from init file
        switch_columns = columns_import('switch', max_title, 'columns')
        # number of switches to check
        switch_num = len(switch_params_lst)
           
        # lists to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        fcrfabric_lst = []
        fcrproxydev_lst = []
        fcrphydev_lst = []
        lsan_lst = []
        fcredge_lst = []
        fcrresource_lst = []
        
        # dictionary to collect fcr device data
        # first element of list is regular expression pattern number,
        # second - list to collect data, third is index in line to which slice extracted list 
        fcrdev_dct = {'fcrproxydev': [5, fcrproxydev_lst, None], 'fcrphydev': [6, fcrphydev_lst, -3]}    
   
        # data imported from init file to extract values from config file
        params, _, comp_keys, match_keys, comp_dct = data_extract_objects('fcr', max_title)  
        
        # switch_params_lst [[switch_params_sw1], [switch_params_sw1]]
        # checking each switch for switch level parameters
        for i, switch_params_data in enumerate(switch_params_lst):       
            # dictionary with parameters for the current switch
            switch_params_data_dct = dict(zip(switch_columns, switch_params_data))
            switch_info_keys = ['configname', 'chassis_name', 'switch_index', 
                                'SwitchName', 'switchRole', 'Fabric_ID', 'FC_Router']
            switch_info_lst = [switch_params_data_dct.get(key) for key in switch_info_keys]
            ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False
            
            # data unpacking from iter param
            sshow_file, *_, switch_name, switch_role, fid, fc_router = switch_info_lst

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} fabric routing information check. FC Routing: {fc_router}'
            print(info, end =" ")

            # search control dictionary. continue to check sshow_file until all parameters groups are found                       
            collected = {'fcrfabric': False, 'fcrproxydev': False, 'fcrphydev': False, 
                         'lsanzone': False, 'fcredge': False, 'fcrresource': False} \
                if switch_role == 'Principal' else {'fcredge': False, 'fcrresource': False}
            
            # check config of FC routers only 
            if fc_router == 'ON':
                # fcrouter_info_lst contains sshow_file, chassis_name, switch_index, switch_name, switch_fid
                fcrouter_info_lst = [*switch_info_lst[:4], switch_info_lst[5]]                                        
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
                            if re.search(comp_dct[comp_keys[0]], line):
                                collected['fcrfabric'] = True
                                if ls_mode_on:
                                    while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid}$',line):
                                        line = file.readline()
                                        if not line:
                                            break 
                                # switchcmd_end_comp
                                while not re.search(comp_dct[comp_keys[4]], line):
                                    line = file.readline()
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # fc_router_match'
                                    if match_dct[match_keys[1]]:                                   
                                        fcrouter_params_lst = line_to_list(comp_dct[comp_keys[1]], line)
                                        # check if line is empty                                    
                                        while not re.match('\r?\n', line):
                                            line = file.readline()
                                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                            # fcr_info_match
                                            if match_dct[match_keys[2]]:
                                                fcrouter_name = match_dct[match_keys[2]].group(1)
                                            # fcr_exports_match                                        
                                            if match_dct[match_keys[3]]:
                                                fcrfabric_lst.append(line_to_list(comp_dct[comp_keys[3]], line, 
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
                                re_num, fcrdev_lst, slice_index = fcrdev_dct[fcrdev_type]
                                # switchcmd_fcrproxydevshow_comp
                                if re.search(comp_dct[comp_keys[re_num]], line):
                                    collected[fcrdev_type] = True                                    
                                    if ls_mode_on:
                                        while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                                            line = file.readline()
                                            if not line:
                                                break                                                            
                                    # switchcmd_end_comp
                                    while not re.search(comp_dct[comp_keys[4]], line):
                                        line = file.readline()
                                        match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                        # fcrdevshow_match
                                        if match_dct[match_keys[7]]:
                                            fcrdev_lst.append(line_to_list(comp_dct[comp_keys[7]], line, *fcrouter_info_lst)[:slice_index])                                            
                                        if not line:
                                            break                                
                            # fcrdevshow section end
                            # lsanzoneshow section start
                            if re.search(comp_dct[comp_keys[8]], line):
                                collected['lsanzone'] = True
                                if ls_mode_on:
                                    while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                                        line = file.readline()
                                        if not line:
                                            break                      
                                # switchcmd_end_comp
                                while not re.search(comp_dct[comp_keys[4]], line):
                                    match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # lsan_name_match
                                    if match_dct[match_keys[9]]:
                                        # switch_info and current connected device wwnp
                                        lsan_name = line_to_list(comp_dct[comp_keys[9]], line)
                                        # move cursor to one line down to get inside while loop
                                        line = file.readline()                                
                                        # lsan_switchcmd_end_comp
                                        while not re.search(comp_dct[comp_keys[11]], line):
                                            line = file.readline()
                                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                            # lsan_members_match
                                            if match_dct[match_keys[10]]:
                                                lsan_member = line_to_list(comp_dct[comp_keys[10]], line)
                                                lsan_lst.append([*fcrouter_info_lst, *lsan_name, *lsan_member])                                       
                                            if not line:
                                                break
                                    else:
                                        line = file.readline()
                                    if not line:
                                        break  
                            # lsanzoneshow section end
                        
                        # fcredge and fcrresource checked for Principal and Subordinate routers
                        # fcredgeshow section start
                        if re.search(comp_dct[comp_keys[12]], line):
                            collected['fcredge'] = True
                            if ls_mode_on:
                                while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                                    line = file.readline()
                                    if not line:
                                        break                      
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[4]], line):
                                line = file.readline()
                                match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # fcredgeshow_match
                                if match_dct[match_keys[13]]:
                                    fcredge_lst.append(line_to_list(comp_dct[comp_keys[13]], line, *fcrouter_info_lst))                                            
                                if not line:
                                    break   
                        # fcredgeshow section end
                        # fcrresourceshow section start
                        if re.search(comp_dct[comp_keys[14]], line):
                            print(line)
                            collected['fcrresource'] = True
                            fcrresource_dct = {}
                            if ls_mode_on:
                                while not re.search(fr'^BASE +SWITCH +CONTEXT *-- *FID: *{fid} *$',line):
                                    line = file.readline()
                                    if not line:
                                        break                      
                            # switchcmd_end_comp
                            while not re.search(comp_dct[comp_keys[4]], line):
                                line = file.readline()
                                match_dct = {match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # fcredgeshow_match
                                if match_dct[match_keys[15]]:
                                    fcrresource_dct[match_dct[match_keys[15]].group(1).rstrip()] = \
                                        [match_dct[match_keys[15]].group(2), match_dct[match_keys[15]].group(3)]                                           
                                if not line:
                                    break
                            # each value of dictionary is list of two elements
                            # itertools.chain makes flat tmp_lst list from all lists in dictionary
                            tmp_lst = list(itertools.chain(*[fcrresource_dct.get(param) 
                                                             if fcrresource_dct.get(param) else [None, None] for param in params]))
                            fcrresource_lst.append([*fcrouter_info_lst, *tmp_lst]) 
                        # fcrresourceshow section end  
                                                                          
                status_info('ok', max_title, len(info))
            else:
                status_info('skip', max_title, len(info))
        # save extracted data to json file
        data_to_json(report_data_lst, data_names, fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst)
            
    return fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst

