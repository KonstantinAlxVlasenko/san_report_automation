"""Module to extract fabric routing information"""


import itertools
import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects


def fcr_extract(switch_params_lst, report_data_lst):
    """Function to extract fabrics routing information
    """

    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['fcrfabric', 'fcrproxydev', 'fcrphydev', 'lsan', 'fcredge', 'fcrresource']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, \
        fcredge_lst, fcrresource_lst = data_lst
    
    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files  
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
                                'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router']
            switch_info_lst = [switch_params_data_dct.get(key) for key in switch_info_keys]
            ls_mode_on = True if switch_params_data_dct['LS_mode'] == 'ON' else False
            
            # data unpacking from iter param
            sshow_file, *_, switch_name, switch_wwn, switch_role, fid, fc_router = switch_info_lst

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} fabric routing. FC Routing: {fc_router}'
            print(info, end =" ")

            # search control dictionary. continue to check sshow_file until all parameters groups are found                       
            collected = {'fcrfabric': False, 'fcrproxydev': False, 'fcrphydev': False, 
                         'lsanzone': False, 'fcredge': False, 'fcrresource': False} \
                if switch_role == 'Principal' else {'fcredge': False, 'fcrresource': False}
            
            # check config of FC routers only 
            if fc_router == 'ON':
                # fcrouter_info_lst contains sshow_file, chassis_name, switch_index, switch_name, switch_fid
                fcrouter_info_lst = [*switch_info_lst[:4], switch_info_lst[6]]                                        
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
                            if re.search(comp_dct[comp_keys[0]], line) and not collected['fcrfabric']:
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
                                if re.search(comp_dct[comp_keys[re_num]], line) and not collected[fcrdev_type]:
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
                            if re.search(comp_dct[comp_keys[8]], line) and not collected['lsanzone'] and not collected['lsanzone']:
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
                        if re.search(comp_dct[comp_keys[12]], line) and not collected['fcredge']:
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
                                    fcredge_lst.append(line_to_list(comp_dct[comp_keys[13]], line, *fcrouter_info_lst, switch_wwn))                                            
                                if not line:
                                    break   
                        # fcredgeshow section end
                        # fcrresourceshow section start
                        if re.search(comp_dct[comp_keys[14]], line) and not collected['fcrresource']:
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
        save_data(report_data_lst, data_names, fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst = verify_data(report_data_lst, data_names, *data_lst)
            
    return fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst, lsan_lst, fcredge_lst, fcrresource_lst

