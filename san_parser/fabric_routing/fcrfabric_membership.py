"""Module to extract fabric routing information"""


import itertools
import re

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.regular_expression_operations as reop
import utilities.servicefile_operations as sfop

from .fcrfabric_membership_sections import (fcrfabricshow_section_extract,
                                            fcrresourceshow_section_extract,
                                            goto_baseswitch_context_fid,
                                            lsanzoneshow_section_extract)


def fcr_membership_extract(switch_params_df, project_constants_lst):
    """Function to extract fabrics routing information"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'fcr_membership_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data 
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
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
        fcrxlateconfig_lst = []
        
        # dictionary to collect fcr device data
        # first element of list is regular expression pattern name of line where section is started,
        # second - is the list to collect data, 
        fcrdev_dct = {'fcrproxydev': ['switchcmd_fcrproxydevshow', fcrproxydev_lst], 
                        'fcrphydev': ['switchcmd_fcrphydevshow', fcrphydev_lst]}    

        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('fcr', max_title)
        fcrresource_params = dfop.list_from_dataframe(re_pattern_df, 'fcrresource_params')

        for i, switch_params_sr in switch_params_df.iterrows():       

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} fabric routing. FC Routing: {switch_params_sr["FC_Router"]}'
            print(info, end =" ")

            if switch_params_sr["FC_Router"] == 'ON':
                current_config_extract(fcrfabric_lst, lsan_lst, fcredge_lst, fcrresource_lst, fcrdev_dct, fcrxlateconfig_lst, 
                                        pattern_dct, switch_params_sr, fcrresource_params)                                                            
                meop.status_info('ok', max_title, len(info))
            else:
                meop.status_info('skip', max_title, len(info))

        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'fcrfabric_columns', 'fcrproxydev_columns', 'fcrphydev_columns', 
                                                                'lsan_columns', 'fcredge_columns', 'fcrresource_columns', 'fcrxlateconfig_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, fcrfabric_lst, fcrproxydev_lst, fcrphydev_lst,
                                                            lsan_lst, fcredge_lst, fcrresource_lst, fcrxlateconfig_lst)
        fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df, fcrxlateconfig_df, *_ = data_lst    
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df, fcrxlateconfig_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return fcrfabric_df, fcrproxydev_df, fcrphydev_df, lsan_df, fcredge_df, fcrresource_df, fcrxlateconfig_df


def current_config_extract(fcrfabric_lst, lsan_lst, fcredge_lst, fcrresource_lst, fcrdev_dct, fcrxlateconfig_lst, 
                            pattern_dct, switch_params_sr, fcrresource_params):
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
                    'lsanzone': False, 'fcredge': False, 'fcrresource': False, 'fcrxlateconfig': False} \
        if switch_role == 'Principal' else {'fcredge': False, 'fcrresource': False, 'fcrxlateconfig': False}
    
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
                    if re.search(pattern_dct['switchcmd_fcrfabricshow'], line) and not collected['fcrfabric']:
                        collected['fcrfabric'] = True
                        line = goto_baseswitch_context_fid(ls_mode_on, line, file, fid)
                        line = fcrfabricshow_section_extract(fcrfabric_lst, pattern_dct, 
                                                                fcrouter_info_lst, line, file)
                    # fcrfabricshow section end
                    # fcrproxydev and fcrphydev are checked in a loop
                    # fcrdevshow section start
                    for fcrdev_type in fcrdev_dct.keys():
                        switchcmd_pattern_name, fcrdev_lst = fcrdev_dct[fcrdev_type]
                        if re.search(pattern_dct[switchcmd_pattern_name], line) and not collected[fcrdev_type]:
                            collected[fcrdev_type] = True                                    
                            line = goto_baseswitch_context_fid(ls_mode_on, line, file, fid)
                            line = reop.lines_extract(fcrdev_lst, pattern_dct, fcrdev_type, fcrouter_info_lst, 
                                                        line, file)
                    # fcrdevshow section end
                    # lsanzoneshow section start
                    if re.search(pattern_dct['switchcmd_lsanzoneshow'], line) and not collected['lsanzone']:
                        collected['lsanzone'] = True
                        line = goto_baseswitch_context_fid(ls_mode_on, line, file, fid)
                        line = lsanzoneshow_section_extract(lsan_lst, pattern_dct, fcrouter_info_lst,
                                                            line, file)
                    # lsanzoneshow section end
                # fcredge and fcrresource checked for Principal and Subordinate routers
                # fcredgeshow section start
                if re.search(pattern_dct['switchcmd_fcredgeshow'], line) and not collected['fcredge']:
                    collected['fcredge'] = True
                    line = goto_baseswitch_context_fid(ls_mode_on, line, file, fid)
                    line = reop.lines_extract(fcredge_lst, pattern_dct, 'fcredgeshow', fcrouter_info_lst, line, file)
                # fcredgeshow section end
                # fcrxlateconfig section start
                if re.search(pattern_dct['switchcmd_fcrxlateconfig'], line) and not collected['fcrxlateconfig']:
                    collected['fcrxlateconfig'] = True
                    line = goto_baseswitch_context_fid(ls_mode_on, line, file, fid)
                    line = reop.lines_extract(fcrxlateconfig_lst, pattern_dct, 'fcrxlateconfig', fcrouter_info_lst, line, file)
                # fcrxlateconfig section end
                # fcrresourceshow section start
                if re.search(pattern_dct['switchcmd_fcrresourceshow'], line) and not collected['fcrresource']:
                    collected['fcrresource'] = True
                    line = goto_baseswitch_context_fid(ls_mode_on, line, file, fid)
                    line = fcrresourceshow_section_extract(fcrresource_lst, pattern_dct,
                                                            fcrouter_info_lst, fcrresource_params,
                                                            line, file)
                # fcrresourceshow section end


