"""Module to extract port information (sfp transceivers, portcfg, trunk area settings)"""


import re

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.regular_expression_operations as reop
import utilities.servicefile_operations as sfop

from .portcfg_sfp_sections import (portcfgshow_section_extract,
                                   sfpshow_section_extract)


def portcfg_sfp_extract(switch_params_df, project_constants_lst):
    """Function to extract switch port information"""
    
    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst
    
    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'port_sfp_cfg_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data 
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:    
        print('\nEXTRACTING SWITCH PORTS SFP, PORTCFG INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')   
        
        # number of switches to check
        switch_num = len(switch_params_df.index)
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('portinfo', max_title)
        sfp_params, sfp_params_add, portcfg_params = dfop.list_from_dataframe(re_pattern_df, 'sfp_params', 'sfp_params_add', 'portcfg_params')

        # dictionary to save portcfg ALL information for all ports in fabric
        portcfgshow_dct = dict((key, []) for key in portcfg_params)
        # list to store only REQUIRED switch parameters
        # collecting sfpshow data for all switches ports during looping
        sfpshow_lst = []
        # list to save portcfg information for all ports in fabric
        portcfgshow_lst = []
        
        for i, switch_params_sr in switch_params_df.iterrows():
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} ports sfp and cfg'
            print(info, end =" ")                      
            sw_sfpshow_lst = current_config_extract(sfpshow_lst, portcfgshow_dct, pattern_dct,
                                    switch_params_sr, sfp_params, sfp_params_add, portcfg_params)                     
            meop.show_collection_status(sw_sfpshow_lst, max_title, len(info))
        # after check all config files create list of lists from dictionary. 
        # each nested list contains portcfg information for one port
        for portcfg_param in portcfg_params:
            portcfgshow_lst.append(portcfgshow_dct.get(portcfg_param))            
        portcfgshow_lst = list(zip(*portcfgshow_lst))
        
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'sfp_columns', 'portcfg_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, sfpshow_lst, portcfgshow_lst)
        sfpshow_df, portcfgshow_df, *_ = data_lst  
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)   
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        sfpshow_df, portcfgshow_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return sfpshow_df, portcfgshow_df


def current_config_extract(sfpshow_lst, portcfgshow_dct, pattern_dct,
                            switch_params_sr, sfp_params, sfp_params_add, portcfg_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False
    sshow_file, _, _, switch_index, *_ = switch_info_lst

    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'sfpshow': False, 'portcfgshow': False}
    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()                        
            if not line:
                break
            # sfpshow section start
            if re.search(pattern_dct['switchcmd_sfpshow'], line) and not collected['sfpshow']:
                collected['sfpshow'] = True
                line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                line, sw_sfpshow_lst = sfpshow_section_extract(sfpshow_lst, pattern_dct, 
                                                switch_info_lst, sfp_params, sfp_params_add, 
                                                line, file)
            # sfpshow section end
            # portcfgshow section start
            if re.search(pattern_dct['switchcmd_portcfgshow'], line) and not collected['portcfgshow']:
                collected['portcfgshow'] = True
                line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                line = portcfgshow_section_extract(portcfgshow_dct, pattern_dct, 
                                                    switch_info_lst, portcfg_params, 
                                                    line, file)
            # portcfgshow section end
    return sw_sfpshow_lst
