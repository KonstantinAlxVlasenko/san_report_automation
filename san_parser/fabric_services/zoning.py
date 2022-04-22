"""Module to extract zoning information"""


import re

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.regular_expression_operations as reop
import utilities.servicefile_operations as sfop

from .zoning_sections import (peer_zoning_section_extract,
                              regular_zoning_section_extract)


def zoning_extract(switch_params_df, project_constants_lst):
    """Function to extract zoning information"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'zoning_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:              
        print('\nEXTRACTING ZONING INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(switch_params_df.index)   
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('zoning', max_title) 
        
        # nested list(s) to store required values of the module in defined order for all switches in SAN
        san_cfg_lst = []
        san_zone_lst = []
        san_alias_lst = []
        san_cfg_effective_lst = []
        san_zone_effective_lst = []
        san_peerzone_effective_lst = []
        san_peerzone_lst = []

        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():       
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} zoning. Switch role: {switch_params_sr["switchRole"]}'
            print(info, end =" ")

            if switch_params_sr["switchRole"] == 'Principal':
                sw_zone_lst = current_config_extract(san_cfg_lst, san_zone_lst, san_peerzone_lst, san_alias_lst, 
                                                        san_cfg_effective_lst, san_zone_effective_lst, san_peerzone_effective_lst, 
                                                        pattern_dct, switch_params_sr)
                meop.show_collection_status(sw_zone_lst, max_title, len(info))
            else:
                meop.status_info('skip', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'cfg_columns', 'zone_columns', 'alias_columns',
                                                                'cfg_effective_columns', 'zone_effective_columns',
                                                                'peerzone_columns', 'peerzone_effective_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, san_cfg_lst, san_zone_lst, san_alias_lst, 
                                                        san_cfg_effective_lst, san_zone_effective_lst,
                                                        san_peerzone_lst, san_peerzone_effective_lst)
        cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df, *_ = data_lst
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return cfg_df, zone_df, alias_df, cfg_effective_df, zone_effective_df, peerzone_df, peerzone_effective_df


def current_config_extract(san_cfg_lst, san_zone_lst, san_peerzone_lst, san_alias_lst, 
                            san_cfg_effective_lst, san_zone_effective_lst, san_peerzone_effective_lst, 
                            pattern_dct, switch_params_sr):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False
    sshow_file, *_, switch_index, switch_name, _, switch_role = switch_info_lst[:7]
    collected = {'cfgshow': False, 'peerzone': False}
    sw_zone_lst = []
    
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
                    collected['cfgshow'] = True
                    line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line, sw_zone_lst = regular_zoning_section_extract(san_cfg_lst, san_zone_lst, san_alias_lst, 
                                                                        san_cfg_effective_lst, san_zone_effective_lst, pattern_dct,
                                                                        principal_switch_lst, line, file) 
                # cfgshow section end
                # peerzone section start
                elif re.search(pattern_dct['switchcmd_peerzone'], line) and not collected['peerzone']:
                    # when section is found corresponding collected dict values changed to True
                    collected['peerzone'] = True
                    line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line = peer_zoning_section_extract(san_peerzone_lst, san_peerzone_effective_lst, pattern_dct,
                                                        principal_switch_lst, line, file)
                # peerzone section end
    return sw_zone_lst