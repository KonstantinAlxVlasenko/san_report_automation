"""Module to extract interswitch connection information and Fabric Shortest Path First (FSPF) link state database"""


import re

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop

from .isl_sections import (islshow_section_extract, lsdbshow_section_extract,
                           porttrunkarea_section_extract,
                           trunkshow_section_extract)


def interswitch_connection_extract(switch_params_df, project_constants_lst):
    """Function to extract interswitch connection information"""  

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'isl_collection')
    # service step information
    print(f'\n\n{project_steps_df.loc[data_names[0], "step_info"]}\n')
    # read data from database if they were saved on previos program execution iteration    
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)

    if force_run:    
        print('\nEXTRACTING INTERSWITCH CONNECTION INFORMATION (ISL, TRUNK, TRUNKAREA) ...\n')   
        
        # number of switches to check
        switch_num = len(switch_params_df.index)   
     
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('isl', max_title)
        lsdb_params = dfop.list_from_dataframe(re_pattern_df, 'lsdb_params')

        # lists to store only REQUIRED infromation
        # collecting data for all switches ports during looping
        isl_lst = []
        trunk_lst = []
        porttrunkarea_lst = []
        lsdb_lst = []

        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} isl, trunk and trunk area ports. Switch mode: {switch_params_sr["switchMode"]}'
            print(info, end =" ")   

            if switch_params_sr["switchMode"] == 'Native':
                current_config_extract(isl_lst, trunk_lst, porttrunkarea_lst, lsdb_lst, pattern_dct, 
                                        switch_params_sr, lsdb_params)
                meop.status_info('ok', max_title, len(info))
            # if switch in Access Gateway mode then skip
            else:
                meop.status_info('skip', max_title, len(info))        
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'isl_columns', 'trunk_columns', 'porttrunkarea_columns', 'lsdb_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, isl_lst, trunk_lst, porttrunkarea_lst, lsdb_lst)
        isl_df, trunk_df, porttrunkarea_df, lsdb_df, *_ = data_lst    
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        isl_df, trunk_df, porttrunkarea_df, lsdb_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return isl_df, trunk_df, porttrunkarea_df, lsdb_df


def current_config_extract(isl_lst, trunk_lst, porttrunkarea_lst, lsdb_lst, pattern_dct, 
                            switch_params_sr, lsdb_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 'switch_index', 
                        'SwitchName', 'switchWwn', 'switchRole', 'Fabric_ID', 'FC_Router', 'switchMode']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False
    sshow_file, _, _, switch_index, *_, switch_mode = switch_info_lst
                     
    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'isl': False, 'trunk': False, 'trunkarea': False, 'lsdb': False}

    if switch_mode == 'Native':
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            # check file until all groups of parameters extracted
            while not all(collected.values()):
                line = file.readline()                        
                if not line:
                    break
                # isl section start   
                if re.search(pattern_dct['switchcmd_islshow'], line) and not collected['isl']:
                    collected['isl'] = True
                    line = meop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line = islshow_section_extract(isl_lst, pattern_dct, switch_info_lst, line, file)                              
                # isl section end
                # trunk section start   
                # switchcmd_trunkshow_comp
                if re.search(pattern_dct['switchcmd_trunkshow'], line) and not collected['trunk']:
                    collected['trunk'] = True
                    line = meop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line = trunkshow_section_extract(trunk_lst, pattern_dct, switch_info_lst, line, file)
                # trunk section end
                # porttrunkarea section start
                if re.search(pattern_dct['switchcmd_trunkarea'], line) and not collected['trunkarea']:
                    collected['trunkarea'] = True
                    line = meop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line = porttrunkarea_section_extract(porttrunkarea_lst, pattern_dct, switch_info_lst, line, file)
                # porttrunkarea section end
                # lsdb section start
                if re.search(pattern_dct['switchcmd_lsdbshow'], line) and not collected['lsdb']:
                    collected['lsdb'] = True
                    line = meop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line = lsdbshow_section_extract(lsdb_lst, pattern_dct, switch_info_lst, lsdb_params, line, file)
                # lsdb section end
