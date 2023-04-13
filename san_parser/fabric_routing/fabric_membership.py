"""Module to extract fabrics information"""

import re

import utilities.data_structure_operations as dsop
import utilities.database_operations as dbop
import utilities.dataframe_operations as dfop
import utilities.filesystem_operations as fsop
import utilities.module_execution as meop
import utilities.regular_expression_operations as reop
import utilities.report_operations as report
import utilities.servicefile_operations as sfop

from .fabric_sections import agshow_section_extract


def fabric_membership_extract(switch_params_df, project_constants_lst):
    """Function to extract from principal switch configuration 
    list of switches in fabric including AG switches"""

    # imported project constants required for module execution
    project_steps_df, max_title, io_data_names_df, *_ = project_constants_lst

    # data titles obtained after module execution
    data_names = dfop.list_from_dataframe(io_data_names_df, 'fabric_membership_collection')
    # module information
    meop.show_module_info(project_steps_df, data_names)
    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(project_constants_lst, *data_names)
    
    # force run when any output data from data_lst is not found in database or 
    # procedure execution explicitly requested (force_run flag is on) for any output data  
    force_run = meop.verify_force_run(data_names, data_lst, project_steps_df, max_title)
    
    if force_run:                 
        print('\nEXTRACTING FABRICS INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(switch_params_df.index)
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping
        san_fabricshow_lst = []
        san_ag_principal_lst = []    
        
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('fabric', max_title)
        ag_params = dfop.list_from_dataframe(re_pattern_df, 'ag_params')          
        
        # checking each switch for switch level parameters
        for i, switch_params_sr in switch_params_df.iterrows():        
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_params_sr["SwitchName"]} fabric environment. Switch role: {switch_params_sr["switchRole"]}'
            print(info, end =" ")

            if switch_params_sr["switchRole"] == 'Principal':
                sw_fabricshow_lst = current_config_extract(san_fabricshow_lst, san_ag_principal_lst, pattern_dct, 
                                                                        switch_params_sr, ag_params)
                meop.show_collection_status(sw_fabricshow_lst, max_title, len(info))
            else:
                meop.status_info('skip', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'fabric_columns', 'ag_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, san_fabricshow_lst, san_ag_principal_lst)
        fabricshow_df, ag_principal_df, *_ = data_lst    
        # write data to sql db
        dbop.write_database(project_constants_lst, data_names, *data_lst)  
    else:
        data_lst = dbop.verify_read_data(max_title, data_names, *data_lst)
        fabricshow_df, ag_principal_df = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        report.dataframe_to_excel(data_frame, data_name, project_constants_lst)
    return fabricshow_df, ag_principal_df


def current_config_extract(san_fabricshow_lst, san_ag_principal_lst, pattern_dct, 
                            switch_params_sr, ag_params):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    switch_info_keys = ['configname', 'chassis_name', 'chassis_wwn', 
                        'switch_index', 'SwitchName', 'switchWwn', 'switchRole', 
                        'Fabric_ID', 'FC_Router', 'switchMode']
    switch_info_lst = [switch_params_sr[key] for key in switch_info_keys]
    ls_mode_on = True if switch_params_sr['LS_mode'] == 'ON' else False 
    
    sshow_file, _, _, switch_index, switch_name, _, switch_role = switch_info_lst[:7]
    collected = {'fabricshow': False, 'ag_principal': False}
    
    # check config of Principal switch only 
    if switch_role == 'Principal':
        # principal_switch_lst contains sshow_file, chassis_name, chassis_wwn, switch_index, switch_name, switch_fid
        principal_switch_lst = [*switch_info_lst[:6], *switch_info_lst[7:9]]
                                
        # search control dictionary. continue to check sshow_file until all parameters groups are found
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            # check file until all groups of parameters extracted
            while not all(collected.values()):
                line = file.readline()
                if not line:
                    break
                # fabricshow section start
                if re.search(pattern_dct['switchcmd_fabricshow'], line):
                    # when section is found corresponding collected dict values changed to True
                    collected['fabricshow'] = True
                    line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line, sw_fabricshow_lst = reop.extract_list_from_line(san_fabricshow_lst, pattern_dct, line, file, 
                                                                                        extract_pattern_name='fabricshow', 
                                                                                        save_local=True, line_add_values=principal_switch_lst)
                # fabricshow section end
                # ag_principal section start
                elif re.search(pattern_dct['switchcmd_agshow'], line):
                    collected['ag_principal'] = True
                    line = reop.goto_switch_context(ls_mode_on, line, file, switch_index)
                    line = agshow_section_extract(san_ag_principal_lst, pattern_dct, principal_switch_lst, ag_params, line, file)
                # ag_principal section end

    return sw_fabricshow_lst
