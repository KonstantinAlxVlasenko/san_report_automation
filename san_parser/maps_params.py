"""Module to extract maps parameters"""

import os.path
import re


import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop


def maps_params_extract(all_config_data, report_creation_info_lst):
    """Function to extract MAPS parameters"""

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['maps_parameters']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)
    
    
    # when any data from data_lst was not saved (file not found) or 
    # force extract flag is on then re-extract data from configuration files  
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)

    if force_run:
        print('\nEXTRACTING MAPS DATA FROM AMS_MAPS_LOG CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(all_config_data)    
        # list to store only REQUIRED parameters
        # collecting data for all switches during looping 
        maps_params_fabric_lst = []
        # data imported from init file to extract values from config file
        # maps_params, maps_params_add, comp_keys, match_keys, comp_dct = sfop.data_extract_objects('maps', max_title)

        pattern_dct, re_pattern_df = sfop.regex_pattern_import('maps', max_title)
        maps_params, maps_params_add = dfop.list_from_dataframe(re_pattern_df, 'maps_params', 'maps_params_add')
        
        # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
        # checking each config set(supportshow file) for chassis level parameters
        for i, switch_config_data in enumerate(all_config_data):
            # data unpacking from iter param
            switch_name, sshow_file, ams_maps_files = switch_config_data
            # number of ams_maps configs
            num_maps = len(ams_maps_files) if ams_maps_files else 0    
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} MAPS parameters. Number of AMS_MAPS configs: {num_maps} ...'
            print(info)
            
            # checking ams_maps log file for each logical switch
            if ams_maps_files:
                for ams_maps_file in ams_maps_files:
                    
                    info = ' '*16+f'{os.path.basename(ams_maps_file)} processing'
                    print(info, end =" ")

                    maps_params_lst = current_config_extract(maps_params_fabric_lst, pattern_dct, 
                                                            switch_name, sshow_file, ams_maps_file, 
                                                            maps_params, maps_params_add)
                    # # search control dictionary. continue to check sshow_file until all parameters groups are found
                    # collected = {'switch_index': False, 'global_dash': False}
                    # # dictionary to store all DISCOVERED switch parameters
                    # # collecting data only for the logical switch in current loop
                    # maps_params_dct = {}
                    
                    # info = ' '*16+f'{os.path.basename(ams_maps_file)} processing'
                    # print(info, end =" ")
                
                    # with open(ams_maps_file, encoding='utf-8', errors='ignore') as file:
                    #     # check file until all groups of parameters extracted
                    #     while not all(collected.values()):
                    #         line = file.readline()
                    #         if not line:
                    #             break
                    #         # logical switch index section start
                    #         if re.search(r'^[= ]*AMS/MAPS *Data *Switch *(\d+)[= ]*$', line):
                    #             # when section is found corresponding collected dict values changed to True
                    #             collected['switch_index'] = True
                    #             match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    #             # pattern #0
                    #             switch_index = match_dct['switch_index'].group(1)
                    #         # logical switch index section end
                    #         # global dashboard section start
                    #         if re.search(r'^[- ]*MAPS +Global +Monitoring +Configuration[ -]*$', line):
                    #             collected['global_dash'] = True
                    #             while not re.search(r'^[- ]*NM +Data[- ]*$',line):
                    #                 line = file.readline()
                    #                 # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    #                 match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    #                 # match_keys ['switch_index_match', 'dashboard_match', 'report_match', 'no_lic_match'] 
                    #                 # 'dashboard_match' pattern #1
                    #                 if match_dct['dashborad']:
                    #                     maps_params_dct[match_dct['dashborad'].group(1).rstrip()] = match_dct['dashborad'].group(2)                            
                    #                 # 'report_match' pattern #2
                    #                 if match_dct['report']:
                    #                     maps_params_dct[match_dct['report'].group(1).rstrip()] = match_dct['report'].group(2)
                    #                 # 'no Fabric lic match' pattern #3
                    #                 if match_dct['no_lic']:
                    #                     for maps_param in maps_params[6:23]:
                    #                         maps_params_dct[maps_param] = 'No FV lic'                                         
                    #                 if not line:
                    #                     break
                    #         # global dashboard section end
                                
                    # # additional values which need to be added to the chassis params dictionary
                    # # chassis_params_add order (configname, ams_maps_config, chassis_name, switch_index)
                    # # values axtracted in manual mode. if change values order change keys order in init.xlsx "maps_params_add" column
                    # maps_params_values = (sshow_file, ams_maps_file, switch_name, switch_index)
                    
                    # # adding additional parameters and values to the chassis_params_switch_dct
                    # for maps_param_add, maps_param_value in zip(maps_params_add,  maps_params_values):
                    #         maps_params_dct[maps_param_add] = maps_param_value

                    # # creating list with REQUIRED maps parameters for the current switch
                    # # if no value in the maps_params_dct for the parameter then None is added
                    # maps_params_lst = [maps_params_dct.get(maps_param, None) for maps_param in maps_params]  
                    # # and appending this list to the list of all switches maps_params_fabric_lst
                    # maps_params_fabric_lst.append(maps_params_lst)

                    if dsop.list_is_empty(maps_params_lst):
                        meop.status_info('no data', max_title, len(info))
                    else:
                        meop.status_info('ok', max_title, len(info))
            else:
                info = ' '*16+'No AMS_MAPS configuration found.'
                print(info, end =" ")
                meop.status_info('skip', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'maps_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, maps_params_fabric_lst)
        maps_params_fabric_df, *_ = data_lst
        # write data to sql db
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)    
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        maps_params_fabric_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)
    return maps_params_fabric_df


def current_config_extract(maps_params_fabric_lst, pattern_dct, 
                            switch_name, sshow_file, ams_maps_file,
                            maps_params, maps_params_add):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""


    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'switch_index': False, 'global_dash': False}
    # dictionary to store all DISCOVERED switch parameters
    # collecting data only for the logical switch in current loop
    maps_params_dct = {}
    
    # info = ' '*16+f'{os.path.basename(ams_maps_file)} processing'
    # print(info, end =" ")

    with open(ams_maps_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # logical switch index section start
            if re.search(r'^[= ]*AMS/MAPS *Data *Switch *(\d+)[= ]*$', line):
                # when section is found corresponding collected dict values changed to True
                collected['switch_index'] = True
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # pattern #0
                switch_index = match_dct['switch_index'].group(1)
            # logical switch index section end
            # global dashboard section start
            if re.search(r'^[- ]*MAPS +Global +Monitoring +Configuration[ -]*$', line):
                collected['global_dash'] = True
                while not re.search(r'^[- ]*NM +Data[- ]*$',line):
                    line = file.readline()
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # match_keys ['switch_index_match', 'dashboard_match', 'report_match', 'no_lic_match'] 
                    # 'dashboard_match' pattern #1
                    if match_dct['dashborad']:
                        maps_params_dct[match_dct['dashborad'].group(1).rstrip()] = match_dct['dashborad'].group(2)                            
                    # 'report_match' pattern #2
                    if match_dct['report']:
                        maps_params_dct[match_dct['report'].group(1).rstrip()] = match_dct['report'].group(2)
                    # 'no Fabric lic match' pattern #3
                    if match_dct['no_lic']:
                        for maps_param in maps_params[6:23]:
                            maps_params_dct[maps_param] = 'No FV lic'                                         
                    if not line:
                        break
            # global dashboard section end
                
    # additional values which need to be added to the chassis params dictionary
    # chassis_params_add order (configname, ams_maps_config, chassis_name, switch_index)
    # values axtracted in manual mode. if change values order change keys order in init.xlsx "maps_params_add" column
    maps_params_values = (sshow_file, ams_maps_file, switch_name, switch_index)
    
    # adding additional parameters and values to the chassis_params_switch_dct
    for maps_param_add, maps_param_value in zip(maps_params_add,  maps_params_values):
            maps_params_dct[maps_param_add] = maps_param_value

    # creating list with REQUIRED maps parameters for the current switch
    # if no value in the maps_params_dct for the parameter then None is added
    maps_params_lst = [maps_params_dct.get(maps_param, None) for maps_param in maps_params]  
    # and appending this list to the list of all switches maps_params_fabric_lst
    maps_params_fabric_lst.append(maps_params_lst)
    return maps_params_lst