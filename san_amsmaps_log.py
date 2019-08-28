import re
import pandas as pd
import os.path
# from files_operations import columns_import, status_info, data_extract_objects
from files_operations import status_info, data_extract_objects, data_to_json, json_to_data

"""Module to extract maps parameters"""


def maps_params_extract(all_config_data, report_data_lst):
    """Function to extract maps parameters
    """
    # report_data_lst = [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 6. MAPS PARAMETERS ...\n')
    
    *_, max_title = report_data_lst
    # check if data already have been extracted
    data_names = ['maps_parameters']
    data_lst = json_to_data(report_data_lst, *data_names)
    maps_params_fabric_lst, = data_lst
    
    # if no data saved than extract data from configurtion files
    if not all(data_lst):    
        print('\nEXTRACTING MAPS DATA FROM AMS_MAPS_LOG CONFIGURATION FILES ...\n')
        
        # number of switches to check
        switch_num = len(all_config_data)    
        # list to store only REQUIRED parameters
        # collecting data for all switches during looping 
        maps_params_fabric_lst = []
        # data imported from init file to extract values from config file
        maps_params, maps_params_add, comp_keys, match_keys, comp_dct = data_extract_objects('maps', max_title)
        
        # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
        # checking each config set(supportshow file) for chassis level parameters
        for i, switch_config_data in enumerate(all_config_data):
            # data unpacking from iter param
            switch_name, sshow_file, ams_maps_files = switch_config_data
            # number of ams_maps configs
            num_maps = len(ams_maps_files) if ams_maps_files else 0    
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {switch_name} MAPS parameters check. Number of AMS_MAPS configs: {num_maps} ...'
            print(info)
            
            # checking ams_maps log file for each logical switch
            if ams_maps_files:
                for ams_maps_file in ams_maps_files:
                    # search control dictionary. continue to check sshow_file until all parameters groups are found
                    collected = {'switch_index': False, 'global_dash': False}
                    # dictionary to store all DISCOVERED switch parameters
                    # collecting data only for the logical switch in current loop
                    maps_params_dct = {}
                    
                    info = ' '*16+f'{os.path.basename(ams_maps_file)} processing'
                    print(info, end =" ")
                
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
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                switch_index = match_dct[match_keys[0]].group(1)
                            # logical switch index section end
                            # global dashboard section start
                            if re.search(r'^[- ]*MAPS +Global +Monitoring +Configuration[ -]*$', line):
                                collected['global_dash'] = True
                                while not re.search(r'^[- ]*NM +Data[- ]*$',line):
                                    line = file.readline()
                                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                                    match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                    # match_keys ['switch_index_match', 'dashboard_match', 'report_match', 'no_lic_match'] 
                                    # 'dashboard_match'
                                    if match_dct[match_keys[1]]:
                                        maps_params_dct[match_dct[match_keys[1]].group(1).rstrip()] = match_dct[match_keys[1]].group(2)                            
                                    # 'report_match'
                                    if match_dct[match_keys[2]]:
                                        maps_params_dct[match_dct[match_keys[2]].group(1).rstrip()] = match_dct[match_keys[2]].group(2)
                                    # 'no Fabric lic match'
                                    if match_dct[match_keys[3]]:
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
                    # and appending this list to the list of all switches maps_params_fabric_lst
                    maps_params_fabric_lst.append([maps_params_dct.get(maps_param, None) for maps_param in maps_params])
                
                    status_info('ok', max_title, len(info))
            else:
                info = ' '*16+'No AMS_MAPS configuration found.'
                print(info, end =" ")
                status_info('skip', max_title, len(info))
        # save extracted data to json file
        data_to_json(report_data_lst, data_names, maps_params_fabric_lst)
        
    return maps_params_fabric_lst