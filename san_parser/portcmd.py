"""Module to extract portFcPortCmdShow information"""


import re


import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop


def portcmd_extract(chassis_params_df, report_creation_info_lst):
    """Function to extract portshow, portloginshow, portstatsshow information"""

    # report_steps_dct contains current step desciption and force and export tags
    report_constant_lst, report_steps_dct, *_ = report_creation_info_lst
    # report_constant_lst contains information: 
    # customer_name, project directory, database directory, max_title
    *_, max_title = report_constant_lst

    # names to save data obtained after current module execution
    data_names = ['portcmd']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # read data from database if they were saved on previos program execution iteration
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)

    # when any data from data_lst was not saved (file not found) or
    # force extract flag is on then re-extract data from configuration files
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run:             
        print('\nEXTRACTING PORTSHOW, PORTLOGINSHOW, PORTSTATSSHOW INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # switch_num = len(chassis_params_fabric_lst)
        switch_num = len(chassis_params_df.index)

        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping       
        portshow_lst = []  
        # data imported from init file to extract values from config file
        pattern_dct, re_pattern_df = sfop.regex_pattern_import('portcmd', max_title)
        portcmd_params, portcmd_params_add = dfop.list_from_dataframe(re_pattern_df, 'portcmd_params', 'portcmd_params_add')
        
        # for i, chassis_params_data in enumerate(chassis_params_fabric_lst):
        for i, chassis_params_sr in chassis_params_df.iterrows():
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_sr["chassis_name"]} switch portshow, portloginshow and statsshow'
            print(info, end =" ")
            # if chassis_params_sr["chassis_name"] == 's1bchwcmn05-fcsw1':
            current_config_extract(portshow_lst, pattern_dct, 
                            chassis_params_sr, portcmd_params, portcmd_params_add)                  
            meop.status_info('ok', max_title, len(info))
        # convert list to DataFrame
        headers_lst = dfop.list_from_dataframe(re_pattern_df, 'portcmd_columns')
        data_lst = dfop.list_to_dataframe(headers_lst, portshow_lst)
        portshow_df, *_ = data_lst
        # write data to sql db
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)      
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        portshow_df, *_ = data_lst
    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)
    return portshow_df


def current_config_extract(portshow_lst, pattern_dct, 
                            chassis_params_sr, portcmd_params, portcmd_params_add):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""

    chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
    chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]

    sshow_file, chassis_name, _ = chassis_info_lst            
    
    # search control dictionary. continue to check sshow_file until all parameters groups are found
    collected = {'portshow': False}
    
    with open(sshow_file, encoding='utf-8', errors='ignore') as file:
        # check file until all groups of parameters extracted
        while not all(collected.values()):
            line = file.readline()
            if not line:
                break
            # sshow_port section start
            if re.search(r'^\| Section: SSHOW_PORT \|$', line):
                # when section is found corresponding collected dict values changed to True
                collected['portshow'] = True
                while not re.search(r'^\| ... rebuilt finished *\|$',line):
                    line = file.readline()
                    if not line:
                        break
                    # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                    match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                    # portFcPortCmdShow section start pattern #0
                    if match_dct['slot_port_number']:
                        # dictionary to store all DISCOVERED parameters
                        # collecting data only for the chassis in current loop
                        portcmd_dct = {}
                        portphys_portscn_details = []
                        # connected devices wwns in portshow section
                        connected_wwn_lst = []
                        # list to store connected devices port_id and wwn pairs in portlogin section
                        portid_wwn_lst = []
                        port_index = None
                        slot_port_lst = dsop.line_to_list(pattern_dct['slot_port_number'], line)
                        while not re.search(r'^portshow +(\d{1,4})$',line):
                            line = file.readline()
                            if not line:
                                break
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # portshow section start pattern #1
                        if match_dct['port_index']:
                            port_index = match_dct['port_index'].group(1)
                            while not re.search(fr'^portloginshow +{int(port_index)}$', line):
                                line = file.readline()
                                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                # two param_names have 1 or 2 values
                                if match_dct['portphys_and_portscn']:
                                    matched_values_lst = dsop.line_to_list(pattern_dct['portphys_and_portscn'], line)
                                    for k, v in zip(matched_values_lst[::3], matched_values_lst[1::3]):
                                            portcmd_dct[k] = v
                                            portphys_portscn_details = [matched_values_lst[2], matched_values_lst[5]]
                                    continue
                                # single param_name have 1 or 2 values has to AFTER portphys_and_portscn
                                elif match_dct['portphys_or_portscn']:
                                    matched_values_lst = dsop.line_to_list(pattern_dct['portphys_or_portscn'], line)
                                    param_name = matched_values_lst[0]
                                    param_value = matched_values_lst[1]
                                    portphys_portscn_details.append(matched_values_lst[2])
                                    portcmd_dct[param_name] = param_value
                                    continue
                                # two or three param_names with single num value
                                elif match_dct['portshow_err_stats']:
                                    matched_values_lst = dsop.line_to_list(pattern_dct['portphys_and_portscn'], line)
                                    for k, v in zip(matched_values_lst[::2], matched_values_lst[1::2]):
                                            portcmd_dct[k] = v
                                    continue
                                # device_connected_wwn_match pattern #7
                                elif match_dct['device_connected_wwn']:
                                    connected_wwn = dsop.line_to_list(pattern_dct['device_connected_wwn'], line)
                                    connected_wwn_lst.append((portcmd_dct.get('portId'), connected_wwn))
                                    continue                                    
                                # param_name with single value without spaces
                                for pattern_name in ['single_param_complete_line', 'portstate', 'portwwn', 'single_param_single_value']:
                                    if match_dct[pattern_name]:
                                        param_name = match_dct[pattern_name].group(1).rstrip()
                                        param_value = match_dct[pattern_name].group(2).rstrip()
                                        portcmd_dct[param_name] = param_value
                                        break
                                if not line:
                                    break
                        # portshow section end
                        # portlogin section start                                      
                        if re.match(fr'^portloginshow +{int(port_index)}$', line):

                            while not re.search(fr'^portregshow +{int(port_index)}$', line):
                                line = file.readline()
                                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                # connected_wwn_match pattern #3
                                if match_dct['login_connected_wwn']:
                                    # first value in tuple unpacking is fe or fd and not required
                                    _, port_id, wwn = dsop.line_to_list(pattern_dct['login_connected_wwn'], line)
                                    # port_id = '0x' + port_id
                                    portid_wwn_lst.append((port_id, wwn))
                                if not line:
                                    break
                            # sorting connected devices list by port_ids
                            if len(portid_wwn_lst) != 0:
                                portid_wwn_lst = sorted(portid_wwn_lst)
                            # if portlogin empty then use connected devices from portshow section
                            # applied for E-ports
                            elif len(connected_wwn_lst) != 0:
                                portid_wwn_lst = connected_wwn_lst.copy()
                            # adding port_id and None wwn if no device is connected or slave trunk link
                            else:
                                portid_wwn_lst.append([portcmd_dct.get('portId'), None])
                        # portlogin section end
                        while not re.match(fr'^portstatsshow +{int(port_index)}$', line):
                            line = file.readline()
                            if not line:
                                break
                        # portstatsshow section start
                        if re.match(fr'^portstatsshow +{int(port_index)}$', line):
                            while not re.search(fr'^(portstats64show|portcamshow) +{int(port_index)}$', line):
                                line = file.readline()
                                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                                # port information without virtual channel numbers pattern #4
                                if match_dct['portstats']:
                                    portcmd_dct[match_dct['portstats'].group(1).rstrip()] = match_dct['portstats'].group(2)
                                # port information with virtual channel numbers pattern #5
                                elif match_dct['portstats_vc']:
                                    line_values = dsop.line_to_list(pattern_dct['portstats_vc'], line)
                                    param_name, start_vc = line_values[0:2]
                                    for i, value in enumerate(line_values[3:]):
                                        param_name_vc = param_name + '_' + str(int(start_vc) + i)
                                        portcmd_dct[param_name_vc] = value
                                if not line:
                                    break
                        # portstatsshow section end
                        # portFcPortCmdShow section end        
                        
                        portphys_portscn_details = [value if value else None for value in portphys_portscn_details]
                        # additional values which need to be added to the dictionary with all DISCOVERED parameters during current loop iteration
                        # chassis_slot_port_values order (configname, chassis_name, port_index, slot_num, port_num, port_ids and wwns of connected devices)
                        # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column
                        for port_id, connected_wwn in portid_wwn_lst:                            
                            chassis_slot_port_values = [*chassis_info_lst, port_index, *slot_port_lst, port_id, connected_wwn, *portphys_portscn_details]
                            # adding or changing data from chassis_slot_port_values to the DISCOVERED dictionary
                            dsop.update_dct(portcmd_params_add, chassis_slot_port_values, portcmd_dct)
                            # adding data to the REQUIRED list for each device connected to the port 
                            portshow_lst.append([portcmd_dct.get(portcmd_param, None) for portcmd_param in portcmd_params])
            # sshow_port section end