"""Module to extract portFcPortCmdShow information"""


import re

import pandas as pd

from common_operations_filesystem import load_data, save_data
from common_operations_miscellaneous import (
    force_extract_check, line_to_list, status_info, update_dct, verify_data)
from common_operations_servicefile import columns_import, data_extract_objects


def portcmdshow_extract(chassis_params_fabric_lst, report_data_lst):
    """Function to extract portshow, portloginshow, portstatsshow information"""

    # report_data_lst contains information: 
    # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['portcmd']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')

    # load data if they were saved on previos program execution iteration
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    portshow_lst, = data_lst

    # data force extract check 
    # list of keys for each data from data_lst representing if it is required 
    # to re-collect or re-analyze data even they were obtained on previous iterations
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # print data which were loaded but for which force extract flag is on
    force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)
    
    # when any of data_lst was not saved or 
    # force extract flag is on then re-extract data  from configueation files  
    if not all(data_lst) or any(force_extract_keys_lst):             
        print('\nEXTRACTING PORTSHOW, PORTLOGINSHOW, PORTSTATSSHOW INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # extract chassis parameters names from init file
        chassis_columns = columns_import('chassis', max_title, 'columns')
        # number of switches to check
        switch_num = len(chassis_params_fabric_lst)     
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping       
        portshow_lst = []  
        # data imported from init file to extract values from config file
        portcmd_params, params_add, comp_keys, match_keys, comp_dct = data_extract_objects('portcmd', max_title)  
        
        # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
        # checking each chassis for switch level parameters
        for i, chassis_params_data in enumerate(chassis_params_fabric_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current chassis
            chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
            sshow_file = chassis_params_data_dct['configname']
            chassis_name = chassis_params_data_dct['chassis_name']
            chassis_wwn = chassis_params_data_dct['chassis_wwn']            
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_data_dct["chassis_name"]} switch portshow, portloginshow and statsshow'
            print(info, end =" ")
            
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
                        while not re.search(r'^\| ... rebuilt finished\|$',line):
                            line = file.readline()
                            if not line:
                                break
                            # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                            match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                            # portFcPortCmdShow section start
                            if match_dct[match_keys[0]]:
                                # dictionary to store all DISCOVERED parameters
                                # collecting data only for the chassis in current loop
                                portcmd_dct = {}
                                # connected devices wwns in portshow section
                                connected_wwn_lst = []
                                # list to store connected devices port_id and wwn pairs in portlogin section
                                portid_wwn_lst = []
                                port_index = None
                                slot_port_lst = line_to_list(comp_dct[comp_keys[0]], line)
                                while not re.search(r'^portshow +(\d{1,4})$',line):
                                    line = file.readline()
                                    if not line:
                                        break
                                match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                # portshow section start
                                if match_dct[match_keys[1]]:
                                    port_index = match_dct[match_keys[1]].group(1)
                                    while not re.search(fr'^portloginshow +{int(port_index)}$', line):
                                        line = file.readline()
                                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                        # portshow_params_match
                                        if match_dct[match_keys[2]]:
                                            portshow_attr, = comp_dct[comp_keys[2]].findall(line)
                                            # portstate parameter has special processing rule
                                            if portshow_attr[0] == 'portState':
                                                portcmd_dct['portState'] = portshow_attr[2]
                                            # portshow_attr can contain up to 3 parameter name and value pairs
                                            # parameters name on even positions in the portshow_attr and values on odd 
                                            else:
                                                for k, v in zip(portshow_attr[::2], portshow_attr[1::2]):
                                                    portcmd_dct[k] = v
                                        # portscn_match has two parameter name and value pairs
                                        if match_dct[match_keys[6]]:
                                            portscn_line = line_to_list(comp_dct[comp_keys[6]], line)
                                            for k, v in zip(portscn_line[::2], portscn_line[1::2]):
                                                portcmd_dct[k] = v
                                        # portdistance_match
                                        if match_dct[match_keys[7]]:
                                            portdistance_line = line_to_list(comp_dct[comp_keys[7]], line)
                                            portcmd_dct[portdistance_line[0]] = portdistance_line[1]
                                        # device_connected_wwn_match
                                        if match_dct[match_keys[8]]:
                                            connected_wwn = line_to_list(comp_dct[comp_keys[8]], line)
                                            connected_wwn_lst.append((portcmd_dct.get('portId'), connected_wwn))
                                        if not line:
                                            break
                                # portshow section end
                                # portlogin section start                                      
                                if re.match(fr'^portloginshow +{int(port_index)}$', line):

                                    while not re.search(fr'^portregshow +{int(port_index)}$', line):
                                        line = file.readline()
                                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                        # connected_wwn_match
                                        if match_dct[match_keys[3]]:
                                            # first value in tuple unpacking is fe or fd and not required
                                            _, port_id, wwn = line_to_list(comp_dct[comp_keys[3]], line)
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
                                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                        # port information without virtual channel numbers
                                        if match_dct[match_keys[4]]:
                                            portcmd_dct[match_dct[match_keys[4]].group(1).rstrip()] = match_dct[match_keys[4]].group(2)
                                        # port information with virtual channel numbers
                                        elif match_dct[match_keys[5]]:
                                            line_values = line_to_list(comp_dct[comp_keys[5]], line)
                                            param_name, start_vc = line_values[0:2]
                                            for i, value in enumerate(line_values[3:]):
                                                param_name_vc = param_name + '_' + str(int(start_vc) + i)
                                                portcmd_dct[param_name_vc] = value
                                        if not line:
                                            break
                                # portstatsshow section end
                                # portFcPortCmdShow section end        

                                # additional values which need to be added to the dictionary with all DISCOVERED parameters during current loop iteration
                                # chassis_slot_port_values order (configname, chassis_name, port_index, slot_num, port_num, port_ids and wwns of connected devices)
                                # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column
                                for port_id, connected_wwn in portid_wwn_lst:
                                    chassis_slot_port_values = [sshow_file, chassis_name, chassis_wwn, port_index, *slot_port_lst, port_id, connected_wwn]
                                    # print('chassis_slot_port_values', chassis_slot_port_values)
                                    # adding or changing data from chassis_slot_port_values to the DISCOVERED dictionary
                                    update_dct(params_add, chassis_slot_port_values, portcmd_dct)
                                    # adding data to the REQUIRED list for each device connected to the port 
                                    portshow_lst.append([portcmd_dct.get(portcmd_param, None) for portcmd_param in portcmd_params])
                                    # print('portshow_lst', portshow_lst)

                    # sshow_port section end                            
            status_info('ok', max_title, len(info))
        # save extracted data to json file    
        save_data(report_data_lst, data_names, portshow_lst)
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        portshow_lst, = verify_data(report_data_lst, data_names, *data_lst)
        
    return portshow_lst

