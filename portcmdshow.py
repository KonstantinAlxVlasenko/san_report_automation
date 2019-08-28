import re
import pandas as pd
from files_operations import columns_import, status_info, data_extract_objects, data_to_json, json_to_data, line_to_list

"""Module to extract portFcPortCmdShow information"""


def portcmdshow_extract(chassis_params_fabric_lst, report_data_lst):
    """Function to extract portshow, portloginshow, portstatsshow information
    """
    # report_data_lst = [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 8. PORTCMD PARAMETERS ...\n')
    
    *_, max_title = report_data_lst
    # check if data already have been extracted
    data_names = ['portcmd']
    data_lst = json_to_data(report_data_lst, *data_names)
    portshow_lst, = data_lst
    
    # if no data saved than extract data from configurtion files  
    if not all(data_lst):             
        print('\nEXTRACTING PORTSHOW, PORTLOGINSHOW, PORTSTATSSHOW INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # extract chassis parameters names from init file
        chassis_columns = columns_import('chassis', max_title, 'columns')
        # number of switches to check
        switch_num = len(chassis_params_fabric_lst)     
        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping       
        portshow_lst = []  
        # data imported from init file to extract values from config file
        portcmd_params, portcmd_params_add, comp_keys, match_keys, comp_dct = data_extract_objects('portcmd', max_title)  
        
        # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
        # checking each chassis for switch level parameters
        for i, chassis_params_data in enumerate(chassis_params_fabric_lst):       
            # data unpacking from iter param
            # dictionary with parameters for the current chassis
            chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
            sshow_file = chassis_params_data_dct['configname']
            chassis_name = chassis_params_data_dct['chassis_name']           
            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_params_data_dct["chassis_name"]} switch portshow, portloginshow and statsshow check.'
            print(info, end =" ")
            
            # search control dictionary. continue to check sshow_file until all parameters groups are found
            collected = {'portshow': False}
            
            with open(sshow_file, encoding='utf-8', errors='ignore') as file:
                # check file until all groups of parameters extracted
                while not all(collected.values()):
                    line = file.readline()
                    if not line:
                        break
                    # configshow section start
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
                                # sets to store port_ids and wwns of port connected devices to avoid duplicates
                                connected_wwns = set()
                                port_ids = set()
                                port_index = None
                                # portshow_attr = []
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
                                        if match_dct[match_keys[2]]:
                                            portshow_attr, = comp_dct[comp_keys[2]].findall(line)
                                            for k, v in zip(portshow_attr[::2], portshow_attr[1::2]):
                                                portcmd_dct[k] = v
                                        if not line:
                                            break
                                # portshow section end
                                # portlogin section start                                      
                                if re.match(fr'^portloginshow +{int(port_index)}$', line):
                                    while not re.search(fr'^portregshow +{int(port_index)}$', line):
                                        line = file.readline()
                                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                                        if match_dct[match_keys[3]]:
                                            port_id, wwn = line_to_list(comp_dct[comp_keys[3]], line)
                                            port_ids.add(port_id)
                                            connected_wwns.add(wwn)
                                        if not line:
                                            break
                                # portlogin section end
                                while not re.match(fr'^portstatsshow +{int(port_index)}$', line):
                                    line = file.readline()
                                    if not line:
                                        break
                                # portstatsshow section start
                                if re.match(fr'^portstatsshow +{int(port_index)}$', line):
                                    while not re.search(fr'^portstats64show +{int(port_index)}$', line):
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
                                chassis_slot_port_values = [sshow_file, chassis_name, port_index, *slot_port_lst, port_ids, connected_wwns]
                                        
                                # adding additional parameters and values to the chassis_params_switch_dct
                                for portcmd_param_add, chassis_slot_port_value in zip(portcmd_params_add,  chassis_slot_port_values):
                                    if chassis_slot_port_value:                
                                        if not isinstance(chassis_slot_port_value, str):
                                            s = ', '
                                            chassis_slot_port_value = f'{s}'.join(chassis_slot_port_value)
                                        portcmd_dct[portcmd_param_add] = chassis_slot_port_value               
                                # appending list with only REQUIRED port info for the current loop iteration to the list with all fabrics port info
                                portshow_lst.append([portcmd_dct.get(portcmd_param, None) for portcmd_param in portcmd_params])
                                                
            status_info('ok', max_title, len(info))
        # save extracted data to json file    
        data_to_json(report_data_lst, data_names, portshow_lst)
        
    return portshow_lst


