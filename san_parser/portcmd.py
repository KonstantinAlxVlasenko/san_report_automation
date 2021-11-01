"""Module to extract portFcPortCmdShow information"""


import re


import utilities.dataframe_operations as dfop
import utilities.database_operations as dbop
import utilities.data_structure_operations as dsop
import utilities.module_execution as meop
import utilities.servicefile_operations as sfop
import utilities.filesystem_operations as fsop

# import pandas as pd
# import dataframe_operations as dfop
# from common_operations_filesystem import load_data, save_data
# from common_operations_miscellaneous import (
#     force_extract_check, line_to_list, status_info, update_dct, verify_data)
# from common_operations_servicefile import columns_import, data_extract_objects
# from common_operations_dataframe import list_to_dataframe
# from common_operations_table_report import dataframe_to_report
# from common_operations_miscellaneous import verify_force_run
# from common_operations_database import read_db, write_db


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

    # load data if they were saved on previos program execution iteration
    # data_lst = load_data(report_constant_lst, *data_names)
    data_lst = dbop.read_database(report_constant_lst, report_steps_dct, *data_names)

    # when any data from data_lst was not saved (file not found) or
    # force extract flag is on then re-extract data from configuration files
    force_run = meop.verify_force_run(data_names, data_lst, report_steps_dct, max_title)
    
    if force_run:             
        print('\nEXTRACTING PORTSHOW, PORTLOGINSHOW, PORTSTATSSHOW INFORMATION FROM SUPPORTSHOW CONFIGURATION FILES ...\n')
        
        # # extract chassis parameters names from init file
        # chassis_columns = sfop.columns_import('chassis', max_title, 'columns')
        # number of switches to check
        
        # switch_num = len(chassis_params_fabric_lst)
        switch_num = len(chassis_params_df.index)

        # list to store only REQUIRED switch parameters
        # collecting data for all switches during looping       
        portshow_lst = []  
        # data imported from init file to extract values from config file
        portcmd_params, params_add, comp_keys, match_keys, comp_dct = sfop.data_extract_objects('portcmd', max_title)  
        
        # chassis_params_fabric_lst [[chassis_params_sw1], [chassis_params_sw1]]
        # checking each chassis for switch level parameters
        
        # for i, chassis_params_data in enumerate(chassis_params_fabric_lst):
        for i, chassis_params_sr in chassis_params_df.iterrows():           
            # data unpacking from iter param
            # dictionary with parameters for the current chassis
            
            # chassis_params_data_dct = dict(zip(chassis_columns, chassis_params_data))
            # sshow_file = chassis_params_data_dct['configname']
            # chassis_name = chassis_params_data_dct['chassis_name']
            # chassis_wwn = chassis_params_data_dct['chassis_wwn']

            # sshow_file = chassis_params_sr['configname']
            # chassis_name = chassis_params_sr['chassis_name']
            # chassis_wwn = chassis_params_sr['chassis_wwn']

            chassis_info_keys = ['configname', 'chassis_name', 'chassis_wwn']
            chassis_info_lst = [chassis_params_sr[key] for key in chassis_info_keys]

            sshow_file, chassis_name, _ = chassis_info_lst            

            # current operation information string
            info = f'[{i+1} of {switch_num}]: {chassis_name} switch portshow, portloginshow and statsshow'
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
                        while not re.search(r'^\| ... rebuilt finished *\|$',line):
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
                                slot_port_lst = dsop.line_to_list(comp_dct[comp_keys[0]], line)
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
                                            portscn_line = dsop.line_to_list(comp_dct[comp_keys[6]], line)
                                            for k, v in zip(portscn_line[::2], portscn_line[1::2]):
                                                portcmd_dct[k] = v
                                        # portdistance_match
                                        if match_dct[match_keys[7]]:
                                            portdistance_line = dsop.line_to_list(comp_dct[comp_keys[7]], line)
                                            portcmd_dct[portdistance_line[0]] = portdistance_line[1]
                                        # device_connected_wwn_match
                                        if match_dct[match_keys[8]]:
                                            connected_wwn = dsop.line_to_list(comp_dct[comp_keys[8]], line)
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
                                            _, port_id, wwn = dsop.line_to_list(comp_dct[comp_keys[3]], line)
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
                                            line_values = dsop.line_to_list(comp_dct[comp_keys[5]], line)
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
                                    
                                    # chassis_slot_port_values = [sshow_file, chassis_name, chassis_wwn, port_index, *slot_port_lst, port_id, connected_wwn]
                                    
                                    chassis_slot_port_values = [*chassis_info_lst, port_index, *slot_port_lst, port_id, connected_wwn]
                                    # adding or changing data from chassis_slot_port_values to the DISCOVERED dictionary
                                    dsop.update_dct(params_add, chassis_slot_port_values, portcmd_dct)
                                    # adding data to the REQUIRED list for each device connected to the port 
                                    portshow_lst.append([portcmd_dct.get(portcmd_param, None) for portcmd_param in portcmd_params])

                    # sshow_port section end                            
            meop.status_info('ok', max_title, len(info))
        # convert list to DataFrame
        portshow_df = dfop.list_to_dataframe(portshow_lst, max_title, sheet_title_import='portcmd')
        # saving data to csv file
        data_lst = [portshow_df]
        # save_data(report_constant_lst, data_names, *data_lst)
        # write data to sql db
        dbop.write_database(report_constant_lst, report_steps_dct, data_names, *data_lst)      
    # verify if loaded data is empty after first iteration and replace information string with empty list
    else:
        # portshow_df = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        # data_lst = [portshow_df]

        data_lst = dbop.verify_read_data(report_constant_lst, data_names, *data_lst)
        portshow_df, *_ = data_lst

    # save data to excel file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        dfop.dataframe_to_excel(data_frame, data_name, report_creation_info_lst)
        
    return portshow_df

