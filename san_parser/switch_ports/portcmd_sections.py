
import re

import utilities.data_structure_operations as dsop


def port_fc_portcmd_section(portshow_lst, pattern_dct, 
                            chassis_info_lst, portcmd_params, portcmd_params_add,
                            line, file):

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
    if match_dct['portshow_port_index']:
        port_index = match_dct['portshow_port_index'].group(1)
        line = portshow_section(portcmd_dct, connected_wwn_lst, portphys_portscn_details, port_index, pattern_dct, line, file)
    # portshow section end
    # portlogin section start                                      
    if re.match(fr'^portloginshow +{int(port_index)}$', line):
        line = portlogin_section(portid_wwn_lst, portcmd_dct, connected_wwn_lst, port_index, 
                            pattern_dct, line, file)
    # portlogin section end
    while not re.match(fr'^portstatsshow +{int(port_index)}$', line):
        line = file.readline()
        if not line:
            break
    # portstatsshow section start
    if re.match(fr'^portstatsshow +{int(port_index)}$', line):
        line = portstats_section(portcmd_dct, port_index, pattern_dct, line, file)
    # portstatsshow section end       

    portphys_portscn_details = [value if value else None for value in portphys_portscn_details]
    # additional values which need to be added to the dictionary with all DISCOVERED parameters during current loop iteration
    # chassis_slot_port_values order (configname, chassis_name, port_index, slot_num, port_num, port_ids and wwns of connected devices)
    # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column
    for port_id, connected_wwn in portid_wwn_lst:                            
        chassis_slot_port_values = [*chassis_info_lst, port_index, *slot_port_lst, port_id, connected_wwn, *portphys_portscn_details]
        # adding or changing data from chassis_slot_port_values to the DISCOVERED dictionary
        dsop.update_dct(portcmd_params_add, chassis_slot_port_values, portcmd_dct)
        # adding data to the REQUIRED list for each device connected to the port 
        portshow_lst.append([portcmd_dct.get(portcmd_param) for portcmd_param in portcmd_params])
    return line


def portshow_section(portcmd_dct, connected_wwn_lst, portphys_portscn_details, port_index, 
                        pattern_dct, line, file):
    """Function to extract portshow information for the current port_index"""

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
            matched_values_lst = dsop.line_to_list(pattern_dct['portshow_err_stats'], line)
            for k, v in zip(matched_values_lst[::2], matched_values_lst[1::2]):
                if k:
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
    return line


def portlogin_section(portid_wwn_lst, portcmd_dct, connected_wwn_lst, port_index, 
                        pattern_dct, line, file):
    """Function to extract portloginshow information for the current port_index"""

    while not re.search(fr'^portregshow +{int(port_index)}$', line):
        line = file.readline()
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # connected_wwn_match pattern #3
        if match_dct['login_connected_wwn']:
            # first value in tuple unpacking is fe or fd and not required
            _, port_id, wwn = dsop.line_to_list(pattern_dct['login_connected_wwn'], line)
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
    return line


def portstats_section(portcmd_dct, port_index, pattern_dct, line, file):
    """Function to extract portstats information for the current port_index"""

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
    return line
