"""Module with auxiliary functions to parse certain section of blade configuration file"""

import re
import utilities.data_structure_operations as dsop
import utilities.regular_expression_operations as reop


def blade_enclosure_section(pattern_dct, file, enclosure_params):
    """Function to extract enslosure information for oa>show all"""

    enclosure_dct = {}
    line = file.readline()
    # while not reach empty line
    while not re.search(r'UUID',line):
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # name_value_pair_match
        if match_dct['name_value_pair']:
            result = match_dct['name_value_pair']
            enclosure_dct[result.group(1).strip()] = result.group(2).strip()
        line = file.readline()
        if not line:
            break
    # creating list with REQUIRED enclosure information only
    enclosure_lst = [enclosure_dct.get(param) for param in enclosure_params]
    return enclosure_lst


def vc_enclosure_section(pattern_dct, file, enclosure_params):
    """Function to extract enclosure information for vc>show all *"""

    line = file.readline()
    enclosure_total_dct = {}
    while not re.search(pattern_dct['vc_information_header'], line):
        line = file.readline()
        if re.match(pattern_dct['vc_enclosure_id'], line):
            enclosure_current_dct = {}
            line = reop.extract_key_value_from_line(enclosure_current_dct, pattern_dct, line, file, 
                                                    extract_pattern_name='name_value_pair', stop_pattern_name='part_number_line', 
                                                    first_line_skip=False)
            # rename Description key to Enclosure Type key for VC
            if enclosure_current_dct.get('Description'):
                enclosure_current_dct['Enclosure Type'] = enclosure_current_dct.pop('Description')
            # creating list with REQUIRED enclosure information only
            enclosure_current_lst = [enclosure_current_dct.get(param) for param in enclosure_params]
            enslosure_id = enclosure_current_dct['ID']
            enclosure_total_dct[enslosure_id] = enclosure_current_lst
    return enclosure_total_dct


def vc_fabric_connection_section(san_blade_vc_lst, enclosure_vc_lst, 
                                    enclosure_total_dct, pattern_dct, file):
    """Function to extract vc fabric information from  vc>show all *"""
    
    line = file.readline()
    while not re.search(r'FC-CONNECTION INFORMATION', line):
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # vc_port_match
        if match_dct['vc_port']:
            vc_port = dsop.line_to_list(pattern_dct['vc_port'], line)
            enslosure_id = vc_port[0]
            enclosure_lst = enclosure_total_dct[enslosure_id]
            vc_port = [*enclosure_lst, *vc_port]

            enclosure_vc_lst.append(vc_port)
            san_blade_vc_lst.append(vc_port)
            line = file.readline()
        else:
            line = file.readline()
            if not line:
                break


def active_oa_section(file, pattern_dct):
    """Function to extract active OA IP address from oa>show all"""

    line = file.readline()
    while not re.search(r'^>SHOW', line):
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # oa_ip_match
        if match_dct['oa_ip']:
            oa_ip = match_dct['oa_ip'].group(1)
            line = file.readline()
            return oa_ip
        else:
            line = file.readline()
            if not line:
                break

def interconnect_module_section(san_module_lst, pattern_dct,
                                file, enclosure_lst, oa_ip, module_num, module_params):
    """Function to extract interconnect modules from oa>show all"""

    line = file.readline()
    while not re.search(r'^>SHOW', line):
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # module_type_num_match
        if match_dct['module_type_num']:
            module_dct = {}
            module_lst= []
            module = match_dct['module_type_num']
            # interconnect module slot number
            module_slot = module.group(1)
            # interconnect module type (Ethernet, FC)
            module_type = module.group(2).rstrip()
            
            line = file.readline()
            line = reop.extract_key_value_from_line(module_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='name_value_pair', stop_pattern_name='module_section_end', 
                                                        first_line_skip=False) 
            # creating list with REQUIRED interconnect module information only
            module_lst = [module_dct.get(param) for param in module_params]
            # add current module information to list containing all modules infromation
            # oa_ip added as None and extracted later in the file
            san_module_lst.append([*enclosure_lst, oa_ip, module_slot, module_type, *module_lst])
            # based on module's number oa_ip is added to module_comprehensive_lst after extraction
            module_num += 1
        else:
            line = file.readline()
            if not line:
                break
    return module_num


def server_hba_flb_section(san_blade_lst, blade_lst, pattern_dct,
                            file, enclosure_lst, blade_params):
    """Function to extract blade server, hba and flb from oa>show all"""
                
    line = file.readline()
    while not re.search(r'^>SHOW', line):
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # blade_server_num_match
        if match_dct['blade_server_num']:
            blade_dct = {}
            blade_lst = []
            hba_lst = []
            result = match_dct['blade_server_num']
            blade_dct[result.group(1)] = result.group(2)
            line = file.readline()
            # server_section_end_comp
            while not re.search(pattern_dct['server_section_end'], line):
                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # mezzanin hba section start
                # mezzanine_model_match
                if match_dct['mezzanine_description']:
                    result = match_dct['mezzanine_description']
                    hba_description = result.group(1)
                    hba_model = result.group(2)
                    line = file.readline()
                    # mezzanine_wwn_comp
                    while re.search(pattern_dct['mezzanine_wwn'], line):
                        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # mezzanine_wwn_match
                        result = match_dct['mezzanine_wwn']
                        wwnp = result.group(1)
                        hba_lst.append([hba_description, hba_model, wwnp])
                        line = file.readline()
                # mezzanin hba section end
                # flex flb hba section start
                # flb_model_match and flex_ethernet_match
                elif match_dct['flb_description'] or match_dct['flex_ethernet']:
                    if match_dct['flb_description']:
                        result = match_dct['flb_description']
                        flex_description = result.group(1)
                        if re.search(pattern_dct['flb_model'], line):
                            flex_model = re.search(pattern_dct['flb_model'], line).group(1)
                        else:
                            flex_model = None
                    elif match_dct['flex_ethernet']:
                        result = match_dct['flex_ethernet']
                        flex_description = result.group(1)
                        flex_model = result.group(1)
                    line = file.readline()
                    # wwn_mac_line_comp
                    while re.search(pattern_dct['wwn_mac_line'], line):
                        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                        # flb_wwn_match
                        if match_dct['flb_wwn']:
                            result = match_dct['flb_wwn']
                            wwnp = result.group(1)
                            hba_lst.append([flex_description, flex_model, wwnp])
                        line = file.readline()
                # flex flb hba section end
                # blade server section start
                # blade_server_info_match
                elif match_dct['blade_server_info']:
                    result = match_dct['blade_server_info']
                    # name = result.group(1) + result.group(2) if result.group(2) else result.group(1)
                    name = result.group(1).rstrip()
                    value = result.group(3).rstrip()
                    # to avoid Type parameter overwrire
                    # add parameter only if parameter has not been added to blade dictionary before
                    if not blade_dct.get(name):
                        blade_dct[name] = value
                    line = file.readline()
                # blade server section end
                # if none of matches found for current blade server than next line
                else:
                    line = file.readline()
                    if not line:
                        break
            # unpopulated blade slots have 'Server Blade Type' line but populated have 'Type' line
            # add 'Server Blade Type' parameter for populated slots for consistency
            if blade_dct.get('Type'):
                blade_dct['Server Blade Type'] = blade_dct.pop('Type')
            # creating list with REQUIRED blade server information only
            blade_lst = [blade_dct.get(param) for param in blade_params]
            # if hba or flex cards installed in blade server
            if len(hba_lst):
                # add line for each hba to blades_comprehensive_lst
                for hba in hba_lst:
                    san_blade_lst.append([*enclosure_lst, *blade_lst, *hba])
            # if no nba add one line with enclosure and blade info only
            else:
                san_blade_lst.append([*enclosure_lst, *blade_lst, None, None])
        # if no blade_server_num_match found in >SHOW SERVER INFO ALL section than next line
        else:
            line = file.readline()
            if not line:
                break
    return blade_lst