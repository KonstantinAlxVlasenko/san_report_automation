"""Module with functions to extract fabricshow and agshow information for the switch context from sshow file"""

import re

import utilities.data_structure_operations as dsop


def fabricshow_section_extract(fabricshow_lst, pattern_dct, 
                                principal_switch_lst, 
                                line, file):
    """Function to extract fabricshow information from sshow file"""

    while not re.search(pattern_dct['switchcmd_end'],line):
        line = file.readline()
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # 'fabricshow_match' pattern #0
        if match_dct['fabricshow']:
            fabricshow_lst.append(dsop.line_to_list(pattern_dct['fabricshow'], line, *principal_switch_lst))                                      
        if not line:
            break
    return line


def agshow_section_extract(ag_principal_lst, pattern_dct, 
                            principal_switch_lst, ag_params,  
                            line, file):
    """Function to extract agshow information from principal switch sshow file"""

    while not re.search(pattern_dct['switchcmd_end'], line):
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # ag_num_match pattern #5
        if match_dct['ag_num']:
            # dictionary to store all DISCOVERED switch ports information
            # collecting data only for the logical switch in current loop
            # Access Gateway common information dictionary
            ag_info_dct = {}
            # Attached F-Port information dictionary
            ag_attach_dct = {}
            # Access Gateway F-Port information dictionary
            ag_fport_dct = {}
            # Domaid ID, port_ID, port_index dictionary 
            did_port_dct = {}
            
            line = file.readline()                                
            # ag_switchcmd_end_comp
            while not re.search(pattern_dct['ag_switchcmd_end'], line):
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
                # ag_info_match pattern #6
                if match_dct['ag_info']:
                    ag_info_dct[match_dct['ag_info'].group(1).rstrip()] = match_dct['ag_info'].group(2).rstrip()
                # ag_attached_match pattern #7
                elif match_dct['ag_attached']:
                    # if Attached F-Port information dictionary is empty than create dictionary with N-Port ID(s) as keys and empty lists as values
                    # if ag_attach_dct has been already created (not empty) then it's preserved
                    ag_attach_dct = ag_attach_dct or dict((n_portid, []) for n_portid in ag_info_dct['N-Port ID(s)'].split(','))
                    # extracting attached F-port data from line to list
                    ag_attach_lst = dsop.line_to_list(pattern_dct['ag_attached'], line)
                    # getting port_ID of N-port from port_id of F-port
                    n_portid = ag_attach_lst[0][:-2] + '00'
                    # adding current line F-port information to Attached F-Port information dictionary 
                    if n_portid in ag_attach_dct.keys():
                        ag_attach_dct[n_portid].append(ag_attach_lst)
                # ag_fport_match pattern #8
                elif match_dct['ag_fport']:
                    # create Access Gateway F-Port information dictionary
                    ag_fport_dct = ag_fport_dct or dict((n_portid, []) for n_portid in ag_info_dct['N-Port ID(s)'].split(','))
                    # extracting access gateway F-port data from line to list
                    ag_fport_lst = dsop.line_to_list(pattern_dct['ag_fport'], line)
                    # getting port_ID of N-port from port_id of F-port
                    n_portid = ag_fport_lst[1][:-2] + '00'
                    # adding current line F-port information to Access Gateway F-Port information dictionary
                    if n_portid in ag_fport_dct.keys():                                                
                        ag_fport_dct[n_portid].append(ag_fport_lst)
                line = file.readline()                                                                                 
                if not line:
                    break

            # list of N-ports extracted from N-Port ID(s) line
            n_portids_lst = ag_info_dct['N-Port ID(s)'].split(',')
            # (domain_id, n_portid)
            did_port_lst = [(int(n_portid[:4], 0), n_portid) for n_portid in n_portids_lst]
            # creating dictionary with n_portid as keys and (domain_id, n_portid) as values
            did_port_dct = {port[1]:list(port) for port in did_port_lst}

            # change values representation in dictionaries
            # before {n_portid: [(port_id_1, port_wwn_1, f-port_num_1)], [(port_id_2, port_wwn_2, f-port_num_2)]}
            # after {n_portid: [(port_id_1, port_id_2), (port_wwn_1, port_wwn_2), (f-port_num_1, f-port_num_1)]                                      
            ag_attach_dct = {n_portid:list(zip(*ag_attach_dct[n_portid])) 
                                for n_portid in n_portids_lst if ag_attach_dct.get(n_portid)}
            ag_fport_dct = {n_portid:list(zip(*ag_fport_dct[n_portid])) 
                            for n_portid in n_portids_lst if ag_fport_dct.get(n_portid)}
                
            # add connected switch port_index to did_port_dct extracted from ag_attach_dct
            # (domain_id, n_portid, n_port_index)
            # if no port_index then add None 
            for n_portid in n_portids_lst:
                if ag_attach_dct.get(n_portid):
                    did_port_dct[n_portid].append(ag_attach_dct[n_portid][2][0])
                else:
                    did_port_dct[n_portid].append(None)
            
            # for each element of list convert tuples to strings
            # if no data extracted for the n_portid then add None for each parameter
            for n_portid in n_portids_lst:
                if ag_attach_dct.get(n_portid):
                    ag_attach_dct[n_portid] = [', '.join(v) for v in ag_attach_dct[n_portid]]
                else:
                    ag_attach_dct[n_portid] = [None]*3                                            
            for n_portid in n_portids_lst:
                if ag_fport_dct.get(n_portid):
                    ag_fport_dct[n_portid] = [', '.join(v) for v in ag_fport_dct[n_portid]]
                else:
                    ag_fport_dct[n_portid] = [None]*3

            # getting data from ag_info_dct in required order
            ag_info_lst = [ag_info_dct.get(param, None) for param in ag_params]               
            # appending list with only REQUIRED ag info for the current loop iteration to the list with all ag switch info
            for n_portid in n_portids_lst:
                ag_principal_lst.append([*principal_switch_lst[:-1], *ag_info_lst, *did_port_dct[n_portid],
                                    *ag_attach_dct[n_portid], *ag_fport_dct[n_portid]])
        else:
            line = file.readline()
        if not line:
            break
    return line