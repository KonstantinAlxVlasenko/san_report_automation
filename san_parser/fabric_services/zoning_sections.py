"""Module with functions to extract regular and peer zoning information for the switch context from sshow file"""

import re

import utilities.data_structure_operations as dsop


def regular_zoning_section_extract(cfg_lst, zone_lst, alias_lst, cfg_effective_lst, zone_effective_lst, pattern_dct,
                                    principal_switch_lst, 
                                    line, file):
    """Function to extract regular zoning information from principal switch config file"""

    # control flag to check if Effective configuration line passed
    effective = False
    # set to collect Defined configuration names 
    defined_configs_set = set()  
    # switchcmd_end_comp
    while not re.search(pattern_dct['switchcmd_end'], line):                               
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # if Effective configuration line passed
        if match_dct['effective']:
            effective = True                                     
        # 'cfg_match'
        if match_dct['cfg']:
            cfg_line = dsop.line_to_list(pattern_dct['cfg'], line)
            # zoning config name
            cfg_name = cfg_line[0]
            # add config name to the set
            defined_configs_set.add(cfg_name)
            # if except config name line contains zone names
            if cfg_line[1]:
                members_lst = cfg_line[1].strip().replace(';', '').split()
                for member in members_lst:
                    cfg_lst.append([*principal_switch_lst, cfg_name, member])
            # if Effective configuration checked then 
            # add Effective and Defined configuration names to the table 
            if effective:
                cfg_effective_lst.append([*principal_switch_lst, cfg_name, ', '.join(defined_configs_set)])
            # switch to the next line to enter the loop
            line = file.readline()
            # zoning_switchcmd_end_comp separates different configs, zones and aliases 
            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                # find all zone names in line
                members_lst = re.findall(pattern_dct['zones'], line)
                for member in members_lst:
                    cfg_lst.append([*principal_switch_lst, cfg_name, member.rstrip(';')])
                line = file.readline()
                if not line:
                    break
        # 'zone_match'
        elif match_dct['zone']:
            zone_line = dsop.line_to_list(pattern_dct['zone'], line)
            zone_name = zone_line[0]
            # if line contains zone name and zone member
            if zone_line[1]:
                member_lst = zone_line[1].strip().replace(';', '').split()
                for member in member_lst:
                    # for Defined configuration add zones to zone_lst
                    if not effective: 
                        zone_lst.append([*principal_switch_lst, zone_name, member])
                    # for Effective configuration add zones to zone_effective_lst
                    elif effective:
                        zone_effective_lst.append([*principal_switch_lst, zone_name, member])
            line = file.readline()
            # zoning_switchcmd_end_comp separates different configs, zones and aliases
            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                members_lst = re.findall(pattern_dct['zones'], line)
                for member in members_lst:
                    # for Defined configuration add zones to zone_lst
                    if not effective:
                        zone_lst.append([*principal_switch_lst, zone_name, member.rstrip(';')])
                    # for Effective configuration add zones to zone_effective_lst
                    elif effective:
                        zone_effective_lst.append([*principal_switch_lst, zone_name, member.rstrip(';')])
                line = file.readline()
                if not line:
                    break
        # 'alias_match'
        elif match_dct['alias']:
            alias_line = dsop.line_to_list(pattern_dct['alias'], line)
            alias_name = alias_line[0]
            # if line contains alias name and alias member
            if alias_line[1]:
                member_lst = alias_line[1].strip().replace(';', '').split()
                for member in member_lst:
                    alias_lst.append([*principal_switch_lst, alias_name, member])
            line = file.readline()
            # zoning_switchcmd_end_comp separates different configs, zones and aliases
            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                member_lst = re.findall(pattern_dct['zones'], line)
                for member in member_lst:
                    alias_lst.append([*principal_switch_lst, alias_name, member.rstrip(';')])
                line = file.readline()
                if not line:
                    break
        # if line doesn't coreesponds to any reg expression pattern then next line
        # until cfgshow command border reached
        else:
            line = file.readline()                                           
        if not line:
            break
    return line


def peer_zoning_section_extract(peerzone_lst, peerzone_effective_lst, pattern_dct,
                                principal_switch_lst, 
                                line, file):
    """Function to extract peer zoning information from principal switch config file"""

    # control flag to check if Effective configuration line passed
    peerzone_effective = False
    # switchcmd_end_comp
    while not re.search(pattern_dct['switchcmd_end'], line):
        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}                              
        # if Effective configuration line passed
        if match_dct['effective']:
            peerzone_effective = True                                     
        # 'zone_match'
        if match_dct['zone']:
            zone_line = dsop.line_to_list(pattern_dct['zone'], line)
            zone_name = zone_line[0]
            line = file.readline()
            # zoning_switchcmd_end_comp separates different zones
            while not re.search(pattern_dct['zoning_switchcmd_end'], line):
                # dictionary with match names as keys and match result of current line with all imported regular expressions as values
                match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}  
                # peerzone_property_match
                if match_dct['peerzone_property']:
                    # peerzone_property is tuple. contains property member ot created by info
                    peerzone_property = match_dct['peerzone_property'].groups()
                    zonemember = [*principal_switch_lst, zone_name, *peerzone_property]
                    # for Effective configuration add member to peerzone_effective_lst
                    if peerzone_effective:
                        peerzone_effective_lst.append(zonemember)
                    # for Defined configuration add member to peerzone_lst
                    else:
                        peerzone_lst.append(zonemember)
                    line = file.readline()
                # peerzone_member_type_match (principal or peer)
                elif match_dct['peerzone_member_type']:
                    member_type = match_dct['peerzone_member_type'].group(1)
                    line = file.readline()
                    # peerzone_member_end_comp separates peer and principals groups
                    while not re.search(pattern_dct['peerzone_member_end'], line):
                        # find zonemembers
                        members_lst = re.findall(pattern_dct['zones'], line)
                        for member in members_lst:
                            # for Defined configuration add zones to zone_lst
                            zonemember = [*principal_switch_lst, zone_name, member_type, member.rstrip(';')]
                            # for Effective configuration add member to peerzone_effective_lst
                            if peerzone_effective:
                                peerzone_effective_lst.append(zonemember)
                            # for Defined configuration add member to peerzone_lst
                            else:
                                peerzone_lst.append(zonemember)
                        line = file.readline()
                        if not line:
                            break
                # if line doesn't coreespond to any reg expression pattern then switch line
                # until zone description border reached
                else:
                    line = file.readline()
                    if not line:
                        break
        # next line until zoneshow --peer command border reached
        else:
            line = file.readline()                                           
        if not line:
            break
    return line
