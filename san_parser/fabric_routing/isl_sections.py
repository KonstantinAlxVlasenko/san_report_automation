"""Module with functions to extract islshow, trunkshow, porttrunkarea and lsdbshow information for the switch context from sshow file"""

import re

import utilities.data_structure_operations as dsop
import utilities.regular_expression_operations as reop


def lsdbshow_section_extract(lsdb_lst, pattern_dct, switch_info_lst, lsdb_params, line, file):
    """Function to extract lsdbshow (link cost) information for the current context from the config file"""

    while not re.search(pattern_dct['switchcmd_end'],line):  

        # dictionary with match names as keys and match result of current line with all imported regular expressions as values
        match_dct = {pattern_name: pattern_dct[pattern_name].match(line) for pattern_name in pattern_dct.keys()}
        # lsdb_domain section start
        if match_dct['lsdb_domain']:
            # dictionary to store all DISCOVERED parameters
            lsdb_param_dct = {}
            # Domain ID described by this LSR. 
            # A (self) keyword after the domain ID indicates that LSR describes the local switch.
            domain_self_tag_lst = dsop.line_to_list(pattern_dct['lsdb_domain'], line)
            line = reop.extract_key_value_from_line(lsdb_param_dct, pattern_dct, line, file, 
                                                    extract_pattern_name='lsdb_param', stop_pattern_name='lsdb_link')
            # list with required params only in order
            lsdb_param_lst = [lsdb_param_dct.get(param_name) for param_name in lsdb_params]
        # lsdb_domain section end
        elif match_dct['lsdb_link']:
            # extract link information
            lsdb_link_lst = dsop.line_to_list(pattern_dct['lsdb_link'], line)
            # add link information to the global list with current switch and lsdb information 
            lsdb_lst.append([*switch_info_lst[:6], *domain_self_tag_lst,*lsdb_param_lst, *lsdb_link_lst])
            line = file.readline()
        else:
            line = file.readline()
        if not line:
            break
    return line