# -*- coding: utf-8 -*-
"""
Created on Sun Jul 30 14:19:53 2023

@author: kavlasenko
"""
import os
import re

script_dir = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\san_report_automation\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop
import general_re_module as reop


max_title = 50
# hw_oceanstor_folder = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\huawei_configs'
hw_oceanstor_folder = r'C:\Users\kavlasenko\Documents\05.PYTHON\Projects\hw_tst'


def storage_oceanstor_extract(hw_oceanstor_folder):

    san_system_oceanstor_lst = []
    san_fcport_oceanstor_lst = []
    san_host_oceanstor_lst = []
    san_host_id_name_oceanstor_lst = []
    san_host_id_fcinitiator_oceanstor_lst = []
    
    san_extracted_oceanstor_dct = {}
    
    
    
    oceanstor_configs_lst = dfop.find_files(hw_oceanstor_folder, max_title, filename_extension='txt')
    configs_num = len(oceanstor_configs_lst)
    
    
    if configs_num:
        for i, storage_config in enumerate(oceanstor_configs_lst):       
            # file name with extension
            configname_wext = os.path.basename(storage_config)
            # remove extension from filename
            configname, _ = os.path.splitext(configname_wext)
            
            # current operation information string
            info = f'[{i+1} of {configs_num}]: {configname} file.'
            print(info, end =" ")
    
            system_summary_lst, fcport_lst, host_id_name_lst, host_id_fcinitiator_lst, host_lst, info, duplicated_config_flag  = storage_params_extract(storage_config, san_extracted_oceanstor_dct, system_params, pattern_dct, info)
            san_system_oceanstor_lst.extend(system_summary_lst)
            san_fcport_oceanstor_lst.extend(fcport_lst)
            if host_id_name_lst:
                san_host_id_name_oceanstor_lst.extend(host_id_name_lst)
            if host_id_fcinitiator_lst:
                san_host_id_fcinitiator_oceanstor_lst.extend(host_id_fcinitiator_lst)
            if host_lst:
                san_host_oceanstor_lst.extend(host_lst)
            
            

    
            
            
            dfop.status_info('ok', max_title, len(info))
            
            
        # print(san_system_oceanstor_lst)
        # print(san_fcport_oceanstor_lst)
        # print(san_host_id_name_oceanstor_lst)
        # print(san_host_id_fcinitiator_oceanstor_lst)
        print(san_host_oceanstor_lst)

            
    return san_system_oceanstor_lst, san_fcport_oceanstor_lst, san_host_oceanstor_lst, san_host_id_name_oceanstor_lst, san_host_id_fcinitiator_oceanstor_lst
            
            
            # # show status blades information extraction from file
            # if blade_lst or enclosure_vc_lst:
            #     meop.status_info('ok', max_title, len(info))
            # else:
            #     meop.status_info('no data', max_title, len(info))
            

patterns = ["PROFILE FOR STORAGE ARRAY: +(.+?) +\((.+?)\)", 
            " *(.+?) *: *(.+)",
             "^ +License-+", 
             "^ +ID: +CTE.+?MGMT",
             " +IPv4 +Address\: +(.+)", 
             " +(IPv4 +Gateway|MAC)\: +.+", 
             
             
             "^FC +Port-+",
             "^ +FC +Port-+",
              
             "^ +ID\: +[\w\.]+", 
             "^ *(SAS|ETH) +Port-+",
             "^\s*$",
             "(^\s*$)|(sfp +info)",
             "^Host-+", "^(Host +Group|Power)-+",
             "^(\d+) +([\w\.-]+) +([\w\.-]+) +([\w\.-]+) +([\w\.-]+) +([\w\.-]+) +([\w\.-]+) +([\w\.-]*) +",
             "^ +Host +\d+-+", "^FC +Initiator-+", "^SFP-+",
             "^(\w+) +([\d-]+) +([\w-]+) +([\w-]+) +([\w-]+) +([\w-]+) +([\w-]+)", 
             "^ +Mapping View +INFO",
             "^ +WWN\: *([\w]+)"]

pattern_names = ['storage_profile', 'parameter_value_pair', 'license_header', "mgmt_eth_id", "ip_addr", "gateway_mac",  
                 "storage_grouped_fcports_header", "controller_grouped_fcports_header", 
                 "port_id", "sas_eth_port_header", "blank_line", "blank_or_sfpinfo",
                 "host_header", "hostgroup_or_power_header", "host_id_name_line", "host_id_header",
                 "fcinitiator_header", "sfp_header", "host_id_fcinitiator_line", "host_mapping_header", "hostport_wwn"]

    
patterns = [re.compile(fr"{element}", re.IGNORECASE) for element in patterns]
pattern_dct = dict(zip(pattern_names, patterns))
        
# pattern_dct = {'storage_profile': r"PROFILE FOR STORAGE ARRAY: +(.+?) +\((.+?)\)", 
#                'parameter_value_pair': r"(.+?) +: +(.+)",
#                'license_header': r"^ +License-+"
#                } 




system_params_add = ['configname', 'IP_Address', 'config_datetime']


system_params = ['configname',
                'System Name',
                'Product Serial Number',
                'Product Model',
                'Product Version',
                'Patch Version',
                'IP_Address',
                'System Location',
                'System Description',
                'Master CPU Usage',
                'Number of total controllers',
                'Number of normal controllers',
                'Number of disk domains',
                'Number of storage pools',
                'System max LUN number',
                'Total number of LUN',
                'Cache low water marker',
                'Cache high water marker',
                'RUN TIME',
                'config_datetime']


# fcport_params_add = ['configname']

# fcport_params = ['configname',
#                 'ID',
#                 'WWN',
#                 'Health Status',
#                 'Running Status',
#                 'SFP Status',
#                 'Type',
#                 'Working Rate(Mbps)',
#                 'Configured Speed(Mbps)',
#                 'Max Speed(Mbps)',
#                 'Role']


fcport_params = [
                'ID',
                'WWN',
                'Health Status',
                'Running Status',
                'SFP Status',
                'Type',
                'Working Rate(Mbps)',
                'Configured Speed(Mbps)',
                'Max Speed(Mbps)',
                'Role']



# host_params_add = ['configname']
host_params = [
               'Id',
               'Name',
               'Os Type',
               'Ip',
               'Location']


def storage_params_extract(storage_config, san_extracted_oceanstor_dct, system_params, pattern_dct, info):
    """Function to extract values from current switch confguration file. 
    Returns list with extracted values"""


    # file name
    configname = os.path.basename(storage_config)
    # search control dictionary. continue to check file until all parameters groups are found
    # collected = {'system': False, 'port': False, 'host': False, 'host_name': False, 'host_fcinitiator': False,}
    collected = {'system': False, 'ip_addr': False, 'fcport': False, "host": False, "fcinitiator": False}
    
    # initialize structures to store collected data for current storage
    # dictionary to store all DISCOVERED parameters
    system_summary_dct = {}
    system_summary_lst = []
    config_datetime = None
    
    mgmt_ip_addr_number = 0
    mgmt_ip_addr_lst = []
    
    fcport_ctrl_group_number = 0
    fcport_lst = []
    
    host_id_name_lst = []
    host_id_fcinitiator_lst = []
    
    host_lst = []


 
    # config_datetime = None
    duplicated_config_flag = False
    
    with open(storage_config, encoding='utf-8', errors='ignore') as file:
        line = file.readline()
        # check file until all groups of parameters extracted
        while not all(collected.values()):
        # while True:
            # line = file.readline()
            # if not line:
            #     break
            # oceanstor_profile_pattern = r"PROFILE FOR STORAGE ARRAY: +(.+?) +\((.+?)\)"
            if re.search(pattern_dct['storage_profile'], line) and not collected['system']:
                print('storage_profile')
                info_system = "System " + re.search(pattern_dct['storage_profile'], line).group(1)
                config_datetime = re.search(pattern_dct['storage_profile'], line).group(2)
                print(info_system, end = " ")
                info = info + " " + info_system
                collected['system'] = True
                # profile and summary section start
                line = reop.extract_key_value_from_line(system_summary_dct, pattern_dct, line, file, 
                                                extract_pattern_name='parameter_value_pair', 
                                                stop_pattern_name='license_header')
                controllers_number = int(system_summary_dct['Number of total controllers'])
                sn = system_summary_dct['Product Serial Number']
                if not sn in san_extracted_oceanstor_dct:
                    san_extracted_oceanstor_dct[sn] = configname
                else:
                    duplicated_config_flag = True
                    system_summary_dct = {}
                    break
                    
                # print(f'{controllers_number=}')
                # system_summary_lst.append([system_summary_dct.get(param) for param in system_params])
            elif re.search(pattern_dct['mgmt_eth_id'], line):
                print('mgmt_eth_id')
                mgmt_ip_addr_number += 1
                if mgmt_ip_addr_number == controllers_number:
                    collected['ip_addr'] = True
                line = reop.extract_value_from_line(mgmt_ip_addr_lst, pattern_dct, 
                                            line, file, 
                                            extract_pattern_name='ip_addr', 
                                            stop_pattern_name='gateway_mac')
            # elif re.search(pattern_dct['storage_grouped_fcports_header'], line):
            #     collected['fcport'] = True
            #     while not re.search(pattern_dct['sas_eth_port'], line):
            #         line = file.readline()
            #         if not line:
            #             break
            #         if re.search(pattern_dct['port_id'], line):
            #             fcport_dct = {}
            #             line = reop.extract_key_value_from_line(fcport_dct, pattern_dct, line, file, 
            #                                             extract_pattern_name='parameter_value_pair', 
            #                                             stop_pattern_name='blank_line', first_line_skip=False)
            #             print('\n')
            #             print(fcport_dct)
            # elif re.search(pattern_dct['controller_groupped_fcports_header'], line):
            #     print('\n')
            #     print(line)
            #     fcport_ctrl_group_number += 1
            #     if fcport_ctrl_group_number == controllers_number:
            #         collected['fcport'] = True
            #     while not re.search(pattern_dct['sas_eth_port'], line):
            #         line = file.readline()
            #         if not line:
            #             break
            #         if re.search(pattern_dct['port_id'], line):
            #             print(line)
            #             fcport_dct = {}
            #             line = reop.extract_key_value_from_line(fcport_dct, pattern_dct, line, file, 
            #                                             extract_pattern_name='parameter_value_pair', 
            #                                             stop_pattern_name='blank_line', first_line_skip=False)
            #             print('\n')
            #             print(fcport_dct)
                        
            elif re.search(pattern_dct['storage_grouped_fcports_header'], line) \
                or re.search(pattern_dct['controller_grouped_fcports_header'], line):
                # print('\n')
                # print(line)
                print('groupped_fcports_header')
                if re.search(pattern_dct['controller_grouped_fcports_header'], line):
                    fcport_ctrl_group_number += 1
                    if fcport_ctrl_group_number == controllers_number:
                        collected['fcport'] = True
                else:
                    collected['fcport'] = True
                while not re.search(pattern_dct['sas_eth_port_header'], line):
                    line = file.readline()
                    if not line:
                        break
                    if re.search(pattern_dct['port_id'], line):
                        # print(line)
                        fcport_dct = {}
                        line = reop.extract_key_value_from_line(fcport_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='parameter_value_pair', 
                                                        stop_pattern_name='blank_or_sfpinfo', first_line_skip=False)
                        # print('\n')
                        # print(fcport_dct)
                        if fcport_dct:
                            # adding additional parameters and values to the parameters dct
                            # reop.update_dct(fcport_params_add, [configname], fcport_dct)                                                
                            # creating list with REQUIRED parameters for the current system.
                            # if no value in the dct for the parameter then None is added 
                            # and appending this list to the list of all systems             
                            fcport_lst.append([configname] + [fcport_dct.get(param) for param in fcport_params])
                            # fcport_lst.append([fcport_dct.get(param) for param in fcport_params])
            # host_id_name section
            elif re.search(pattern_dct['host_header'], line):
                print('host_header')
                collected['host'] = True
                while not re.search(pattern_dct['hostgroup_or_power_header'], line):
                    line = file.readline()
                    if not line:
                        break
                    if re.search(pattern_dct['host_id_name_line'], line):
                
                        line = reop.extract_list_from_line(host_id_name_lst, pattern_dct, 
                                                    line, file, 
                                                    extract_pattern_name= 'host_id_name_line', 
                                                    stop_pattern_name='blank_line', 
                                                    first_line_skip=False, line_add_values=configname)
                        # print('\n')
                        # print(host_id_name_lst)
                    elif re.search(pattern_dct['host_id_header'], line):
                        host_details_dct = {}
                        hostport_wwn_lst = []
                        host_details_lst = []
                        line = reop.extract_key_value_from_line(host_details_dct, pattern_dct, line, file, 
                                                        extract_pattern_name='parameter_value_pair', 
                                                        stop_pattern_name='host_mapping_header')
                        line = reop.extract_value_from_line(hostport_wwn_lst, pattern_dct, line, file, 
                                                        extract_pattern_name='hostport_wwn', 
                                                        stop_pattern_name='blank_line')
                        
                        
                        if host_details_dct:
                            # adding additional parameters and values to the parameters dct
                            # reop.update_dct(host_params_add, [configname], host_details_dct)                                                
                            # creating list with REQUIRED parameters for the current system.
                            # if no value in the dct for the parameter then None is added 
                            # and appending this list to the list of all systems             
                            host_details_lst = [configname] + [host_details_dct.get(param) for param in host_params]
                            for wwn in hostport_wwn_lst:
                                host_lst.append([*host_details_lst, wwn])
                        
                        
                        
                        # print('\n')
                        # print(host_details_dct)
                        # print(hostport_wwn)
                                            
            elif re.search(pattern_dct['fcinitiator_header'], line):
                print('fcinitiator_header')
                collected['fcinitiator'] = True
                while not re.search(pattern_dct['sfp_header'], line):
                    line = file.readline()
                    if not line:
                        break
                    if re.search(pattern_dct['host_id_fcinitiator_line'], line):
                        
                        line = reop.extract_list_from_line(host_id_fcinitiator_lst, pattern_dct, 
                                                    line, file, 
                                                    extract_pattern_name= 'host_id_fcinitiator_line', 
                                                    stop_pattern_name='blank_line', 
                                                    first_line_skip=False, line_add_values=configname)
                        # print('\n') 
            else:
                line = file.readline()
                if not line:
                    break
                # line = goto_line(line, file, line_pattern='storage_fcport_header')
                
                
            # elif re.search(pattern_dct['config_end'], line):
            #     break
            
        # print(f'{mgmt_ip_addr_number=}')
        # print(line)

    
        if system_summary_dct:
            ip_addr = ', '.join(mgmt_ip_addr_lst) if mgmt_ip_addr_lst else None
            system_summary_values = (configname, ip_addr, config_datetime)
            # adding additional parameters and values to the parameters dct
            reop.update_dct(system_params_add, system_summary_values, system_summary_dct)                                                
            # creating list with REQUIRED parameters for the current system.
            # if no value in the dct for the parameter then None is added 
            # and appending this list to the list of all systems             
            system_summary_lst.append([system_summary_dct.get(param) for param in system_params])

        
    
        # print('\n')
        # print(mgmt_ip_addr_lst)
        
        # print('\n')
        # print(fcport_lst)
        
        # print('\n')
        # print(host_lst)
        
        
                
        return system_summary_lst, fcport_lst, host_id_name_lst, host_id_fcinitiator_lst, host_lst, info, duplicated_config_flag

san_system_oceanstor_lst, san_fcport_oceanstor_lst, san_host_oceanstor_lst, san_host_id_name_oceanstor_lst, san_host_id_fcinitiator_oceanstor_lst = storage_oceanstor_extract(hw_oceanstor_folder)


'WWN' 'Host Id' 'Running Status' 'Multipath Type' 'Failover Mode' 'Path Type' 'Special Mode Type'