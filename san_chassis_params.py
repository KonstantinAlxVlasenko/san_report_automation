import re
import pandas as pd
# from files_operations import columns_import, status_info, data_extract_objects
from files_operations import status_info, data_extract_objects

"""Module to extract chassis parameters"""

# def data_extract_objects(sheet_title, max_title):
#     params_names, params_add_names = columns_import(sheet_title, max_title, 'params', 'params_add')
#     # params_add_names = columns_import(sheet_title, 'params_add', max_title)
#     # print(params_add_names)
#     keys = columns_import(sheet_title, max_title, 're_names')
#     comp_keys = [key+'_comp' for key in keys]
#     match_keys = [key + '_match' for key in keys]
#     comp_values = columns_import(sheet_title,  max_title, 'comp_values')
#     # comp_values_r = []
#     comp_values_re = [re.compile(element) for element in comp_values]
#     comp_dct = dict(zip(comp_keys, comp_values_re))
    
#     return params_names, params_add_names, comp_keys, match_keys, comp_dct

def chassis_params_extract(all_config_data, max_title):
        
    print('\n\nEXTRACTING CHASSIS PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...')
    # # extract chassis parameters values from init file
    # chassis_params = columns_import('chassis', 'params', max_title)
    # chassis_params_keys  = columns_import('chassis', 'params_add', max_title)
    
    # number of switches to check
    switch_num = len(all_config_data)
    
    # list to store REQUIRED chassis parameters
    chassis_params_fabric_lst = []
    
    # comp_keys = columns_import('chassis', 'comp_names', max_title)
    # comp_values = columns_import('chassis', 'comp_values', max_title)
    # comp_values_re = [re.compile(element) for element in comp_values]
    # comp_dct = dict(zip(comp_keys, comp_values_re))

    chassis_params, chassis_params_add, comp_keys, match_keys, comp_dct = data_extract_objects('chassis', max_title)

    # lines example Number of LS = 4, system.i2cTurboCnfg:1
    # chassis_param_comp = re.compile(r'^([\w .-]+) ?(=|:) ?([\w. :/]+)$')
    # lines example snmp.snmpv3TrapTarget.0.trapTargetAddr:10.99.245.222
    snmp_target_comp = re.compile(r'^(snmp.snmpv3TrapTarget.\d.trapTargetAddr):([\d.]+)$')
    # lines example syslog.address.1:10.99.116.66
    syslog_comp = re.compile(r'^(syslog.address.\d):([\d.]+)$')
    # lines example ts.tzh:3
    tz_comp = re.compile(r'^(ts.tz[hm]):(\d+)$')
    # lines example  09:46:50 up 75 days, 22:29, 1 user, load average: 7.38, 3.83, 2.00
    uptime_cpu_comp = re.compile(r'^ [\d: ]+up\s+([\d]+)\s+days,?\s+[\d:]+,\s+[\w ]+,\s+[a-z ]+:\s+[\d.,]+\s+[\d.,]+\s+([\d.]+)$')
    # lines example /dev/root  394440    207140    166940  55% /
    flash_comp =  re.compile(r'^/dev/root\s+\d+\s+\d+\s+\d+\s+(\d+)%\s+/$')
    # lines example MemTotal: 504348 kB
    memory_comp = re.compile(r'^(\w+):\s+(\d+)\s+kB$')    

    
    # all_confg_data format ([swtch_name, supportshow file, (ams_maps_log files, ...)])
    for i, switch_config_data in enumerate(all_config_data):
        # dictionary to store DISCOVERED chassis parameters
        chassis_params_switch_dct = {}
        # sets and list to store parameters which could be joined in one group 
        snmp_target_set = set()
        syslog_set = set()
        tz_lst = []
        
        # search control dictionary. continue to check sshow_file until all parameters groups are found
        collected = {'configshow': False, 'uptime_cpu': False, 'flash': False, 'memory': False}

        switch_name, sshow_file, ams_maps_file = switch_config_data
        info = f'[{i+1} of {switch_num}]: {switch_name} chassis parameters check'
        print(info, end =" ")
        with open(sshow_file, encoding='utf-8', errors='ignore') as file:
            while not all(collected.values()):
                line = file.readline()
                if not line:
                    break
                # configshow section
                if re.search(r'^\[Configuration upload Information\]$', line):
                    collected['configshow'] = True
                    while not re.search(r'^\[Chassis Configuration End\]$',line):
                        line = file.readline()
                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                        # match_dct = {'chassis_param_match': comp_dct['chassis_param_comp'].match(line)}
                        
                        # chassis_param_match = comp_dct['chassis_param_comp'].match(line)
                        # # chassis_param_match = chassis_param_comp.match(line)
                        # snmp_target_match = snmp_target_comp.match(line)
                        # syslog_match = syslog_comp.match(line)
                        # tz_match = tz_comp.match(line)
                        # if chassis_param_match:
                        #     chassis_params_switch_dct[chassis_param_match.group(1).rstrip()] = chassis_param_match.group(3)
                        # # for snmp and syslog data addresses are added to the coreesponding sets to avoid duplicates
                        # if snmp_target_match and snmp_target_match.group(2) != '0.0.0.0':
                        #     snmp_target_set.add(snmp_target_match.group(2))
                        # if syslog_match:
                        #     syslog_set.add(syslog_match.group(2))
                        # # for timezone extracted data added to the list for later concatenation
                        # if tz_match:
                        #     tz_lst.append(tz_match.group(2))

                        # match_keys ['chassis_param_match', 'snmp_target_match', 'syslog_match', 'tz_match', 'uptime_cpu_match', 'memory_match', 'flash_match'] 
                        # 'chassis_param_match'
                        if match_dct[match_keys[0]]:
                            chassis_params_switch_dct[match_dct[match_keys[0]].group(1).rstrip()] = match_dct[match_keys[0]].group(3)                            
                        # for snmp and syslog data addresses are added to the coreesponding sets to avoid duplicates
                        # 'snmp_target_match'
                        if match_dct[match_keys[1]] and match_dct[match_keys[1]].group(2) != '0.0.0.0':
                            snmp_target_set.add(match_dct[match_keys[1]].group(2))
                        # 'syslog_match'
                        if match_dct[match_keys[2]]:
                            syslog_set.add(match_dct[match_keys[2]].group(2))
                        # for timezone extracted data added to the list for later concatenation
                        # 'tz_match'
                        if match_dct[match_keys[3]]:
                            tz_lst.append(match_dct[match_keys[3]].group(2))                                             
                        if not line:
                            break
                # uptime section
                elif re.search(r'^(/fabos/cliexec/)?uptime *:$', line):
                    collected['uptime_cpu'] = True
                    while not re.search(r'^real [\w.]+$',line):
                        line = file.readline()
                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                        # uptime_cpu_match = uptime_cpu_comp.match(line)
                        # 'uptime_cpu_match'   
                        if match_dct[match_keys[4]]:
                            uptime = match_dct[match_keys[4]].group(1)
                            cpu_load = match_dct[match_keys[4]].group(2)
                        if not line:
                            break
                # flash section
                elif re.search(r'^(/bin/)?df\s*:$', line):
                    collected['flash'] = True
                    while not re.search(r'^real [\w.]+$',line):
                        line = file.readline()
                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                        # flash_match = flash_comp.match(line)
                        # 'flash_match'                      
                        if match_dct[match_keys[6]]:
                            flash = match_dct[match_keys[6]].group(1)
                        if not line:
                            break
                # memory section
                elif re.search(r'^(/bin/)?cat\s+/proc/meminfo\s*:$', line):
                    collected['memory'] = True
                    memory_dct = {}
                    while not re.search(r'^real [\w.]+$',line):
                        line = file.readline()
                        match_dct ={match_key: comp_dct[comp_key].match(line) for comp_key, match_key in zip(comp_keys, match_keys)}
                        # memory_match = memory_comp.match(line)
                        # 'memory_match'                      
                        if match_dct[match_keys[5]]:
                            memory_dct[match_dct[match_keys[5]].group(1)] = match_dct[match_keys[5]].group(2)
                        if not line:
                            break
                    # free_mem + buffers > 5% from total memory
                    # memory usage < 95% 
                    memory = round((1 - (int(memory_dct['MemFree']) + int(memory_dct['Buffers']))/int(memory_dct['MemTotal']))*100)
                    memory = str(memory)                            
                         

        # chassis_params_add order ('configname', 'switchname', 'snmp_server', 'syslog_server', 'timezone_h:m')
        # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column
        chassis_params_values = (sshow_file, ams_maps_file, switch_name, snmp_target_set, syslog_set, tz_lst, uptime, cpu_load, memory, flash)
        
        # adding additional parameters and values to the chassis_params_switch_dct
        for chassis_param_add, chassis_param_value in zip(chassis_params_add,  chassis_params_values):
            if chassis_param_value:                
                if not isinstance(chassis_param_value, str):
                    s = ':' if chassis_param_add == 'timezone_h:m' else ', '
                    chassis_param_value = f'{s}'.join(chassis_param_value)
                chassis_params_switch_dct[chassis_param_add] = chassis_param_value

        # creating list with REQUIRED chassis parameters for the current switch 
        # and appending this list to the list of all switches chassis_params_fabric_lst
        chassis_params_fabric_lst.append([chassis_params_switch_dct.get(chassis_param, None) for chassis_param in chassis_params])
            
        status_info('ok', max_title, len(info))
        
    return chassis_params_fabric_lst