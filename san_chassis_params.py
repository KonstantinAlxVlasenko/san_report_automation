import re
import pandas as pd
from files_operations import columns_import, status_info

"""Module to extract chassis parameters"""


def chassis_params_extract(all_config_data, max_title):
        
    print('\n\nEXTRACTING CHASSIS PARAMETERS FROM SUPPORTSHOW CONFIGURATION FILES ...')
    # extract chassis parameters values from init file
    chassis_params = columns_import('parameters', 'chassis_params', max_title)
    chassis_params_keys  = columns_import('parameters', 'chassis_params_add', max_title)
    
    # number of switches to check
    switch_num = len(all_config_data)
    
    # list to store REQUIRED chassis parameters
    chassis_params_fabric_lst = []
    

    
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
            # lines example Number of LS = 4, system.i2cTurboCnfg:1
            chassis_param_comp = re.compile(r'^([\w .-]+) ?(=|:) ?([\w. :/]+)$')
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
            
            while not all(collected.values()):
                line = file.readline()
                if not line:
                    break
                # configshow section
                if re.search(r'^\[Configuration upload Information\]$', line):
                    collected['configshow'] = True
                    while not re.search(r'^\[Chassis Configuration End\]$',line):
                        line = file.readline()
                        chassis_param_match = chassis_param_comp.match(line)
                        snmp_target_match = snmp_target_comp.match(line)
                        syslog_match = syslog_comp.match(line)
                        tz_match = tz_comp.match(line)
                        if chassis_param_match:
                            chassis_params_switch_dct[chassis_param_match.group(1).rstrip()] = chassis_param_match.group(3)
                        # for snmp and syslog data addresses are added to the coreesponding sets to avoid duplicates
                        if snmp_target_match and snmp_target_match.group(2) != '0.0.0.0':
                            snmp_target_set.add(snmp_target_match.group(2))
                        if syslog_match:
                            syslog_set.add(syslog_match.group(2))
                        # for timezone extracted data added to the list for later concatenation
                        if tz_match:
                            tz_lst.append(tz_match.group(2))                
                        if not line:
                            break
                # uptime section
                elif re.search(r'^(/fabos/cliexec/)?uptime *:$', line):
                    collected['uptime_cpu'] = True
                    while not re.search(r'^real [\w.]+$',line):
                        line = file.readline()
                        uptime_cpu_match = uptime_cpu_comp.match(line)                       
                        if uptime_cpu_match:
                            uptime = uptime_cpu_match.group(1)
                            cpu_load = uptime_cpu_match.group(2)
                        if not line:
                            break
                # flash section
                elif re.search(r'^(/bin/)?df\s*:$', line):
                    collected['flash'] = True
                    while not re.search(r'^real [\w.]+$',line):
                        line = file.readline()
                        flash_match = flash_comp.match(line)                      
                        if flash_match:
                            flash = flash_match.group(1)
                        if not line:
                            break
                # memory section
                elif re.search(r'^(/bin/)?cat\s+/proc/meminfo\s*:$', line):
                    collected['memory'] = True
                    memory_dct = {}
                    while not re.search(r'^real [\w.]+$',line):
                        line = file.readline()
                        memory_match = memory_comp.match(line)                      
                        if memory_match:
                            memory_dct[memory_match.group(1)] = memory_match.group(2)
                        if not line:
                            break
                    # free_mem + buffers > 5% from total memory
                    # memory usage < 95% 
                    memory = round((1 - (int(memory_dct['MemFree']) + int(memory_dct['Buffers']))/int(memory_dct['MemTotal']))*100)
                    memory = str(memory)                            
                         

        # chassis_params_keys order ('configname', 'switchname', 'snmp_server', 'syslog_server', 'timezone_h:m')
        # values axtracted in manual mode. if change values order change keys order in init.xlsx "chassis_params_add" column
        chassis_params_values = (sshow_file, ams_maps_file, switch_name, snmp_target_set, syslog_set, tz_lst, uptime, cpu_load, memory, flash)
        
        # adding additional parameters and values to the chassis_params_switch_dct
        for chassis_param_key, chassis_param_value in zip(chassis_params_keys,  chassis_params_values):
            if chassis_param_value:                
                if not isinstance(chassis_param_value, str):
                    s = ':' if chassis_param_key == 'timezone_h:m' else ', '
                    chassis_param_value = f'{s}'.join(chassis_param_value)
                chassis_params_switch_dct[chassis_param_key] = chassis_param_value

        # creating list with REQUIRED chassis parameters and adding this list to the chassis_params_fabric_lst
        chassis_params_fabric_lst.append([chassis_params_switch_dct.get(chassis_param, None) for chassis_param in chassis_params])
            
        status_info('ok', max_title, len(info))
        
    return chassis_params_fabric_lst