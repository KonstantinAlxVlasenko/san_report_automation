# from os import walk
import os
import sys
import re
import pandas as pd
import openpyxl

enclosure_params = ['Enclosure Type', 'Enclosure Name', 'Serial Number']
module_params = ['User Assigned Name', 'Product Name', 'Serial Number', 'In-Band IPv4 Address', 'Manufacturer', 'Part Number', 'Firmware Version']
blade_params = ['Blade', 'Server Blade Type', 'Manufacturer', 'Product Name',
                'Part Number', 'Serial Number', 'Server Name', 'IP Address']
hba_params = ['HBA Model', 'WWNp']
vc_params = ['Bay', 'Port', 'Speed', 'WWNp', 'Connected To']

comprehesive_params = [*enclosure_params, *blade_params, *hba_params]
comprehesive_params[2], comprehesive_params[7], comprehesive_params[8] = 'Enclosure Serial Number', 'Server Part Number', 'Server Serial Number'
# print(comprehesive_params)

def create_files_list(blade_path):
    """
    Function to create two lists with unparsed supportshow and amps_maps configs data files.
    Directors have two ".SSHOW_SYS.txt.gz" files. For Active and Standby CPs
    Configuration file for Active CP has bigger size
    """
    
    print(f'\n\nSTEP 1. CHECKING CONFIGURATION DATA ...\n')
    print(f'Configuration data folder {blade_path}')
    # list to save unparsed configuration data files
    files_lst = []
    # going through all directories inside ssave folder to find configurutaion data
    for root, _, files in os.walk(blade_path):
        blade_file_path = None
        for file in files:
            print(file)
            if file.endswith(".txt"):
                blade_file_path = os.path.normpath(os.path.join(root, file))
                files_lst.append(blade_file_path)
            # add info to unparsed list only if supportshow file has been found in current directory
            # if supportshow found but there is no ams_maps files then empty ams_maps list appended to config set 
            # if blade_file_path:
            #     files_lst.append(blade_file_path)
            
    files_num = len(files_lst)
    print(f'Blade system configs: {files_num}')
    
    if files_num == 0:
        print('\nNo confgiguration data found')
        sys.exit()
              
    return files_lst


def check_blades_configs(blade_files_lst):

    files_num = len(blade_files_lst)
    blades_comprehensive_lst = []
    module_comprehensive_lst = []
    vc_comprehensive_lst = []

    for i, blade_config in enumerate(blade_files_lst):

        collected = {'enclosure': False, 'oa_ip': False, 'module': False, 'servers': False, 'vc': False}
        configname_wext = os.path.basename(blade_config)
        configname, _ = os.path.splitext(configname_wext)
        oa_ip = None
        module_num = 0

        info = f'[{i+1} of {files_num}]: {configname}'
        print(info, end='')

        with open(blade_config, encoding='utf-8', errors='ignore') as file:
            # check file until all groups of parameters extracted
            while not all(collected.values()):
                line = file.readline()
                # print(line)
                if not line:
                    break
                # enclosure info collection
                if re.search(r'>SHOW ENCLOSURE INFO|^ +ENCLOSURE INFORMATION$', line):
                    enclosure_dct = {}
                    # line = file.readline()
                    collected['enclosure'] = True
                    # while not re.search(r'Enclosure Information', line):
                    #     print(line)
                    #     line = file.readline()
                    #     if not line:
                    #         break



                    # while not reach empty line
                    while not re.search(r'Serial Number',line):
                        line = file.readline()

                        if re.match(r'^\s*([\w ]+) *: *([\w -]+)', line):
                            # print(line)
                            result = re.match(r'^\s*([\w ]+) *: *([\w -]+)', line)
                            enclosure_dct[result.group(1).strip()] = result.group(2).strip()     
                        if not line:
                            break
                    
                    if enclosure_dct.get('Description'):
                        enclosure_dct['Enclosure Type'] = enclosure_dct.pop('Description')

                    enclosure_lst = [enclosure_dct.get(param) for param in enclosure_params]

                    # print("enclosure_dct: ", enclosure_dct)

                    # print('enclosure_lst: ', enclosure_lst)

                elif re.search(r'FABRIC INFORMATION', line):
                    print(' Type VC')
                    line = file.readline()
                    collected['vc'] = True
                    while not re.search(r'FC-CONNECTION INFORMATION', line):
                        if re.match(r'^ *\w+:(\d+):(\d+) +[\w]+ +([\w]+) +((?:[0-9a-fA-F]{2}:){7}[0-9a-fA-F]{2}) +((?:[0-9a-fA-F]{2}:){7}[0-9a-f]{2}) *$', line):
                            re_object = re.compile(r'^ *\w+:(\d+):(\d+) +[\w]+ +([\w]+) +((?:[0-9a-fA-F]{2}:){7}[0-9a-fA-F]{2}) +((?:[0-9a-fA-F]{2}:){7}[0-9a-f]{2}) *$')
                            # print(line)
                            # vc_slot_port_wwn = re.match(r'^ *\w+:(\d+):(\d+) +[\w]+ +([\w]+) +((?:[0-9a-fA-F]{2}:){7}[0-9a-fA-F]{2}) +((?:[0-9a-fA-F]{2}:){7}[0-9a-f]{2}) *$', line)
                            # slot = vc_slot_port_wwn.group(1)
                            # port = 
                            vc_port_lst = line_to_list(re_object, line, *enclosure_lst)
                            vc_comprehensive_lst.append(vc_port_lst)
                            # print(vc_port_lst)
                            line = file.readline()
                        else:
                            line= file.readline()
                    


                # onboard administrator
                elif re.search(r'>SHOW TOPOLOGY *$', line):
                    print(' Type Blade')
                    line = file.readline()
                    collected['oa_ip'] = True
                    while not re.search(r'^>SHOW', line):
                        if re.match(r'^[\w-]+ +\w+ +\w+ +([\d.]+) +[\w]+ *$', line):
                            oa_ip = re.match(r'^[\w-]+ +\w+ +\w+ +([\d.]+) +[\w]+ *$', line).group(1)
                            line = file.readline()
                        else:
                            line = file.readline()




                # interconnect modules info collection
                elif re.search(r'>SHOW INTERCONNECT INFO ALL', line):
                    line = file.readline()
                    collected['modules'] = True
                    while not re.search(r'^>SHOW', line):
                        # line = file.readline()
                        # if not line:
                        #     break
                        if re.match(r'^(\d). *([\w ]+)$', line):
                            module_dct = {}
                            module_lst= []
                            module = re.match(r'^(\d). *([\w ]+)$', line)
                            module_slot = module.group(1)
                            module_type = module.group(2).rstrip()
                            line = file.readline()
                            while not re.search(r'^(\d). *([\w ]+)$|^>SHOW', line):
                                if re.match(r'^\s+([\w -]+):([\w\(\) .:/-]+)$', line):
                                    result = re.match(r'^\s+([\w -]+):([\w\(\) .:/-]+)$', line)
                                    name = result.group(1).strip()
                                    value = result.group(2).strip()
                                    module_dct[name] = value
                                    # if not blade_dct.get(name_clean):
                                    #     blade_dct[name_clean] = value
                                    # print(f'{name}: {value}')
                                    line = file.readline()
                                else:
                                    line = file.readline()
                                    if not line:
                                        break
                            module_lst = [module_dct.get(param) for param in module_params]
                            module_comprehensive_lst.append([*enclosure_lst, oa_ip, module_slot, module_type, *module_lst])
                            module_num += 1
                        else:
                            line = file.readline()


                elif re.search(r'>SHOW SERVER INFO ALL', line):
                    line = file.readline()
                    collected['servers'] = True
                    while not re.search(r'^>SHOW', line):
                        if re.match(r'^Server\s+(Blade)\s+#(\d+) Information:$', line):
                            blade_dct = {}
                            blade_lst = []
                            hba_lst = []
                            result = re.match(r'^Server\s+(Blade)\s+#(\d+) Information:$', line)
                            blade_dct[result.group(1)] = result.group(2)
                            blade_num = result.group(2)
                            # print("Blade number:", blade_num)
                            line = file.readline()
                            while not re.search(r'^Server Blade #(\d+) Information:$|^>SHOW', line):
                                # print(line)
                                
                                # mezzanin hba info
                                if re.match(r'^\s+Mezzanine \d+: *([\w -]+)$', line):
                                    result = re.match(r'^\s+Mezzanine \d+: *([\w -]+)$', line)
                                    hba_model = result.group(1)
                                    # print(hba_model)
                                    line = file.readline()
                                    while re.match(r'^\s+Port \d+: *([a-f0-9:]{23})$', line):
                                        result = re.match(r'^\s+Port \d+: *([a-f0-9:]{23})$', line)
                                        wwnp = result.group(1)
                                        hba_lst.append([hba_model, wwnp])
                                        # print(wwnp)
                                        line = file.readline()
                                # flb hba info
                                elif re.match(r'^\s+FLB +Adapter +\d+: *([\w -]+)$', line):
                                    result = re.match(r'^\s+FLB +Adapter +\d+: *([\w -]+)$', line)
                                    flex_model = result.group(1)
                                    # print(flex_model)
                                    line = file.readline()
                                    while re.search(r'[\w\d:]{17,23}', line):
                                        # print(line)
                                        if re.match(r'^\s+FCoE FlexHBA LOM\d:\d-\w\s+([\w\d:]{23})$', line):
                                            result = re.match(r'^\s+FCoE FlexHBA LOM\d:\d-\w\s+([\w\d:]{23})$', line)
                                            wwnp = result.group(1)
                                            hba_lst.append([flex_model, wwnp])
                                            # print(wwnp)
                                        line = file.readline()
                                # blade info
                                elif re.match(r'^\s+([\w ]+):(\d-\w)? +([\w\(\) .:/-]+)$', line):
                                    result = re.match(r'^\s+([\w ]+):(\d-\w)? +([\w\(\) .:/-]+)$', line)
                                    name = result.group(1) + result.group(2) if result.group(2) else result.group(1)
                                    name_clean = result.group(1)
                                    value = result.group(3).rstrip()
                                    if not blade_dct.get(name_clean):
                                        blade_dct[name_clean] = value
                                    # print(f'{name}: {value}')
                                    line = file.readline()
                                else:
                                    line = file.readline()
                                    if not line:
                                        break

                            if blade_dct.get('Type'):
                                blade_dct['Server Blade Type'] = blade_dct.pop('Type')


                            blade_lst = [blade_dct.get(param) for param in blade_params]

                            # enclosure_blade_lst = [*enclosure_lst, *blade_lst]

                            if len(hba_lst):
                                for hba in hba_lst:
                                    blades_comprehensive_lst.append([*enclosure_lst, *blade_lst, *hba])
                            else:
                                blades_comprehensive_lst.append([*enclosure_lst, *blade_lst, None, None])

                            # print(enclosure_blade_lst)
                            # print(blade_lst)
                            # print(hba_lst)
        

                        else:
                            line = file.readline()
    
            for num in range(-1, -module_num-1, -1):
                module_comprehensive_lst[num][3] = oa_ip

            # print(vc_comprehensive_lst)
    # print(module_comprehensive_lst)



    return module_comprehensive_lst, blades_comprehensive_lst, vc_comprehensive_lst


def save_blade(blade_path, data_lst, columns_title):

    data_df = pd.DataFrame(data_lst, columns=columns_title)
    data_df['Manufacturer'] = data_df['Manufacturer'].str.upper()
    data_df['Server Model'] = data_df['Manufacturer'] + ' ' + data_df['Product Name']
    data_df.dropna(subset=['HBA Model', 'WWNp'], inplace=True)
    data_df.drop(columns=['Server Blade Type', 'Manufacturer', 'Product Name'], inplace=True)

    # change columns order
    columns_title = data_df.columns.tolist()
    columns_title.insert(4, columns_title.pop(10))
    columns_title.insert(6, columns_title.pop(7))
    data_df = data_df.reindex(columns = columns_title)

    # servers list dataframe
    servers_df = data_df.copy()
    servers_df.drop_duplicates(subset = ['Blade', 'Server Name', 'Server Serial Number'], inplace = True)
    servers_df.drop(columns = ['HBA Model', 'WWNp'], inplace = True)

    file = 'Blades_info.xlsx'
    file_path = os.path.normpath(os.path.join(blade_path, file))

    with pd.ExcelWriter(file_path, engine='openpyxl',  mode='w') as writer:
        data_df.to_excel(writer, sheet_name='blade_hba', index=False)

    with pd.ExcelWriter(file_path, engine='openpyxl',  mode='a') as writer:
        servers_df.to_excel(writer, sheet_name='blade_servers', index=False)

def save_module(blade_path, module_lst, columns_title):

    module_df = pd.DataFrame(module_lst, columns=columns_title)
    file = 'Blades_info.xlsx'
    file_path = os.path.normpath(os.path.join(blade_path, file))
    with pd.ExcelWriter(file_path, engine='openpyxl',  mode='a') as writer:
        module_df.to_excel(writer, sheet_name='modules', index=False)


def save_vc(blade_path, vc_lst, columns_title):

    vc_df = pd.DataFrame(vc_lst, columns=columns_title)
    file = 'Blades_info.xlsx'
    file_path = os.path.normpath(os.path.join(blade_path, file))
    with pd.ExcelWriter(file_path, engine='openpyxl',  mode='a') as writer:
        vc_df.to_excel(writer, sheet_name='vc_ports', index=False)



def line_to_list(re_object, line, *args):
    """Function to extract values from line with regex object 
    and combine values with other optional data into list
    """

    values, = re_object.findall(line)
    if isinstance(values, tuple) or isinstance(values, list):
        values_lst = [value.rstrip() if value else None for value in values]
    else:
        values_lst = [values.rstrip()]
    return [*args, *values_lst]



if __name__ == "__main__":

    blade_path = r'C:\Users\vlasenko\Documents\06.CONFIGS\SBRF\Nov 2019\blade\SZB_Chassis'
    # blade_path = r'C:\Users\vlasenko\Documents\06.CONFIGS\SBRF\Nov 2019\blade\sp-5599-sddb-02'
    # blade_path = r'C:\Users\vlasenko\Documents\06.CONFIGS\Ural'
    blade_files_lst = create_files_list(blade_path)
    module_comprehensive_lst, blades_comprehensive_lst, vc_comprehensive_lst = check_blades_configs(blade_files_lst)
    save_blade(blade_path, blades_comprehensive_lst, comprehesive_params)
    save_module(blade_path, module_comprehensive_lst, [*enclosure_params, 'OA_IP', "module_slot", "module_type", *module_params])
    save_vc(blade_path, vc_comprehensive_lst, [*enclosure_params, *vc_params])