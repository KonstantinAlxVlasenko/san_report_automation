import pandas as pd
import numpy as np
import os
import re

filepath = r'C:\Users\vlasenko\Documents\01.CUSTOMERS\Sibintek\nscamshow'
filename = r'nscamshow.xlsx'
file = os.path.join(filepath, filename)

nscamshow_df = pd.read_excel(file, sheet_name='nscamshow')
# nscamshow_df.duplicated(subset = ['PortName']).value_counts()
nscamshow_symb_columns = ['portSymbUsed', 'nodeSymbUsed', 'deviceManufacturer', 'deviceModel', 'deviceSN', 'deviceName', 'devicePort', 'deviceFw',
                          'HBA_Manufacturer', 'HBA_Model', 'Host_Name', 'Host_OS', 'HBA_Firmware', 'HBA_Driver']


def nsshow_main(nscamshow_df):
    
    nscamshow_join_df = nscamshow_preparation(nscamshow_df)
    # nscamshow_symb_columns = ['deviceManufacturer', 'deviceModel', 'deviceSN', 'deviceName', 'deviceFw',
    #                           'HBA_Manufacturer', 'HBA_Model', 'Host_Name', 'Host_OS', 'HBA_Firmware', 'HBA_Driver']
    
    # add 'deviceType', 'deviceSubtype' columns
    nscamshow_join_df = nscamshow_join_df.reindex(columns=[*nscamshow_join_df.columns.tolist(), *nscamshow_symb_columns])
    
    nscamshow_join_df[nscamshow_symb_columns] = nscamshow_join_df.apply(lambda series: nscamshow_symb_split(series), axis = 1)
    
    # nscamshow_join_df.deviceModel.replace(to_replace = ' +', value = r' ', regex = True, inplace = True)
    # nscamshow_join_df.replace(to_replace = ' +', value = r' ', regex = True, inplace = True)
    
    # show unchecked PortSymb and NodeSymb
    mask1 = nscamshow_join_df[['portSymbUsed', 'nodeSymbUsed']].isna().all(axis=1)
    mask2 = nscamshow_join_df[['PortSymb',	'NodeSymb']].notna().any(axis=1)
    nscamshow_unchecked_df = nscamshow_join_df.loc[mask1&mask2]
    
    with pd.ExcelWriter(file, engine='openpyxl',  mode='a') as writer:
        nscamshow_join_df.to_excel(writer, index = False, sheet_name = 'nscamshow_processed')
    
    

def nscamshow_symb_split(series):
    # print(series)
    
    xp_msa_pattern = r'^(([A-Z]+) +[\w -]+) +([A-Z\d]{4})$'
    
    par3_node_pattern = r'^(((HPE?)_3PAR(?: +InServ)? *\w{4,5}) *- *([\w]+)) *- *fw:([\d]+)$'
    par3_port_pattern = r'^\w+ +- +(\d:\d:\d) +- +([\w]+)'
    
    qlogic_node_pattern = r'^(\w+) +FW:v([\d.]+) DVR:v([\w.-]+)'
    emulex_node_pattern = r'^(\w+) ([\w/-]+) +FV([\w.]+) +DV([\w.-]+)(?: +HN:)? *([\w.\(\)-]+)?(?: +OS:)?([\w .]+)?$'
    
    ultrium_node_pattern = r'^((\w+) +U[LlTt]+[\w\-]+ ?\d?[ -]?(?:SCSI|Fibre Channel)? ?(?:\w{2})?) +(\w{4})? ?(?:S/N-)?(\w{10})?$'
    ultrium_port_pattern = r'^(\w+)? ?(Ultrium +\d+) +Fibre +Channel +(\w{4})? ?S/N-(\w{10}) *(Port-\w+)$'
    
    eva_port_pattern = r'^(HSV\d{3}) +- +(EVA[\w_]+) +- +\w+$'
    
    storeonce_node_pattern = r'^((HPE?) +StoreOnce) S/N-(\w{10}) (?:[\w ]+)$'
    storeonce_port_pattern =r'^(?:HPE? StoreOnce S/N-\w{14} )?((HPE?) +StoreOnce)(?:[\w ]+)? S/N-(\w{10})(?:[\w ]+)?(Port[\- ]\d?)$'
    
    hpux_node_pattern = r'^([\w_-]+)_(HP-UX_B.11.\d{2})$'
    qlogic_brocade_cna_port_pattern = r'^(QLogic|Brocade)[ -]?(\d+) *(?:\w+)? ?v?([\d.v]+)?(?:[\w ]+)?$'
    qlogic_emulex_port_pattern = r'^(QLogic|Emulex) +P(?:ort\d+ +WW)?PN[ -]?([0-9a-f]{2}:){7}[0-9a-f]{2}(.+)?$'
    emc_vplex_node_port_pattern = r'^(EMC) +(\w+) +([\w]+) +(?:\w+)? ?([\w\-]+)?$'
    clariion_port_pattern = r'^(\w+):+(SP\w+):+FC:+$'
    infinibox_port_pattern = r'^((NFINIDAT)InfiniBox) *\w*$'
    msl_port_pattern = r'^((?:[\w ]+) +Library) +S/N[ -]?(\w+) +v?(Port-\w)?$'
    
    port_symb = series['PortSymb']
    node_symb = series['NodeSymb']
    # print('port_symb: ', port_symb, pd.isnull(port_symb))
    

        
    # if not pd.isna(series[['PortSymb', 'NodeSymb']]).all() and re.match(xp_msa_pattern, port_symb) and re.match(xp_msa_pattern, port_symb):
    
    # 3par node symb
    if not pd.isnull(node_symb) and re.match(par3_node_pattern, node_symb):
        match = re.match(par3_node_pattern, node_symb)
        # series['deviceManufacturer'] = match.group(1)
        # series['deviceModel'] = match.group(1) + " " + match.group(2)
        # series['deviceSN'] = match.group(3)
        # series['deviceName'] = match.group(1) + " " + match.group(2) + " " + match.group(3)
        # series['deviceFw'] = match.group(4)
        # series['nodeSymbUsed'] = 'yes'
        series['deviceManufacturer'] = match.group(3)
        series['deviceModel'] = match.group(2)
        series['deviceSN'] = match.group(4)
        series['deviceName'] = match.group(1)
        series['deviceFw'] = match.group(5)
        series['nodeSymbUsed'] = 'yes'
        # 3par port symb
        if not pd.isnull(port_symb) and re.match(par3_port_pattern, port_symb):
            match = re.match(par3_port_pattern, port_symb)
            series['devicePort'] = match.group(1)
            series['HBA_Model'] = match.group(2)
            series['portSymbUsed'] = 'yes'
    # qlogic node_symb
    elif not pd.isnull(node_symb) and re.match(qlogic_node_pattern, node_symb):
        match = re.match(qlogic_node_pattern, node_symb)
        series['HBA_Model'] = match.group(1)
        series['HBA_Firmware'] = match.group(2)
        series['HBA_Driver'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
        # xp, msa, p2000 port_symb
        if not pd.isnull(port_symb) and re.match(xp_msa_pattern, port_symb):
            match = re.match(xp_msa_pattern, series['PortSymb'])
            # series['deviceManufacturer'] = match.group(1)
            # series['deviceModel'] = match.group(1) + " " + match.group(2)
            # series['deviceFw'] = match.group(3)
            # series['portSymbUsed'] = 'yes'
            series['deviceManufacturer'] = match.group(2)
            series['deviceModel'] = match.group(1)
            series['deviceFw'] = match.group(3)
            series['portSymbUsed'] = 'yes'
        # qlogic port symb
        elif not pd.isnull(port_symb) and re.match(qlogic_emulex_port_pattern, port_symb):
            match = re.match(qlogic_emulex_port_pattern, series['PortSymb'])
            series['HBA_Manufacturer'] = match.group(1)
            series['portSymbUsed'] = 'yes'
        # infinibox port_symb
        elif not pd.isnull(port_symb) and re.match(infinibox_port_pattern, port_symb):
            match = re.match(infinibox_port_pattern, series['PortSymb'])
            series['deviceManufacturer'] = match.group(2)
            series['deviceModel'] = match.group(1)
            series['portSymbUsed'] = 'yes'
        
    # emulex node_symb
    elif not pd.isnull(node_symb) and re.match(emulex_node_pattern, node_symb):
        match = re.match(emulex_node_pattern, node_symb)
        'HBA_Manufacturer', 'HBA_Model', 'Host_Name', 'Host_OS', 'HBA_Firmware', 'HBA_Driver'
        series['HBA_Manufacturer'] = match.group(1)
        series['HBA_Model'] = match.group(2)
        series['HBA_Firmware'] = match.group(3)
        series['HBA_Driver'] = match.group(4)
        if match.group(5) and not re.search(r'localhost|none', match.group(5)):
            series['Host_Name'] = match.group(5).rstrip('.')
        if match.group(6):
            series['Host_OS'] = match.group(6).rstrip('.')
        series['nodeSymbUsed'] = 'yes'
    # hpux node symb
    elif not pd.isnull(node_symb) and re.match(hpux_node_pattern, node_symb):
        match = re.match(hpux_node_pattern, node_symb)
        if match.group(1) and not re.search(r'localhost|none', match.group(1)):
            series['Host_Name'] = match.group(1)
        series['Host_OS'] = match.group(2)
        series['nodeSymbUsed'] = 'yes'
    # ultrium node symb
    elif not pd.isnull(node_symb) and re.match(ultrium_node_pattern, node_symb):
        match = re.match(ultrium_node_pattern, node_symb)
        # series['deviceManufacturer'] = match.group(1)
        # series['deviceModel'] = match.group(1) + " " + match.group(2)
        # series['deviceSN'] = match.group(4)
        # series['deviceFw'] = match.group(3)
        series['deviceManufacturer'] = match.group(2)
        series['deviceModel'] = match.group(1)
        series['deviceSN'] = match.group(4)
        series['deviceFw'] = match.group(3)
        series['nodeSymbUsed'] = 'yes'
    
    
    # storeonce port symb
    elif not pd.isnull(port_symb) and re.match(storeonce_port_pattern, port_symb):
        match = re.match(storeonce_port_pattern, port_symb)
        series['deviceManufacturer'] = match.group(2)
        series['deviceModel'] = match.group(1)
        series['deviceSN'] = match.group(3)
        if match.group(3):
            series['deviceName'] = match.group(1) + " " + match.group(3)
        series['devicePort'] = match.group(4)
        series['portSymbUsed'] = 'yes'
    # storeonce node symb
    elif not pd.isnull(node_symb) and re.match(storeonce_node_pattern, node_symb):
        match = re.match(storeonce_node_pattern, node_symb)
        # series['deviceManufacturer'] = match.group(1)
        # series['deviceModel'] = match.group(1) + " " + match.group(2)
        # series['deviceSN'] = match.group(3)
        # series['nodeSymbUsed'] = 'yes'
        series['deviceManufacturer'] = match.group(2)
        series['deviceModel'] = match.group(1)
        series['deviceSN'] = match.group(3)
        if match.group(3):
            series['deviceName'] = match.group(1) + " " + match.group(3)
        series['nodeSymbUsed'] = 'yes'
    
        
    # emc vplex node_symb
    elif not pd.isnull(node_symb) and re.match(emc_vplex_node_port_pattern, node_symb):
        match = re.match(emc_vplex_node_port_pattern, node_symb)
        series['deviceManufacturer'] = match.group(1)
        series['deviceModel'] = match.group(1) + " " + match.group(2)
        series['deviceSN'] = match.group(3)
        
        series['deviceName'] = match.group(1) + " " + match.group(2) +  " " +  match.group(3)
        series['nodeSymbUsed'] = 'yes'
        # emc vplex port symb
        if not pd.isnull(port_symb) and re.match(emc_vplex_node_port_pattern, port_symb):
            match = re.match(emc_vplex_node_port_pattern, port_symb)
            series['devicePort'] = match.group(4)
            series['portSymbUsed'] = 'yes'

    # xp, msa, p2000 port_symb
    elif not pd.isnull(port_symb) and re.match(xp_msa_pattern, port_symb):
        match = re.match(xp_msa_pattern, port_symb)
        series['deviceManufacturer'] = match.group(2)
        series['deviceModel'] = match.group(1)
        series['deviceFw'] = match.group(3)
        series['portSymbUsed'] = 'yes'
    # eva port symb
    elif not pd.isnull(port_symb) and re.match(eva_port_pattern, port_symb):
        match = re.match(eva_port_pattern, port_symb)
        series['deviceModel'] = match.group(1)
        series['deviceName'] = match.group(2)
        series['portSymbUsed'] = 'yes'
    # # ultrium port symb
    # elif not pd.isnull(port_symb) and re.match(ultrium_port_pattern, port_symb):
    #     match = re.match(ultrium_port_pattern, port_symb)
    #     series['deviceManufacturer'] = match.group(1)
    #     series['deviceModel'] = match.group(1) + " " + match.group(2)
    #     series['deviceFw'] = match.group(3)
    #     series['deviceSN'] = match.group(4)
    #     series['devicePort'] = match.group(5)
    #     series['portSymbUsed'] = 'yes'
    
    # msl port symb
    elif not pd.isnull(port_symb) and re.match(msl_port_pattern, port_symb):
        match = re.match(msl_port_pattern, port_symb)
        series['deviceModel'] = match.group(1)
        # series['deviceName'] = match.group(1)
        series['deviceSN'] = match.group(2)
        series['devicePort'] = match.group(3)
        series['portSymbUsed'] = 'yes'
    # brocade hba, glogic hba,cna port symb
    elif not pd.isnull(port_symb) and re.match(qlogic_brocade_cna_port_pattern, port_symb):
        match = re.match(qlogic_brocade_cna_port_pattern, port_symb)
        series['HBA_Manufacturer'] = match.group(1)
        series['HBA_Model'] = match.group(1) + " " + match.group(2)
        series['HBA_Driver'] = match.group(3)
        series['portSymbUsed'] = 'yes'
    # clariion port symb
    elif not pd.isnull(port_symb) and re.match(clariion_port_pattern, port_symb):
        match = re.match(clariion_port_pattern, port_symb)
        series['deviceManufacturer'] = 'EMC'
        series['deviceModel'] = 'EMC ' + match.group(1)
        series['devicePort'] = match.group(2)
        series['portSymbUsed'] = 'yes'
    # emulex port symb when node symb is empty
    elif not pd.isnull(port_symb) and re.match(qlogic_emulex_port_pattern, port_symb):
        match = re.match(qlogic_emulex_port_pattern, series['PortSymb'])
        series['HBA_Manufacturer'] = match.group(1)
        series['portSymbUsed'] = 'yes'
    else:
        if not pd.isnull(node_symb):
            series['deviceName'] = series['NodeSymb']
        if not pd.isnull(port_symb):
            series['devicePort'] = series['PortSymb']

        
    return pd.Series([series[column] for column in nscamshow_symb_columns])


def nscamshow_preparation(nscamshow_df):
    """Function to remove PortName duplicates and clean PortSymb and NodeSymb columns"""
    """Function to label Remote devices in the Name Server (NS) cache DataFrame"""

    # create Remote devices in the Name Server (NS) cache DataFrame
    nscamshow_lst = ['configname', 'chassis_name', 'chassis_wwn', 'SwitchName', 'switchWwn', 'PortName', 'PortSymb', 'NodeSymb', 'Device_type']
    nscamshow_join_df = nscamshow_df.loc[:, nscamshow_lst]
    

    
    # rename SwitchName columnname
    nscamshow_join_df.rename(columns = {'SwitchName': 'switchName'}, inplace = True)
    # lowercase SwitchName
    nscamshow_lst[3] = nscamshow_lst[3][0].lower() + nscamshow_lst[3][1:] 
    
    # nscamshow_join_df.duplicated(subset = ['PortName']).value_counts()
    
    # # label Remote devices in the Name Server (NS) cache with Fabric labels
    # nscamshow_join_df = nscamshow_join_df.merge(fabric_labels_df, how = 'left', on = nscamshow_lst[:5])
    # # drop switch information columns
    # nscamshow_join_df.drop(columns = nscamshow_lst[:5], inplace= True)
    # drop duplicates WWNp
    nscamshow_join_df.drop_duplicates(subset = ['PortName'], inplace = True)
    
    nscamshow_join_df['PortSymbOrig'] = nscamshow_join_df['PortSymb']
    nscamshow_join_df['NodeSymbOrig'] = nscamshow_join_df['NodeSymb']

    # node_symb_pattern = r' *\[?\d*\]? *"([\w -.:/]+)"?$'
    # extract value from quotaion marks
    node_symb_pattern = r' *\[?\d*\]? *"([\w .:/-]+) *"?$'
    nscamshow_join_df.PortSymb = nscamshow_join_df.PortSymb.str.extract(node_symb_pattern)
    # remove 
    nscamshow_join_df.PortSymb.replace(to_replace = ' +', value = r' ', regex = True, inplace = True)
    nscamshow_join_df.PortSymb.replace(to_replace = '^\d$', value = np.nan, regex = True, inplace = True)
    nscamshow_join_df.NodeSymb = nscamshow_join_df.NodeSymb.str.extract(node_symb_pattern)
    nscamshow_join_df.NodeSymb.replace(to_replace = ' +', value = r' ', regex = True, inplace = True)
    
    nscamshow_join_df.PortSymb = nscamshow_join_df.PortSymb.str.strip()
    nscamshow_join_df.NodeSymb = nscamshow_join_df.NodeSymb.str.strip()
    # if cell contains white spaces only replace it with None
    nscamshow_join_df[['PortSymb', 'NodeSymb']] = nscamshow_join_df[['PortSymb', 'NodeSymb']].replace(r'^\s*$', np.nan, regex=True)
    
    # nscamshow_join_df[['PortSymb', 'NodeSymb']] = nscamshow_join_df[['PortSymb', 'NodeSymb']].replace(to_replace = ' +', value = r' ', regex = True, inplace = True)
    # nscamshow_join_df.PortSymb.replace(to_replace = ' +', value = r' ', regex = True, inplace = True)
    # nscamshow_join_df.NodeSymb.replace(to_replace = ' +', value = r' ', regex = True, inplace = True)
    
    return nscamshow_join_df

    