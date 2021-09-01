"""Module to extend portshow_df DataFrame with information from switch, nameserver, 
blade and synergy enclosures DataFrames"""

from common_operations_filesystem import save_xlsx_file
import numpy as np
import pandas as pd

from analysis_portcmd_aliasgroup import alias_preparation, group_name_fillna
from analysis_portcmd_bladesystem import blade_server_fillna, blade_vc_fillna, vc_name_fillna
from analysis_portcmd_storage import storage_3par_fillna
from analysis_portcmd_devicetype import oui_join, type_check
from analysis_portcmd_gateway import verify_gateway_link, verify_trunkarea_link
from analysis_portcmd_nameserver import nsshow_analysis_main
from analysis_portcmd_switch import fill_isl_link, fill_switch_info, switchparams_join, switchshow_join
from common_operations_dataframe import dataframe_fabric_labeling


def portshow_aggregated(portshow_df, switchshow_ports_df, switch_params_df, switch_params_aggregated_df, 
                        isl_aggregated_df, nsshow_df, nscamshow_df, ag_principal_df, porttrunkarea_df, switch_models_df, alias_df, oui_df, fdmi_df, 
                        blade_module_df, blade_servers_df, blade_vc_df, synergy_module_df, synergy_servers_df, system_3par_df, port_3par_df, 
                        re_pattern_lst, report_data_lst):
    """
    Function to fill portshow DataFrame with information from DataFrames passed as params
    and define fabric device types
    """
    
    # lower case WWNp
    blade_servers_df.portWwn = blade_servers_df.portWwn.str.lower()
    portshow_df.Connected_portWwn = portshow_df.Connected_portWwn.str.lower()
    # add switch information (switchName, portType, portSpeed) to portshow DataFrame
    portshow_aggregated_df = switchshow_join(portshow_df, switchshow_ports_df)
    # add fabric information (FabricName, FabricLabel)
    portshow_aggregated_df = dataframe_fabric_labeling(portshow_aggregated_df, switch_params_aggregated_df)
    # add switchMode to portshow_aggregated DataFrame
    portshow_aggregated_df = switchparams_join(portshow_aggregated_df, switch_params_df, 
                                                switch_params_aggregated_df, report_data_lst)
    # prepare alias_df (label fabrics, replace WWNn with WWNp if present)
    alias_wwnp_df, alias_wwnn_wwnp_df, fabric_labels_df = \
        alias_preparation(nsshow_df, alias_df, switch_params_aggregated_df)
    # retrieve storage, host, HBA information from Name Server service and FDMI data
    nsshow_join_df, nsshow_unsplit_df = \
        nsshow_analysis_main(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df, re_pattern_lst)
    # add nsshow and alias informormation to portshow_aggregated_df DataFrame
    portshow_aggregated_df = \
        alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nsshow_join_df)
    # fillna portshow_aggregated DataFrame null values with values from blade_servers_join_df
    portshow_aggregated_df = \
        blade_server_fillna(portshow_aggregated_df, blade_servers_df, synergy_servers_df, re_pattern_lst)
    # fillna portshow_aggregated DataFrame null values with values from blade_vc_join_df
    portshow_aggregated_df = \
        blade_vc_fillna(portshow_aggregated_df, blade_module_df, blade_vc_df, synergy_module_df)
    # fillna portshow_aggregated DataFrame null values with values from collected 3PAR configs
    portshow_aggregated_df = \
        storage_3par_fillna(portshow_aggregated_df, system_3par_df, port_3par_df)
    # calculate virtual channel id for medium priority traffic
    portshow_aggregated_df = vc_id(portshow_aggregated_df)
    # add 'deviceType', 'deviceSubtype' columns
    portshow_aggregated_df = \
        portshow_aggregated_df.reindex(
            columns=[*portshow_aggregated_df.columns.tolist(), 'deviceType', 'deviceSubtype'])
    # add preliminarily device type (SRV, STORAGE, LIB, SWITCH, VC) and subtype based on oui (WWNp)
    portshow_aggregated_df = oui_join(portshow_aggregated_df, oui_df)
    # preliminarily assisgn to all initiators type SRV
    mask_initiator = portshow_aggregated_df.Device_type.isin(['Physical Initiator', 'NPIV Initiator'])
    portshow_aggregated_df.loc[mask_initiator, ['deviceType', 'deviceSubtype']] = ['SRV', 'SRV']
    # define oui for each connected device to identify device type
    switches_oui = switch_params_aggregated_df['switchWwn'].str.slice(start = 6)
    # final device type define
    portshow_aggregated_df[['deviceType', 'deviceSubtype']] = portshow_aggregated_df.apply(
        lambda series: type_check(series, switches_oui, blade_servers_df, synergy_servers_df), axis = 1)
    # identify MSA port numbers (A1-A4, B1-B4) based on PortWwn
    portshow_aggregated_df.Device_Port = \
        portshow_aggregated_df.apply(lambda series: find_msa_port(series) \
        if (pd.notna(series['PortName']) and  series['deviceSubtype'] == 'MSA') else series['Device_Port'], axis=1)   

    # verify access gateway links
    portshow_aggregated_df, expected_ag_links_df = \
        verify_gateway_link(portshow_aggregated_df, switch_params_aggregated_df, ag_principal_df, switch_models_df)



    # fill isl links information
    portshow_aggregated_df = \
        fill_isl_link(portshow_aggregated_df, isl_aggregated_df)
    # fill connected switch information
    portshow_aggregated_df = \
        fill_switch_info(portshow_aggregated_df, switch_params_df, 
                            switch_params_aggregated_df, report_data_lst)
    # filled device information for trunkarea links
    portshow_aggregated_df = verify_trunkarea_link(portshow_aggregated_df, porttrunkarea_df)

    # libraries Device_Host_Name correction to avoid hba information from FDMI DataFrame usage for library name
    portshow_aggregated_df.Device_Host_Name = \
        portshow_aggregated_df.apply(lambda series: lib_name_correction(series) \
        if pd.notna(series[['deviceType', 'Device_Name']]).all() else series['Device_Host_Name'], axis=1)
    # fill portshow_aggregated DataFrame Device_Host_Name column null values with alias group name values
    portshow_aggregated_df = group_name_fillna(portshow_aggregated_df)
    # fill portshow_aggregated DataFrame Device_Host_Name column null values with alias values 
    portshow_aggregated_df.Device_Host_Name.fillna(portshow_aggregated_df.alias, inplace = True)
    # fill portshow_aggregated DataFrame Device_Host_Name column null values for VC modules 
    # with combination of 'VC' and serial number
    portshow_aggregated_df = vc_name_fillna(portshow_aggregated_df)

    # if Device_Host_Name is still empty fill empty values in portshow_aggregated_df Device_Host_Name column 
    # with combination of device class and it's wwnp
    portshow_aggregated_df.Device_Host_Name = \
        portshow_aggregated_df.apply(lambda series: device_name_fillna(series), axis=1)
    # sorting DataFrame
    sort_columns = ['Fabric_name', 'Fabric_label', 'chassis_wwn', 'chassis_name', 
                    'switchWwn', 'switchName']
    sort_order = [True, True, False, True, False, True]
    portshow_aggregated_df.sort_values(by=sort_columns, ascending=sort_order, inplace=True)

    return portshow_aggregated_df, alias_wwnn_wwnp_df, nsshow_unsplit_df, expected_ag_links_df


def alias_nsshow_join(portshow_aggregated_df, alias_wwnp_df, nsshow_join_df):
    """Function to add porttype (Target, Initiator) and alias to portshow_aggregated DataFrame"""
    
    nsshow_join_df.drop(columns = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn'], inplace = True)

    # adding porttype (Target, Initiator) to portshow_aggregated DataFrame
    portshow_aggregated_df = portshow_aggregated_df.merge(nsshow_join_df, how = 'left', 
                                                          left_on = ['Fabric_name', 'Fabric_label', 'Connected_portWwn'], 
                                                          right_on = ['Fabric_name', 'Fabric_label', 'PortName'])
    
    # if switch in AG mode then device type must be replaced to Physical instead of NPIV
    mask_ag = portshow_aggregated_df.switchMode == 'Access Gateway Mode'
    portshow_aggregated_df.loc[mask_ag, 'Device_type'] = \
        portshow_aggregated_df.loc[mask_ag, 'Device_type'].str.replace('NPIV', 'Physical')

    # add aliases to portshow_aggregated_df
    portshow_aggregated_df = portshow_aggregated_df.merge(alias_wwnp_df, how = 'left', 
                                                          on = ['Fabric_name', 'Fabric_label', 'PortName'])

    # add Unknown(initiator/target) tag for all F-port and N-port for which Device_type is not found 
    mask_nport_fport = portshow_aggregated_df['portType'].isin(['F-Port', 'N-Port'])
    mask_device_type_empty = portshow_aggregated_df['Device_type'].isna()
    mask_not_trunk = ~portshow_aggregated_df['portScn'].str.contains('Trunk port')
    portshow_aggregated_df.loc[mask_nport_fport & mask_device_type_empty & mask_not_trunk, 'Device_type'] = 'Unknown(initiator/target)'

    return portshow_aggregated_df


def device_name_fillna(series):
    """
    Function to fill empty values in portshow_aggregated_df Device_Host_Name column
    with combination of device class and it's wwnn
    """

    mask_switch = series['deviceType'] == 'SWITCH'
    # mask_vc = series['deviceType'] == 'VC'
    mask_name_notna = pd.notna(series['Device_Host_Name'])
    mask_devicetype_wwn_isna =  pd.isna(series[['deviceType', 'NodeName']]).any()
    # if device is switch or already has name no action required
    if mask_switch or mask_name_notna:
        return series['Device_Host_Name']
    # if no device class defined or no wwn then it's not possible to concatenate to values
    if  mask_devicetype_wwn_isna:
        return np.nan
    
    return series['deviceType'] + ' ' + series['NodeName']


def vc_id(portshow_aggregated_df):
    """
    Function to calculate virtual channel id for medium priority traffic.
    VC2, VC3, VC4, VC5
    """

    portshow_aggregated_df = portshow_aggregated_df.copy()
    # extract AreaID from PortID address
    portshow_aggregated_df['Virtual_Channel'] = portshow_aggregated_df.Connected_portId.str.slice(start=2, stop=4)
    # convert string AreaID to integer with base 16 (hexadicimal)
    # vc id defined as remainder of AreaID division by four plus 2
    portshow_aggregated_df.Virtual_Channel = portshow_aggregated_df.Virtual_Channel.apply(lambda x: int(x, 16)%4 + 2)
    # add VC to vc_id
    portshow_aggregated_df.Virtual_Channel = 'VC' + portshow_aggregated_df.Virtual_Channel.astype('str')

    return portshow_aggregated_df


def lib_name_correction(series):
    """
    Function avoid usage of HBA Host_Name value from FDMI DataFrame 
    for libraries Device_Host_Name in portshowaggregated DataFrame
    """

    # libraries mask
    mask_lib = series['deviceType'] == 'LIB'
    # correct device name for libraries
    if mask_lib:
        return series['Device_Name']
    else:
        return series['Device_Host_Name']


def find_msa_port(series):
    """
    Function to identify port name (A1-A4, B1-B4) for the MSA Storage
    based on second digit of PortWwn number (0-3 A1-A4, 4-7 B1-B4)
    """ 

    # port name based on second digit of portWwn
    port_num = int(series['PortName'][1])
    # Controller A ports
    if port_num in range(4):
        return 'A' + str(port_num+1)
    # Controller B ports
    elif port_num in range(4,8):
        return 'B' + str(port_num-3)
        
    return series['Device_Port']