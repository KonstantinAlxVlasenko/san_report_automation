"""Module to generate 'InterSwitch links', 'InterFabric links' customer report tables"""


import numpy as np
import pandas as pd

from common_operations_dataframe import dataframe_join, dataframe_segmentation
from common_operations_filesystem import load_data, save_data, save_xlsx_file
from common_operations_miscellaneous import force_extract_check, status_info, verify_data, verify_force_run


def isl_main(fabricshow_ag_labels_df, switch_params_aggregated_df, report_columns_usage_dct, 
    isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, report_data_lst):
    """Main function to create ISL and IFR report tables"""
    
   # report_data_lst contains information: 
   # customer_name, dir_report, dir to save obtained data, max_title, report_steps_dct
    *_, max_title, report_steps_dct = report_data_lst

    # names to save data obtained after current module execution
    data_names = ['isl_aggregated', 'Межкоммутаторные_соединения', 'Межфабричные_соединения']
    # service step information
    print(f'\n\n{report_steps_dct[data_names[0]][3]}\n')
    
    # loading data if were saved on previous iterations 
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    # pylint: disable=unbalanced-tuple-unpacking
    isl_aggregated_df, isl_report_df, ifl_report_df = data_lst

    # list of data to analyze from report_info table
    analyzed_data_names = ['isl', 'trunk', 'fcredge', 'sfpshow', 'portcfgshow', 
                            'chassis_parameters', 'switch_parameters', 'switchshow_ports', 
                            'maps_parameters', 'blade_interconnect', 'fabric_labels']

    # force run when any data from data_lst was not saved (file not found) or 
    # procedure execution explicitly requested for output data or data used during fn execution  
    force_run = verify_force_run(data_names, data_lst, report_steps_dct, 
                                            max_title, analyzed_data_names)
    if force_run:
        # current operation information string
        info = f'Generating ISL and IFL tables'
        print(info, end =" ")

        # get aggregated DataFrames
        isl_aggregated_df, fcredge_df = \
            isl_aggregated(fabricshow_ag_labels_df, switch_params_aggregated_df, 
            isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_ports_df)

        # after finish display status
        status_info('ok', max_title, len(info))      

        # partition aggregated DataFrame to required tables
        isl_report_df, = dataframe_segmentation(isl_aggregated_df, [data_names[1]], report_columns_usage_dct, max_title)
        # if no trunks in fabric drop trunk columns
        if trunk_df.empty:
            isl_report_df.drop(columns = ['Идентификатор транка', 'Deskew', 'Master'], inplace = True)
        # check if IFL table required
        if not fcredge_df.empty:
            ifl_report_df, = dataframe_segmentation(fcredge_df, [data_names[2]], report_columns_usage_dct, max_title)
        else:
            ifl_report_df = fcredge_df.copy()

        # create list with partitioned DataFrames
        data_lst = [isl_aggregated_df, isl_report_df, ifl_report_df]
        # saving fabric_statistics and fabric_statistics_summary DataFrames to csv file
        save_data(report_data_lst, data_names, *data_lst)
    # verify if loaded data is empty and replace information string with empty DataFrame
    else:
        isl_aggregated_df, isl_report_df, ifl_report_df = \
            verify_data(report_data_lst, data_names, *data_lst)
        data_lst = [isl_aggregated_df, isl_report_df, ifl_report_df]
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst)

    return isl_aggregated_df


def isl_aggregated(fabric_labels_df, switch_params_aggregated_df, 
    isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_df):
    """Function to create ISL aggregated DataFrame"""

    # remove unlabeled fabrics and slice DataFrame to drop unnecessary columns
    fabric_clean_df = fabric_clean(fabric_labels_df)
    # add switchnames to trunk and fcredge DataFrames
    isl_df, trunk_df, fcredge_df = switchname_join(fabric_clean_df, isl_df, trunk_df, fcredge_df)
    # outer join of isl and trunk DataFrames 
    isl_aggregated_df = trunk_join(isl_df, trunk_df)
    # adding switchshow port information to isl aggregated DataFrame
    isl_aggregated_df, fcredge_df = porttype_join(switchshow_df, isl_aggregated_df, fcredge_df)

    # adding sfp information to isl aggregated DataFrame
    isl_aggregated_df = sfp_join(sfpshow_df, isl_aggregated_df)

    # adding switch informatio to isl aggregated DataFrame
    isl_aggregated_df = switch_join(switch_params_aggregated_df, isl_aggregated_df)

    # adding portcfg information to isl aggregated DataFrame
    isl_aggregated_df = portcfg_join(portcfgshow_df, isl_aggregated_df)

    # adding fabric labels to isl aggregated DataFrame
    isl_aggregated_df, fcredge_df = fabriclabel_join(fabric_clean_df, isl_aggregated_df, fcredge_df)
    # calculate maximum link speed
    isl_aggregated_df = max_isl_speed(isl_aggregated_df)
    # calculate link attenuation
    isl_aggregated_df = attenuation_calc(isl_aggregated_df)
    # remove unlabeled switches from isl aggregated Dataframe
    isl_aggregated_df, fcredge_df = remove_empty_fabrics(isl_aggregated_df, fcredge_df)    
    
    return isl_aggregated_df, fcredge_df
    

def remove_empty_fabrics(isl_aggregated_df, fcredge_df):
    """
    Function to remove switches which are not part of research
    and sort required switches
    """

    # drop switches with empty fabric labels
    isl_aggregated_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
    # sort switches by switch names 
    isl_aggregated_df.sort_values(by=['switchType', 'chassis_name', 'switch_index', 'Fabric_name', 'Fabric_label'], \
        ascending=[False, True, True, True, True], inplace=True)
    # reset indexes
    isl_aggregated_df.reset_index(inplace=True, drop=True)
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
        fcredge_df.sort_values(by=['chassis_name', 'switch_index', 'Fabric_name', 'Fabric_label'], \
                                      ascending=[True, True, True, True], inplace=True)        
    
    return isl_aggregated_df, fcredge_df


def attenuation_calc(isl_aggregated_df):
    """Function to calculate ISL link signal attenuation"""

    # switch ports power values column names
    sfp_power_lst = ['RX_Power_dBm', 'TX_Power_dBm', 'RX_Power_uW', 'TX_Power_uW']
    # connected switch ports power values column names
    sfp_power_connected_lst = [ 'Connected_TX_Power_dBm', 'Connected_RX_Power_dBm', 'Connected_TX_Power_uW', 'Connected_RX_Power_uW', ]
    # type of attenuation column names
    sfp_attenuation_lst = ['In_Attenuation_dB', 'Out_Attenuation_dB', 'In_Attenuation_dB(lg)', 'Out_Attenuation_dB(lg)']
    
    # turn off division by zero check due to some power values might be 0
    # and mask_notzero apllied after division calculation
    np.seterr(divide = 'ignore')
    
    for attenuation_type, power, connected_power in zip(sfp_attenuation_lst, sfp_power_lst, sfp_power_connected_lst):
        # empty values mask
        mask_notna = isl_aggregated_df[[power, connected_power]].notna().all(1)
        # inifinite values mask
        mask_finite = np.isfinite(isl_aggregated_df[[power, connected_power]]).all(1)
        # zero values mask
        mask_notzero = (isl_aggregated_df[[power, connected_power]] != 0).all(1)
        
        # incoming signal attenuation calculated using dBm values
        if attenuation_type == 'In_Attenuation_dB':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            isl_aggregated_df[connected_power] - isl_aggregated_df[power]
        # outgoing signal attenuation calculated using dBm values
        elif attenuation_type == 'Out_Attenuation_dB':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            isl_aggregated_df[power] - isl_aggregated_df[connected_power]
        # incoming signal attenuation calculated using uW values
        elif attenuation_type == 'In_Attenuation_dB(lg)':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            round(10 * np.log10(isl_aggregated_df[connected_power]/(isl_aggregated_df[power])), 1)
        # outgoing signal attenuation calculated using uW values
        elif attenuation_type == 'Out_Attenuation_dB(lg)':
            isl_aggregated_df.loc[mask_notna & mask_finite & mask_notzero, attenuation_type] = \
            round(10 * np.log10(isl_aggregated_df[power].div(isl_aggregated_df[connected_power])), 1)
    # turn on division by zero check    
    np.seterr(divide = 'warn')
    
    return isl_aggregated_df
    

def max_isl_speed(isl_aggregated_df):
    """
    Function to evaluate maximum available port speed
    and check if ISL link operates on maximum speed.
    Maximum available link speed is calculated as minimum of next values 
    speed_chassis1, speed_chassis2, max_sfp_speed_switch1, max_sfp_speed_switch2
    """

    # columns to check speed
    speed_lst = ['Transceiver_speedMax', 'Connected_Transceiver_speedMax', 
                 'switch_speedMax', 'Connected_switch_speedMax']
    # minimum of four speed columns
    isl_aggregated_df['Link_speedMax'] = isl_aggregated_df.loc[:, speed_lst].min(axis = 1, skipna = False)
    # actual link speed
    isl_aggregated_df['Link_speedActual'] = isl_aggregated_df['speed'].str.extract(r'(\d+)').astype('float64')
    # mask to check speed in columns are not None values
    mask_speed = isl_aggregated_df[['Link_speedActual', 'Link_speedMax']].notna().all(1)
    # compare values in Actual and Maximum speed columns
    isl_aggregated_df.loc[mask_speed, 'Link_speedActualMax']  = pd.Series(np.where(isl_aggregated_df['Link_speedActual'].eq(isl_aggregated_df['Link_speedMax']), 'Да', 'Нет'))
    
    return isl_aggregated_df
    

def fabriclabel_join(fabric_clean_df, isl_aggregated_df, fcredge_df):
    """Adding Fabric labels and IP addresses to ISL aggregated and FCREdge DataFrame"""

    # column names list to slice fabric_clean DataFrame and join with isl_aggregated Dataframe 
    fabric_labels_lst = ['Fabric_name', 'Fabric_label', 'SwitchName', 'switchWwn']
    # addition fabric labels information to isl_aggregated and fcredge DataFrames 
    isl_aggregated_df = isl_aggregated_df.merge(fabric_clean_df.loc[:, fabric_labels_lst], 
                                                how = 'left', on = fabric_labels_lst[2:])
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df = fcredge_df.merge(fabric_clean_df.loc[:, fabric_labels_lst],
                                      how = 'left', on = fabric_labels_lst[2:])
    # column names list to slice fabric_clean DataFrame and join with isl_aggregated Dataframe
    switch_ip_lst = ['SwitchName', 'switchWwn', 'Enet_IP_Addr']
    # addition IP adddreses information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, fabric_clean_df,  switch_ip_lst, 2)

    return isl_aggregated_df, fcredge_df


def portcfg_join(portcfgshow_df, isl_aggregated_df):
    """Adding portcfg information to ISL aggregated DataFrame"""

    # column names list to slice portcfg DataFrame and join with isl_aggregated Dataframe
    portcfg_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Octet_Speed_Combo', 'Speed_Cfg',  'Trunk_Port',
                   'Long_Distance', 'VC_Link_Init', 'Locked_E_Port', 'ISL_R_RDY_Mode', 'RSCN_Suppressed',
                   'LOS_TOV_mode', 'QOS_Port', 'Rate_Limit', 'Credit_Recovery', 'Compression', 'Encryption', 
                   '10G/16G_FEC', 'Fault_Delay', 'TDZ_mode', 'Fill_Word(Current)', 'FEC']
    # addition portcfg port information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, portcfgshow_df, portcfg_lst, 4)
    
    return isl_aggregated_df


def switch_join(switch_params_aggregated_df, isl_sfp_connected_df):
    """Adding switch licenses, max speed, description"""

    # column names list to slice switch_params_aggregated DataFrame and join with isl_aggregated Dataframe
    switch_lst = ['SwitchName', 'switchWwn', 'switchType','licenses', 'switch_speedMax', 'HPE_modelName']   
    # addition switch parameters information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_sfp_connected_df, switch_params_aggregated_df, switch_lst, 2)
    # convert switchType column to float for later sorting
    isl_aggregated_df = isl_aggregated_df.astype(dtype = {'switchType': 'float64'}, errors = 'ignore')
    # check if Trunking lic present on both swithes in ISL link
    switch_trunking_dct = {'licenses' : 'Trunking_license', 'Connected_licenses' : 'Connected_Trunking_license'}
    for lic, trunking_lic in switch_trunking_dct.items():
        isl_aggregated_df[trunking_lic] = isl_aggregated_df.loc[isl_aggregated_df[lic].notnull(), lic].apply(lambda x: 'Trunking' in x)
        isl_aggregated_df[trunking_lic].replace(to_replace={True: 'Да', False: 'Нет'}, inplace = True)
       
    return isl_aggregated_df

    
def sfp_join(sfpshow_df, isl_aggregated_df):
    """Adding sfp infromation for both ports of the ISL link"""

    # column names list to slice sfphshow DataFrame and join with isl_aggregated Dataframe
    sfp_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Transceiver_PN', 'Wavelength_nm', 'Transceiver_mode',	'RX_Power_dBm',	'TX_Power_dBm',	'RX_Power_uW', 'TX_Power_uW']
    # convert sfp power data to float
    sfp_power_dct = {sfp_power: 'float64' for sfp_power in sfp_lst[7:]}
    sfpshow_df = sfpshow_df.astype(dtype = sfp_power_dct, errors = 'ignore')

    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, sfpshow_df, sfp_lst, 4)    
    #max Transceiver speed
    sfp_speed_dct = {
            'Transceiver_mode': 'Transceiver_speedMax', 
            'Connected_Transceiver_mode': 'Connected_Transceiver_speedMax'
            }
    # extract tranceivers speed and take max value
    for sfp, sfp_sp_max in sfp_speed_dct.items():
            # extract speed values
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp].str.extract(r'^([\d,]+)_(?:Gbps|MB)')
            # split string to create list of available speeds
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].str.split(',')
            # if list exist (speeds values was found) then choose maximum 
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].apply(lambda x: max([int(sp) for sp in x]) if isinstance(x, list) else np.nan)
            # if speed in Mb/s then convert to Gb/s
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].apply(lambda x: x/100 if x >= 100 else x)
    
    return isl_aggregated_df
    
    
def porttype_join(switchshow_df, isl_aggregated_df, fcredge_df):
    """Adding slot, port, speed and portType information for both ports of the ISL and IFR link"""

    # raname switchName column to allign with isl_aggregated DataFrame
    switchshow_join_df = switchshow_df.rename(columns={'switchName': 'SwitchName'})
    # column names list to slice switchshow DataFrame and join with isl_aggregated Dataframe
    porttype_lst = ['SwitchName', 'switchWwn', 'portIndex', 'slot', 'port', 'speed', 'portType']
    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, switchshow_join_df, porttype_lst, 3)   
    
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        # add portIndex to fcredge
        port_index_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'portIndex']
        switchshow_portindex_df = switchshow_join_df.loc[:, port_index_lst].copy()
        fcredge_df = fcredge_df.merge(switchshow_portindex_df, how = 'left', on= port_index_lst[:-1])
        # drop slot and port columns to avoid duplicate columns after dataframe function 
        fcredge_df.drop(columns = ['slot', 'port'], inplace = True)
        # addition switchshow port information to fcredge DataFrame
        fcredge_df = dataframe_join(fcredge_df, switchshow_join_df, porttype_lst, 3)
    
    return isl_aggregated_df, fcredge_df
    

def fabric_clean(fabricshow_ag_labels_df):
    """Function to prepare fabricshow_ag_labels DataFrame for join operation"""

    # create copy of fabricshow_ag_labels DataFrame
    fabric_clean_df = fabricshow_ag_labels_df.copy()
    # remove switches which are not part of research 
    # (was not labeled during Fabric labeling procedure)
    fabric_clean_df.dropna(subset=['Fabric_name', 'Fabric_label'], inplace = True)
    # reset fabrics DataFrame index after droping switches
    fabric_clean_df.reset_index(inplace=True, drop=True)
    # extract required columns
    fabric_clean_df = fabric_clean_df.loc[:, ['Fabric_name', 'Fabric_label', 'Worldwide_Name', 'Name', 'Enet_IP_Addr']]
    # rename columns as in switch_params DataFrame
    fabric_clean_df.rename(columns={'Worldwide_Name': 'switchWwn', 'Name': 'SwitchName'}, inplace=True)

    return fabric_clean_df


def switchname_join(fabric_clean_df, isl_df, trunk_df, fcredge_df):
    """Function to add switchnames to Trunk and FCREdge DataFrames"""

    # slicing fabric_clean DataFrame 
    switch_name_df = fabric_clean_df.loc[:, ['switchWwn', 'SwitchName']].copy()
    switch_name_df.rename(
            columns={'switchWwn': 'Connected_switchWwn', 'SwitchName': 'Connected_SwitchName'}, inplace=True)
    # adding switchnames to trunk_df
    trunk_df = trunk_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    # adding switchnames to isl_df
    isl_df = isl_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df = fcredge_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
        
    return isl_df, trunk_df, fcredge_df


def trunk_join(isl_df, trunk_df):
    """
    Join Trunk and ISL DataFrames
    Add switcNames to Trunk and FCREdge DataFrames
    """
    
    # convert numerical data in ISL and TRUNK DataFrames to float
    isl_df = isl_df.astype(dtype = 'float64', errors = 'ignore')    
    trunk_df  = trunk_df.astype(dtype = 'float64', errors = 'ignore')
    
    # List of columns DataFrames are joined on     
    join_lst = ['configname', 'chassis_name', 'switch_index', 'SwitchName',
                'switchWwn', 'switchRole', 'FabricID', 'FC_router', 'portIndex', 
                'Connected_portIndex', 'Connected_SwitchName',
                'Connected_switchWwn', 'Connected_switchDID']  

    # merge updated ISL and TRUNK DataFrames 
    isl_aggregated_df = trunk_df.merge(isl_df, how = 'outer', on = join_lst)

    return isl_aggregated_df
