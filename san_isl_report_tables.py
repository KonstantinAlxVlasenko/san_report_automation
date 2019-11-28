import pandas as pd
import numpy as np
from files_operations import status_info, load_data, save_data 
from files_operations import force_extract_check, save_xlsx_file, dataframe_import, dct_from_columns, columns_import

"""Module to generate 'InterSwitch links', 'InterFabric links' customer report tables"""


def isl_main(fabricshow_ag_labels_df, switch_params_aggregated_df, chassis_column_usage, 
    isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_ports_df, report_data_lst):
    """Main function to create ISL and IFR report tables"""
    
    # report_data_lst contains [customer_name, dir_report, dir_data_objects, max_title]
    
    print('\n\nSTEP 18. INTERSWITCH AND INTERFABRIC TABLES...\n')
    
    *_, max_title, report_steps_dct = report_data_lst
    # check if data already have been extracted
    data_names = ['Межкоммутаторные_соединения', 'Межфабричные_соединения']
    # loading data if were saved on previous iterations 
    data_lst = load_data(report_data_lst, *data_names)
    # unpacking DataFrames from the loaded list with data
    isl_report_df, ifl_report_df = data_lst

    # data force extract check 
    # if data have been calculated on previous iterations but force key is ON 
    # then data re-calculated again and saved
    # force key for each DataFrame
    force_extract_keys_lst = [report_steps_dct[data_name][1] for data_name in data_names]
    # check if data was loaded and not empty
    data_check = force_extract_check(data_names, data_lst, force_extract_keys_lst, max_title)  
    # flag if fabrics labels was forced to be changed 
    fabric_labels_change = True if report_steps_dct['fabric_labels'][1] else False
   
    # get aggregated DataFrames
    fabric_clean_df, isl_aggregated_df, fcredge_df = \
        isl_aggregated(fabricshow_ag_labels_df, switch_params_aggregated_df, chassis_column_usage, 
        isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_ports_df)
    # export DataFrame to excel if required
    save_xlsx_file(isl_aggregated_df, 'isl_aggregated', report_data_lst, report_type = 'service')

    # when no data saved or force extract flag is on or fabric labels have been changed than 
    # analyze extracted config data  
    if not all(data_check) or any(force_extract_keys_lst) or fabric_labels_change:
        # information string if fabric labels force changed was initiated
        # and statistics recounting required
        if fabric_labels_change and not any(force_extract_keys_lst) and all(data_check):
            info = f'ISL, IFL information force extract due to change in Fabrics labeling'
            print(info, end =" ")
            status_info('ok', max_title, len(info))

        # partition aggregated DataFrame to required tables
        isl_report_df, = dataframe_segmentation(isl_aggregated_df, [data_names[0]], chassis_column_usage, max_title)
        if not fcredge_df.empty:
            ifl_report_df, = dataframe_segmentation(fcredge_df, [data_names[1]], chassis_column_usage, max_title)
        else:
            ifl_report_df = fcredge_df.copy()

        # create list with partitioned DataFrames
        data_lst = [isl_report_df, ifl_report_df]
        # current operation information string
        info = f'Generating ISL and IFL tables'
        print(info, end =" ")   
        # after finish display status
        status_info('ok', max_title, len(info))
        # saving fabric_statistics and fabric_statistics_summary DataFrames to csv file
        save_data(report_data_lst, data_names, *data_lst)        
    # save data to service file if it's required
    for data_name, data_frame in zip(data_names, data_lst):
        save_xlsx_file(data_frame, data_name, report_data_lst, report_type = 'SAN_Assessment_tables')

    return fabric_clean_df, isl_report_df, ifl_report_df


def dataframe_segmentation(dataframe_to_segment_df, dataframes_to_create_lst, chassis_column_usage, max_title):
    """Function to split aggregated table to required DataFrames
    As parameters function get DataFrame to be partitioned and
    list of allocated DataFrames names 
    """
    # construct columns titles from data_names to use in dct_from_columns function
    tables_names_lst = [
        [data_name.rstrip('_report') + '_eng', data_name.rstrip('_report')+'_ru'] 
        for data_name in dataframes_to_create_lst
        ]      

    # dictionary used to rename DataFrame english columns names to russian
    data_columns_names_dct = {}
    # for each data element from data_names list import english and russian columns title
    # data_name is key and two lists with columns names are values for data_columns_names_dct
    for dataframe_name, eng_ru_columns in zip(dataframes_to_create_lst, tables_names_lst):
        data_columns_names_dct[dataframe_name]  = \
            dct_from_columns('customer_report', max_title, *eng_ru_columns, init_file = 'san_automation_info.xlsx')

    # construct english columns titles from tables_names_lst to use in columns_import function
    tables_names_eng_lst = [table_name_lst[0] for table_name_lst in tables_names_lst]
    # dictionary to extract required columns from aggregated DataFrame f_s_c_m_i
    data_columns_names_eng_dct = {}
    # for each data element from data_names list import english columns title
    for dataframe_name, df_eng_column in zip(dataframes_to_create_lst, tables_names_eng_lst):
        # dataframe_name is key and list with columns names is value for data_columns_names_eng_dct
        data_columns_names_eng_dct[dataframe_name] = columns_import('tables_columns_names', max_title, df_eng_column, init_file = 'san_automation_info.xlsx')
        # if no need to use chassis information in tables
        if not chassis_column_usage:
            if 'chassis_name' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('chassis_name')
            if 'chassis_wwn' in data_columns_names_eng_dct[dataframe_name]:
                data_columns_names_eng_dct[dataframe_name].remove('chassis_wwn')
            
    # list with partitioned DataFrames
    segmented_dataframes_lst = []
    for dataframe_name in dataframes_to_create_lst:
        # get required columns from aggregated DataFrame
        sliced_dataframe = dataframe_to_segment_df[data_columns_names_eng_dct[dataframe_name]].copy()

        # translate columns to russian
        sliced_dataframe.rename(columns = data_columns_names_dct[dataframe_name], inplace = True)
        # add partitioned DataFrame to list
        segmented_dataframes_lst.append(sliced_dataframe)

    return segmented_dataframes_lst

def isl_aggregated(fabric_labels_df, switch_params_aggregated_df, chassis_column_usage, 
    isl_df, trunk_df, fcredge_df, sfpshow_df, portcfgshow_df, switchshow_df):
    """Function to create ISL aggregated DataFrame"""
    # remove unlabeled fabrics and slice DataFrame to drop unnecessary columns
    fabric_clean_df = fabric_clean(fabric_labels_df)
    # add switchnames to trunk and fcredge DataFrames
    trunk_df, fcredge_df = switchname_join(fabric_clean_df, trunk_df, fcredge_df)
    # outer join of isl and trunk DataFrames 
    isl_aggregated_df = trunk_join(fabric_clean_df, isl_df, trunk_df)
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
    
    return fabric_clean_df, isl_aggregated_df, fcredge_df
    

def remove_empty_fabrics(isl_aggregated_df, fcredge_df):
    """Function to remove switches which are not part of research
    and sort required switches"""
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
    """Function to evaluate maximum available port speed
    and check if ISL link operates on maximum speed.
    Maximum available link speed is calculated as minimum of next values 
    speed_chassis1, speed_chassis2, max_sfp_speed_switch1, max_sfp_speed_switch2"""
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


def dataframe_join(left_df, right_df, columns_lst, columns_join_index = None):
    """Auxiliary function to join DataFrames
    Function take as parameters two DataFrames, 
    list with names in right DataFrame with, index which used to separate columns names which join operation performed on
    from columns with infromation to join 
    """
    right_join_df = right_df.loc[:, columns_lst]
    # left join on switch columns
    left_df = left_df.merge(right_join_df, how = 'left', on = columns_lst[:columns_join_index])
    # columns names for connected switch 
    columns_connected_lst = ['Connected_' + column_name for column_name in columns_lst]
    # dictionary to rename columns in right DataFrame
    rename_dct = dict(zip(columns_lst, columns_connected_lst))
    # rename columns in right DataFrame
    right_join_df.rename(columns = rename_dct, inplace = True)
    # left join connected switch columns
    left_df = left_df.merge(right_join_df, how = 'left', on = columns_connected_lst[:columns_join_index])
    
    return left_df


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
        isl_aggregated_df[trunking_lic] = isl_aggregated_df[isl_aggregated_df[lic].notnull()][lic].apply(lambda x: 'Trunking' in x)
        isl_aggregated_df[trunking_lic].replace(to_replace={True: 'Да', False: 'Нет'}, inplace = True)
       
    return isl_aggregated_df

    
def sfp_join(sfpshow_df, isl_aggregated_df):
    """Adding sfp infromation for both ports of the ISL link"""
    # column names list to slice sfphshow DataFrame and join with isl_aggregated Dataframe
    sfp_lst = ['SwitchName', 'switchWwn', 'slot', 'port', 'Vendor_PN', 'Wavelength_nm',	'Transceiver',	'RX_Power_dBm',	'TX_Power_dBm',	'RX_Power_uW', 'TX_Power_uW']
    # convert sfp power data to float
    sfp_power_dct = {sfp_power: 'float64' for sfp_power in sfp_lst[7:]}
    sfpshow_df = sfpshow_df.astype(dtype = sfp_power_dct, errors = 'ignore')
    
    # addition switchshow port information to isl_aggregated DataFrame
    isl_aggregated_df = dataframe_join(isl_aggregated_df, sfpshow_df, sfp_lst, 4)    
    #max Transceiver speed
    sfp_speed_dct = {
            'Transceiver': 'Transceiver_speedMax', 
            'Connected_Transceiver': 'Connected_Transceiver_speedMax'
            }
    # extract tranceivers speed and take max value
    for sfp, sfp_sp_max in sfp_speed_dct.items():
            # extract speed values
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp].str.extract(r'^([\d,]+)_Gbps')
            # split string to create list of available speeds
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].str.split(',')
            # if list exist (speeds values was found) then choose maximum 
            isl_aggregated_df[sfp_sp_max] = isl_aggregated_df[sfp_sp_max].apply(lambda x: max([int(sp) for sp in x]) if type(x) == list else np.nan)
    
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
        switchshow_portindex_df = switchshow_join_df.loc[:, port_index_lst]
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


def switchname_join(fabric_clean_df, trunk_df, fcredge_df):
    """Function to add switchnames to Trunk and FCREdge DataFrames"""
    # slicing fabric_clean DataFrame 
    switch_name_df = fabric_clean_df.loc[:, ['switchWwn', 'SwitchName']]
    switch_name_df.rename(
            columns={'switchWwn': 'Connected_switchWwn', 'SwitchName': 'Connected_SwitchName'}, inplace=True)
    # adding switchnames to trunk_df
    trunk_df = trunk_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df = fcredge_df.merge(switch_name_df, how = 'left', on='Connected_switchWwn')
        
    return trunk_df, fcredge_df

def trunk_join(fabric_clean_df, isl_df, trunk_df):
    """Join Trunk and ISL DataFrames
    Add switcNames to Trunk and FCREdge DataFrames"""
    # convert numerical data in ISL and TRUNK DataFrames to float
    isl_df = isl_df.astype(dtype = 'float64', errors = 'ignore')    
    if not trunk_df.empty:
        trunk_df  = trunk_df.astype(dtype = 'float64', errors = 'ignore')
    
    # List of columns DataFrames are joined on
    join_lst = ['configname',
                'chassis_name',
                'switch_index',
                'SwitchName',
                'switchWwn',
                'switchRole',
                'FabricID',
                'FC_router',
                'portIndex',
                'Connected_portIndex',
                'Connected_switchWwn',
                'Connected_switchDID',
                'Connected_SwitchName']        
    
    # merge updated ISL and TRUNK DataFrames 
    isl_aggregated_df = trunk_df.merge(isl_df, how = 'outer', on = join_lst)
        
    return isl_aggregated_df