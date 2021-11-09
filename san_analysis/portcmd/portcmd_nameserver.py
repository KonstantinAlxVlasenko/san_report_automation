"""
Module to retrieve storage, host, HBA information from Name Server service data.
Auxiliary to analysis_portcmd module.
"""


import warnings

import numpy as np
import pandas as pd

from .portcmd_nameserver_split import nsshow_symb_split

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
# import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop

# from common_operations_dataframe import dataframe_fillna


def nsshow_analysis_main(nsshow_df, nscamshow_df, nsshow_dedicated_df, fdmi_df, fabric_labels_df, re_pattern_lst):

    # label DataFrames
    nsshow_labeled_df, nscamshow_labeled_df, fdmi_labeled_df = fabric_labels(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df)
    # local Name Server (NS) Device_type (Initiatir, Target) information fillna 
    nsshow_labeled_df = device_type_fillna(nsshow_labeled_df, nscamshow_labeled_df, nsshow_dedicated_df)
    # remove unnecessary symbols in  PortSymb and NodeSymb columns in NameServer DataFrame
    nsshow_join_df = nsshow_clean(nsshow_labeled_df, re_pattern_lst)
    # split up PortSymb and NodeSymb columns in NameServer DataFrame
    nsshow_join_df, nsshow_unsplit_df = nsshow_symb_split(nsshow_join_df, re_pattern_lst)
    # fillna hba information
    nsshow_join_df = hba_fillna(nsshow_join_df, fdmi_labeled_df, re_pattern_lst)
    # fill Device_Host_Name for libs, storages, switches
    nsshow_join_df.Device_Host_Name.fillna(nsshow_join_df.Device_Name, inplace = True)

    return nsshow_join_df, nsshow_unsplit_df


def fabric_labels(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df):
    """Function to label nsshow_df, nscamshow_df, fdmi_df Dataframes with Fabric labels"""

    # create fabric labels DataFrame
    fabric_labels_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn', 'Fabric_name', 'Fabric_label']
    # fabric_labels_df = switch_params_aggregated_df.loc[:, fabric_labels_lst].copy()

    # copy DataFrames
    nsshow_labeled_df = nsshow_df.copy()
    nscamshow_labeled_df = nscamshow_df.copy()
    fdmi_labeled_df = fdmi_df.copy()

    df_lst = [nsshow_labeled_df, nscamshow_labeled_df, fdmi_labeled_df]

    for i, df in enumerate(df_lst):
        # rename switchname column for merging
        df.rename(columns = {'SwitchName': 'switchName'}, inplace = True)
        # label switches and update DataFrane in the list
        df_lst[i] = df.merge(fabric_labels_df, how = 'left', on = fabric_labels_lst[:5])

    return df_lst


def device_type_fillna(nsshow_labeled_df, nscamshow_labeled_df, nsshow_dedicated_df):
    """Function to fillna local Name Server (NS) Device_type (Initiatir, Target) information"""

    # drop duplcate WWNs in labeled nscamshow DataFrame with remote devices in the Name Server (NS) cache
    nscamshow_labeled_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_labeled_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # set Fabric_name, Fabric_label, PortName as index in order to perform fillna
    nscamshow_labeled_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_labeled_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # fillna empty device type cells
    nsshow_labeled_df[['Device_type', 'PortSymb', 'NodeSymb']] = \
        nsshow_labeled_df[['Device_type', 'PortSymb', 'NodeSymb']].fillna(nscamshow_labeled_df[['Device_type', 'PortSymb', 'NodeSymb']])

    # nsshow_join_df.Device_type.fillna(nscamshow_labeled_df.Device_type, inplace = True)
    # reset index
    nsshow_labeled_df.reset_index(inplace = True)

    if not nsshow_dedicated_df.empty:
        nsshow_labeled_df = dfop.dataframe_fillna(nsshow_labeled_df, nsshow_dedicated_df, 
                                                join_lst=['Pid', 'PortName'], filled_lst=['Device_type'])
    return nsshow_labeled_df


def nsshow_clean(nsshow_labeled_df, re_pattern_lst):
    """Function to clean (remove unnecessary symbols) PortSymb and NodeSymb columns in NameServer DataFrame"""

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    # columns of Name Server (NS) registered devices DataFrame
    nsshow_lst = [
        'Fabric_name', 'Fabric_label', 'configname', 'chassis_name', 'chassis_wwn', 
        'switchName', 'switchWwn', 'PortName', 'NodeName', 'PortSymb', 'NodeSymb', 'Device_type', 
        'LSAN', 'Slow_Drain_Device', 'Connected_through_AG', 'Real_device_behind_AG'
        ]

    nsshow_join_df = nsshow_labeled_df.loc[:, nsshow_lst]
    nsshow_join_df.fillna(np.nan, inplace=True)
    
    # nsshow_join_df['PortSymbOrig'] = nsshow_join_df['PortSymb']
    # nsshow_join_df['NodeSymbOrig'] = nsshow_join_df['NodeSymb']
    # columns to clean
    symb_columns = ['PortSymb', 'NodeSymb']

    # clean 'PortSymb' and 'NodeSymb' columns
    for symb_column in symb_columns:
        # symb_clean_comp removes brackets and quotation marks
        warnings.filterwarnings("ignore", 'This pattern has match groups')
        mask_symb_clean = nsshow_join_df[symb_column].str.contains(comp_dct[comp_keys[1]], regex=True, na=False)
        nsshow_join_df.loc[mask_symb_clean, symb_column] = nsshow_join_df.loc[mask_symb_clean, symb_column].str.extract(comp_dct[comp_keys[1]]).values
        # replace multiple whitespaces with single whitespace
        nsshow_join_df[symb_column].replace(to_replace = r' +', value = r' ', regex = True, inplace = True)
        # replace cells with one digit or whatespaces only with None value
        nsshow_join_df[symb_column].replace(to_replace = r'^\d$|^\s*$', value = np.nan, regex = True, inplace = True)
        # remove whitespace from the right and left side
        nsshow_join_df[symb_column] = nsshow_join_df[symb_column].str.strip()
        # hostname_clean_comp
        nsshow_join_df[symb_column].replace(to_replace = comp_dct[comp_keys[0]], value = np.nan, regex=True, inplace = True)
    
    return nsshow_join_df


def hba_fillna(nsshow_join_df, fdmi_labeled_df, re_pattern_lst):
    """Function to fillna values in HBA related columns of local Name Server (NS) DataFrame"""

    # regular expression patterns
    comp_keys, _, comp_dct = re_pattern_lst

    # fill empty cells in PortName column with values from WWNp column
    fdmi_labeled_df.PortName.fillna(fdmi_labeled_df.WWNp, inplace = True)
    # hostname_clean_comp
    fdmi_labeled_df.Host_Name = fdmi_labeled_df.Host_Name.replace(comp_dct[comp_keys[0]], np.nan, regex=True)
    # remove point at the end
    fdmi_labeled_df.Host_Name = fdmi_labeled_df.Host_Name.str.rstrip('.')
    # perenthesis_remove_comp remove parehthesis and brackets values
    for column in ['HBA_Driver', 'HBA_Firmware', 'Host_OS']:
        # pattern contains groups but str.cotains used to identify mask
        # supress warning message
        warnings.filterwarnings("ignore", 'This pattern has match groups')
        current_mask = fdmi_labeled_df[column].str.contains(comp_dct[comp_keys[18]], regex=True, na=False)
        fdmi_labeled_df.loc[current_mask, column] = \
            fdmi_labeled_df.loc[current_mask, column].str.extract(comp_dct[comp_keys[18]]).values
    # release_remove_comp
    warnings.filterwarnings("ignore", 'This pattern has match groups')
    mask_release = fdmi_labeled_df['Host_OS'].str.contains(comp_dct[comp_keys[24]], regex=True, na=False)
    fdmi_labeled_df.loc[mask_release, 'Host_OS'] = \
        fdmi_labeled_df.loc[mask_release, 'Host_OS'].str.extract(comp_dct[comp_keys[24]]).values

    # TO_REMOVE
    # fdmi_labeled_df.HBA_Driver = fdmi_labeled_df.HBA_Driver.str.extract(comp_dct[comp_keys[18]])
    # fdmi_labeled_df.HBA_Firmware = fdmi_labeled_df.HBA_Firmware.str.extract(comp_dct[comp_keys[18]])
    # fdmi_labeled_df.Host_OS = fdmi_labeled_df.Host_OS.str.extract(comp_dct[comp_keys[18]])
    # fdmi_labeled_df.Host_OS = fdmi_labeled_df.Host_OS.str.extract(comp_dct[comp_keys[24]])

    # drop duplcate WWNs in labeled fdmi DataFrame
    fdmi_labeled_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # set Fabric_name, Fabric_label, PortName as index in order to perform fillna
    fdmi_labeled_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_join_df.drop_duplicates(subset = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    nsshow_join_df.set_index(keys = ['Fabric_name', 'Fabric_label', 'PortName'], inplace = True)
    # fillna empty device type cells in nsshow_join DataFrame with values from fdmi DataFrame
    # HBA_Manufacturer, HBA_Model, HBA_Description,	HBA_Driver,	HBA_Firmware, Host_OS
    hba_columns_lst = nsshow_join_df.columns[-7:]
    nsshow_join_df[hba_columns_lst] = nsshow_join_df[hba_columns_lst].fillna(fdmi_labeled_df[hba_columns_lst])
    # reset index
    nsshow_join_df.reset_index(inplace = True)

    nsshow_join_df['Device_Host_Name'] = nsshow_join_df.Host_Name
    
    # TO REMOVE reloocated to main module
    # nsshow_join_df.Device_Host_Name.fillna(nsshow_join_df.Device_Name, inplace = True)

    return nsshow_join_df
