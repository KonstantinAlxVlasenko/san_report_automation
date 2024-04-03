"""Module to retrieve storage, host, HBA information from Name Server service data.
Auxiliary to analysis_portcmd module."""


import warnings

import numpy as np

import utilities.dataframe_operations as dfop

from .nameserver_split import nsshow_symb_split


def nsshow_analysis(nsshow_df, nscamshow_df, nsshow_dedicated_df, fdmi_df, fabric_labels_df, pattern_dct):

    # label DataFrames
    nsshow_labeled_df, nscamshow_labeled_df, fdmi_labeled_df = fabric_labels(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df)
    # local Name Server (NS) Device_type (Initiatir, Target) information fillna 
    nsshow_labeled_df = device_type_fillna(nsshow_labeled_df, nscamshow_labeled_df, nsshow_dedicated_df)
    # remove unnecessary symbols in  PortSymb and NodeSymb columns in NameServer DataFrame
    nsshow_join_df = nsshow_clean(nsshow_labeled_df, pattern_dct)
    # split up PortSymb and NodeSymb columns in NameServer DataFrame
    nsshow_join_df, nsshow_unsplit_df = nsshow_symb_split(nsshow_join_df, pattern_dct)
    # fillna hba information
    nsshow_join_df = hba_fillna(nsshow_join_df, fdmi_labeled_df, pattern_dct)
    # fill Device_Host_Name for libs, storages, switches
    nsshow_join_df.Device_Host_Name.fillna(nsshow_join_df.Device_Name, inplace = True)
    return nsshow_join_df, nsshow_unsplit_df


def fabric_labels(nsshow_df, nscamshow_df, fdmi_df, fabric_labels_df):
    """Function to label nsshow_df, nscamshow_df, fdmi_df Dataframes with Fabric labels"""

    # create fabric labels DataFrame
    fabric_labels_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                            'switchName', 'switchWwn', 
                            'Fabric_name', 'Fabric_label']
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
        df_lst[i] = df.merge(fabric_labels_df, how='left', on=fabric_labels_lst[:5])
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


def nsshow_clean(nsshow_labeled_df, pattern_dct):
    """Function to clean (remove unnecessary symbols) PortSymb and NodeSymb columns in NameServer DataFrame"""

    # columns of Name Server (NS) registered devices DataFrame
    nsshow_lst = [
        'Fabric_name', 'Fabric_label', 'configname', 'chassis_name', 'chassis_wwn', 
        'switchName', 'switchWwn', 'PortName', 'NodeName', 'PortSymb', 'NodeSymb', 'Device_type', 
        'LSAN', 'Slow_Drain_Device', 'Connected_through_AG', 'Real_device_behind_AG', 
        'SCR', 'SCR_code', 'FC4s', 'FCoE', 'FC4 Features [FCP]', 'FC4 Features [FC-NVMe]'
        ]

    nsshow_join_df = nsshow_labeled_df.loc[:, nsshow_lst]
    nsshow_join_df.fillna(np.nan, inplace=True)
    
    # columns to clean
    symb_columns = ['PortSymb', 'NodeSymb']

    # clean 'PortSymb' and 'NodeSymb' columns
    for symb_column in symb_columns:
        if nsshow_join_df[symb_column].notna().any():
            # symb_clean_comp removes brackets and quotation marks
            warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
            mask_symb_clean = nsshow_join_df[symb_column].str.contains(pattern_dct['symb_clean'], regex=True, na=False)
            nsshow_join_df.loc[mask_symb_clean, symb_column] = \
                nsshow_join_df.loc[mask_symb_clean, symb_column].str.extract(pattern_dct['symb_clean']).values
            # replace multiple whitespaces with single whitespace
            nsshow_join_df[symb_column].replace(to_replace = r' +', value = r' ', regex = True, inplace = True)
            # replace cells with one digit or whatespaces only with None value
            nsshow_join_df[symb_column].replace(to_replace = r'^\d$|^\s*$', value = np.nan, regex = True, inplace = True)
            # remove whitespace from the right and left side
            if nsshow_join_df[symb_column].notna().any():
                nsshow_join_df[symb_column] = nsshow_join_df[symb_column].str.strip()
            # hostname_clean_comp
            nsshow_join_df[symb_column].replace(to_replace = pattern_dct['hostname_clean'], value = np.nan, regex=True, inplace = True)
    return nsshow_join_df


def hba_fillna(nsshow_join_df, fdmi_labeled_df, pattern_dct):
    """Function to fillna values in HBA related columns of local Name Server (NS) DataFrame"""

    # fill empty cells in PortName column with values from WWNp column
    fdmi_labeled_df.PortName.fillna(fdmi_labeled_df.WWNp, inplace = True)
    # hostname_clean_comp
    fdmi_labeled_df.Host_Name = fdmi_labeled_df.Host_Name.replace(pattern_dct['hostname_clean'], np.nan, regex=True)
    # remove point at the end
    fdmi_labeled_df.Host_Name = fdmi_labeled_df.Host_Name.str.rstrip('.')
    # perenthesis_remove_comp remove parehthesis and brackets values
    for column in ['HBA_Driver', 'HBA_Firmware', 'Host_OS']:
        # pattern contains groups but str.cotains used to identify mask
        # supress warning message
        # warnings.filterwarnings("ignore", "has match groups")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            current_mask = fdmi_labeled_df[column].str.contains(pattern_dct['perenthesis_remove'], regex=True, na=False)
            fdmi_labeled_df.loc[current_mask, column] = \
                fdmi_labeled_df.loc[current_mask, column].str.extract(pattern_dct['perenthesis_remove']).values
    # release_remove_comp
    # warnings.filterwarnings("ignore", "has match groups")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        mask_release = fdmi_labeled_df['Host_OS'].str.contains(pattern_dct['release_remove'], regex=True, na=False)
        fdmi_labeled_df.loc[mask_release, 'Host_OS'] = \
            fdmi_labeled_df.loc[mask_release, 'Host_OS'].str.extract(pattern_dct['release_remove']).values

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
    return nsshow_join_df
