"""Module to count values in DataFrame"""

import numpy as np
import pandas as pd


def sequential_equality_note(df, columns1: list, columns2: list, note_column: str):
    """Function to check if values in the list of columns1 and columns2 are sequentially equal
    for rows where all values are not na. note_column contains 'Yes' if corresponding values 
    are equal and 'No' if they aren't. Rows with any absent value from columns1 and column2
    have na in note_column"""
    
    # to compare more than two columns for sequantial equality they must have equal column names
    # for corresponding columns. two DataFrames sliced from df DataFrame.
    df1 = df[columns1].copy()
    df2 = df[columns2].copy()
    # rename column names in df2 to correspond names in df1
    rename_dct = {column2: column1 for column1, column2 in zip(columns1, columns2)}
    df2.rename(columns=rename_dct, inplace=True)
    # mask where values in corresponding columns are equal
    mask_equality = (df1 == df2).all(axis=1)
    # only rows with notna values for all columns from columns1 and columns2 lists are taken into account
    columns = columns1 + columns2
    mask_notna = df[columns].notna().all(axis=1)
    # check equality and fill note_column with 'Yes' or 'No'
    df[note_column] = np.select([mask_notna & mask_equality, mask_notna & ~mask_equality], ['Yes', 'No'], default=pd.NA)
    df.fillna(np.nan, inplace=True)
    return df


def threshold_exceed(df, value_column: str, threshold: float, result_column: str):
    """Function to check if value in value_column exceeds threshold.
    'Yes' or 'No' in result_column"""
    
    mask_value_notna = df[value_column].notna()
    mask_threshold_exceeded = df[value_column] >= threshold
    df[result_column] = np.select([mask_value_notna & mask_threshold_exceeded, 
                                   mask_value_notna & ~mask_threshold_exceeded], 
                                  ['Yes', 'No'], default=pd.NA)
    df.fillna(np.nan, inplace=True)
    return df


def verify_lic(df, lic_column: str, lic_name: str):
    """Function to check if lic_name is present on both connected switches (trunk capability)"""
    
    connected_lic_column = 'Connected_' + lic_column
    lic_columns_dct = {lic_column : lic_name + '_license', connected_lic_column : 'Connected_' + lic_name + '_license'}

    # verify lic installed on each switch 
    for licenses_column, verified_lic_column in lic_columns_dct.items():
        df[verified_lic_column] = \
            df.loc[df[licenses_column].notnull(), licenses_column].apply(lambda x: lic_name.lower() in x.lower())
        df[verified_lic_column].replace(to_replace={True: 'Yes', False: 'No'}, inplace = True)
        
    # verify lic installed on both switches
    lic_both_switches_column = lic_name + '_lic_both_switches'    
    df[lic_both_switches_column] = np.nan
    # masks license info present and license installed or not
    mask_lic_notna_both_sw = df[lic_columns_dct.values()].notna().all(axis=1)
    mask_all_yes = (df[lic_columns_dct.values()] == 'Yes').all(axis=1)
    mask_any_no = (df[lic_columns_dct.values()] == 'No').any(axis=1)
    
    df[lic_both_switches_column] = np.select(
        [mask_lic_notna_both_sw & mask_all_yes, mask_lic_notna_both_sw & mask_any_no],
        ['Yes', 'No'], default='Unknown')
    return df


def verify_max_link_speed(df):
    """
    Function to evaluate maximum available port speed
    and check if link operates on maximum speed.
    Maximum available link speed is calculated as minimum of next values 
    speed_chassis1, speed_chassis2, max_sfp_speed_switch1, max_sfp_speed_switch2
    """

    # columns to check speed
    speed_lst = ['Transceiver_speedMax', 'Connected_Transceiver_speedMax', 
                 'switch_speedMax', 'Connected_switch_speedMax']
    
    if pd.Series(speed_lst).isin(df.columns).all():
        # minimum of four speed columns
        mask_speed_notna = df[speed_lst].notna().all(axis=1)
        # minimum of four speed columns
        df.loc[mask_speed_notna, 'Link_speedMax'] = df.loc[mask_speed_notna, speed_lst].min(axis = 1, numeric_only=True)
        # actual link speed
        df['Link_speedActual'] = df['speed'].str.extract(r'(\d+)').astype('float64')
        # mask to check speed in columns are not None values
        mask_speed = df[['Link_speedActual', 'Link_speedMax']].notna().all(1)
        # compare values in Actual and Maximum speed columns
        df.loc[mask_speed, 'Link_speedActualMax']  = \
            pd.Series(np.where(df['Link_speedActual'].eq(df['Link_speedMax']), 'Yes', 'No'))  
    else:
        df['Link_speedMax'] = np.nan
    return df


