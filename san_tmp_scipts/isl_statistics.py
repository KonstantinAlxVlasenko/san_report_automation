# -*- coding: utf-8 -*-
"""
Created on Wed Feb  9 22:08:56 2022

@author: vlasenko
"""

import os
script_dir = r'C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\05.PYTHON\Projects\san_tmp_scipts'
# Change the current working directory
os.chdir(script_dir)
import general_cmd_module as dfop

import numpy as np

db_path = r"C:\Users\vlasenko\OneDrive - Hewlett Packard Enterprise\Documents\01.CUSTOMERS\MTS\SAN Assessment\NOV2021\mts_south\database_MTS_south"
db_file = r"MTS_south_analysis_database.db"

data_names = ['isl_statistics']

isl_statistics_df = dfop.read_database(db_path, db_file, *data_names)


isl_statistics_cp_df = isl_statistics_df.dropna(subset=['switchWwn']).copy()






def verify_group_symmetry(statistics_df, symmetry_grp, symmetry_columns, summary_column='Asymmetry_note'):
    """Function to verify if rows are symmetric in each symmetry_grp from
    symmetry_columns values point of view. Function adds Assysmetric_note to statistics_df.
    Column contains parameter name(s) for which symmetry condition is not fullfilled"""

    # drop invalid fabric labels
    mask_not_valid = statistics_df['Fabric_label'].isin(['x', '-'])
    # drop fabric summary rows (rows with empty Fabric_label)
    mask_fabric_label_notna = statistics_df['Fabric_label'].notna()
    statistics_cp_df = statistics_df.loc[~mask_not_valid & mask_fabric_label_notna].copy()
    
    # find number of unique values in connection_symmetry_columns
    symmetry_df = \
        statistics_cp_df.groupby(by=symmetry_grp)[symmetry_columns].agg('nunique')

    # temporary ineqaulity_notes columns for  connection_symmetry_columns
    symmetry_notes = [column + '_inequality' for column in symmetry_columns]
    for column, column_note in zip(symmetry_columns, symmetry_notes):
        symmetry_df[column_note] = np.nan
        # if fabrics are symmetric then number of unique values in groups should be equal to one 
        # mask_values_nonuniformity = symmetry_df[column] == 1
        mask_values_uniformity = symmetry_df[column].isin([0, 1])
        # use current column name as value in column_note for rows where number of unique values exceeds one 
        symmetry_df[column_note].where(mask_values_uniformity, column.lower(), inplace=True)
        
    # merge temporary ineqaulity_notes columns to Asymmetry_note column and drop temporary columns
    symmetry_df = dfop.concatenate_columns(symmetry_df, summary_column, 
                                                 merge_columns=symmetry_notes)
    # drop columns with quantity of unique values
    symmetry_df.drop(columns=symmetry_columns, inplace=True)
    # add Asymmetry_note column to statistics_df
    statistics_df = statistics_df.merge(symmetry_df, how='left', on=symmetry_grp)
    # clean notes for dropped fabrics
    if mask_not_valid.any():
        statistics_df.loc[mask_not_valid, summary_column] = np.nan
    return statistics_df



isl_statistics_cp_df = verify_group_symmetry(isl_statistics_cp_df, symmetry_grp=['Fabric_name','switchPair_id', 'Connected_switchPair_id'], 
                                            symmetry_columns=['Logical_link_quantity', 'Physical_link_quantity', 'Port_quantity', 'Bandwidth_Gbps'])

