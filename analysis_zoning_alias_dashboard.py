
"""Module to create alias configuration summary statistics"""

import numpy as np
import pandas as pd
from common_operations_dataframe import count_frequency, find_mean_max_min
from analysis_zoning_statistics_aux_fn import active_vs_configured_ports, merge_df

def alias_dashboard(alias_aggregated_df, portshow_zoned_aggregated_df):

    alias_aggregated_cp_df = alias_aggregated_df.copy()

    # duplicates_free_columns = ['cfg_type']

    mask_zonemember_duplicates_free = alias_aggregated_cp_df['zonemember_duplicates_free'].isna()
    alias_aggregated_duplicates_free_df = alias_aggregated_df.loc[~mask_zonemember_duplicates_free].copy()


    # count total aliases quantity, aliases used in effective, defined configurations and unused  
    alias_aggregated_duplicates_free_df['Total_alias_quantity'] = 'Total_alias_quantity'
    mask_alias_unused = alias_aggregated_duplicates_free_df['zone_number_alias_used_in'] == 0
    alias_aggregated_duplicates_free_df['alias_unused'] = \
        np.where(mask_alias_unused, 'unused', pd.NA)
    alias_quantity_summary_df = count_frequency(alias_aggregated_duplicates_free_df, count_columns=['Total_alias_quantity', 'cfg_type', 'alias_unused'])

    # count device port status (local, remote_na, absent, etc) general statistics
    subset_columns = ['Fabric_name', 'Fabric_label', 'alias_member', 'PortName']
    ports_status_df = alias_aggregated_df.drop_duplicates(subset=subset_columns).copy()
    ports_status_summary_df = count_frequency(ports_status_df, count_columns=['Fabric_device_status'])

    # count wwn type and wwnn_unpacked general staistics
    subset_columns = ['Fabric_name', 'Fabric_label', 'alias_member']
    wwn_type_df = alias_aggregated_df.drop_duplicates(subset=subset_columns).copy()
    wwn_type_df['Wwnn_unpack'] = \
        wwn_type_df['Wwnn_unpack'].where(wwn_type_df['Wwnn_unpack'].isna(), 'Wwnn_unpack')
    wwn_type_summary = count_frequency(wwn_type_df, count_columns=['Wwn_type', 'Wwnn_unpack'])

    # count total device ports vs device ports which have no aliases in Fabric
    active_vs_noalias_ports_summary_df = \
        active_vs_configured_ports(portshow_zoned_aggregated_df, configuration_type='alias')

    # verify if alias contains wwnn, unpacked_wwnn, duplixcated_wwnp
    zonemember_columns = ['Fabric_name', 'Fabric_label', 'zone_member']
    # count aliases with wwnn
    alias_wwnn_df = \
        alias_aggregated_df.groupby(by=zonemember_columns, as_index=False)['Wwn_type'].agg(lambda x: 'alias_Wwnn' if x.isin(['Wwnn']).any() else np.nan)
    alias_wwnn_summary_df = count_frequency(alias_wwnn_df, count_columns=['Wwn_type'])
    # count aliases with unpacked wwnn
    alias_wwnn_unpacked_df = \
        alias_aggregated_df.groupby(by=zonemember_columns, as_index=False)['Wwnn_unpack'].agg(lambda x: 'alias_Wwnn_unpack' if x.notna().any() else np.nan)
    alias_wwnn_unpacked_summary_df = count_frequency(alias_wwnn_unpacked_df, count_columns=['Wwnn_unpack'])
    # count aliases with duplicated wwnp inside alias
    alias_wwnp_duplicated_df = \
        alias_aggregated_df.groupby(by=zonemember_columns, as_index=False)['wwnp_instance_number_per_alias'].agg(lambda x: 'alias_duplicated_wwnp' if (x > 1).any() else np.nan)
    alias_wwnp_duplicated_summary_df = count_frequency(alias_wwnp_duplicated_df, count_columns=['wwnp_instance_number_per_alias'])

    # count duplicated aliases (aliases containing identical Wwnp in different aliases)
    mask_alias_duplicated = alias_aggregated_cp_df['alias_count'] > 1
    alias_aggregated_cp_df['alias_count'] = \
        np.where(mask_alias_duplicated, 'alias_duplicated', pd.NA)
    alias_duplicated_summary_df = count_frequency(alias_aggregated_cp_df, count_columns=['alias_count'])



    # count mean, max, min values for defined ports, active ports number and number zones alias applied in in aliases for each Fabric
    # defined and active ports number for each zonemember (alias) were calculated in advance
    # thus each alias should be counted only once
    count_columns_lst = ['ports_per_alias', 'active_ports_per_alias', 'zone_number_alias_used_in']
    count_columns_dct = {k:k for k in count_columns_lst}
    alias_port_statistics_df = find_mean_max_min(alias_aggregated_duplicates_free_df, count_columns = count_columns_dct)

    # merge all statistics DataFrames
    df_lst = [ 
                alias_quantity_summary_df, ports_status_summary_df, active_vs_noalias_ports_summary_df, wwn_type_summary, alias_wwnn_summary_df, 
                alias_wwnn_unpacked_summary_df, alias_wwnp_duplicated_summary_df, alias_duplicated_summary_df,
                alias_port_statistics_df]
    alias_statistics_df = merge_df(df_lst, fillna_index=7)    


    # TO_REMOVE replcaed by fn
    # alias_statistics_df = alias_quantity_summary_df.copy()
    # summary_lst = [ 
    #             ports_status_summary_df, active_vs_noalias_ports_summary_df, wwn_type_summary, alias_wwnn_summary_df, 
    #             alias_wwnn_unpacked_summary_df, alias_wwnp_duplicated_summary_df, alias_duplicated_summary_df,
    #             alias_port_statistics_df]
    # for i, df in enumerate(summary_lst):
    #     if not df.empty:
    #         alias_statistics_df = alias_statistics_df.merge(df, how='outer', on=['Fabric_name', 'Fabric_label'])
    #         if i == 6:
    #             alias_statistics_df.fillna(0, inplace=True)

    return alias_statistics_df

# def merge_df(df_lst, fillna_index, merge_on=['Fabric_name', 'Fabric_label']):
#     """Function to join DataFrames from df_lst. And fill nan values with zeroes in all
#     DataFrames till fillna_index (including). fillna_index is index of last DataFrame 
#     in df_lst required to fill nan values"""

#     merged_df = df_lst[0].copy()
#     for i, df in enumerate(df_lst):
#         if not df.empty:
#             merged_df = merged_df.merge(df, how='outer', on=merge_on)
#             if i == fillna_index:
#                 merged_df.fillna(0, inplace=True)

#     return merged_df