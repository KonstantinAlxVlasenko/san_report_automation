
"""Module to create alias configuration summary statistics"""

import numpy as np
import pandas as pd
# from common_operations_dataframe import count_frequency, find_mean_max_min
from .zoning_statistics_aux_fn import active_vs_configured_ports, merge_df

import utilities.dataframe_operations as dfop
# import utilities.database_operations as dbop
# import utilities.data_structure_operations as dsop
# import utilities.module_execution as meop
# import utilities.servicefile_operations as sfop
# import utilities.filesystem_operations as fsop


def alias_dashboard(alias_aggregated_df, portshow_zoned_aggregated_df):
    """Function to count alias statistics"""

    zonemember_columns = ['Fabric_name', 'Fabric_label', 'zone_member']
    mask_zonemember_duplicates_free = alias_aggregated_df['zonemember_duplicates_free'].isna()
    alias_aggregated_duplicates_free_df = alias_aggregated_df.loc[~mask_zonemember_duplicates_free].copy()

    def count_alias_quantity(alias_aggregated_duplicates_free_df):
        """Function to  count total aliases quantity, aliases used in effective, 
        defined configurations and unused."""

        alias_aggregated_duplicates_free_df['Total_alias_quantity'] = 'Total_alias_quantity'
        mask_alias_unused = alias_aggregated_duplicates_free_df['zone_number_alias_used_in'] == 0
        alias_aggregated_duplicates_free_df['alias_unused'] = \
            np.where(mask_alias_unused, 'unused', pd.NA)
        alias_quantity_summary_df = dfop.count_frequency(alias_aggregated_duplicates_free_df, 
                                                        count_columns=['Total_alias_quantity', 'cfg_type', 'alias_unused'])
        return alias_quantity_summary_df


    def count_device_port_status(alias_aggregated_df):
        """Function to count device port status (local, remote_na, absent, etc) general statistics"""
        
        subset_columns = ['Fabric_name', 'Fabric_label', 'alias_member', 'PortName']
        ports_status_df = alias_aggregated_df.drop_duplicates(subset=subset_columns).copy()
        ports_status_summary_df = dfop.count_frequency(ports_status_df, count_columns=['Fabric_device_status'])
        return ports_status_summary_df


    def count_wwn_type(alias_aggregated_df):
        """Function to count wwn type and wwnn_unpacked general staistics 
        (for alias configuration in general but not for each alias particularly)"""

        subset_columns = ['Fabric_name', 'Fabric_label', 'alias_member']
        wwn_type_df = alias_aggregated_df.drop_duplicates(subset=subset_columns).copy()
        wwn_type_df['Wwnn_unpack'] = \
            wwn_type_df['Wwnn_unpack'].where(wwn_type_df['Wwnn_unpack'].isna(), 'Wwnn_unpack')
        wwn_type_summary = dfop.count_frequency(wwn_type_df, count_columns=['Wwn_type', 'Wwnn_unpack'])
        return wwn_type_summary


    def count_alias_with_wwnn(alias_aggregated_df):
        """Function to verify if there are wwnns, unpacked_wwnns, duplicated wwnps
        'inside' each alias and count number of these aliases"""

        # count aliases with wwnn
        alias_wwnn_df = \
            alias_aggregated_df.groupby(by=zonemember_columns, as_index=False)['Wwn_type'].agg(
                lambda x: 'alias_Wwnn' if x.isin(['Wwnn']).any() else np.nan)
        alias_wwnn_summary_df = dfop.count_frequency(alias_wwnn_df, count_columns=['Wwn_type'])
        # count aliases with unpacked wwnn
        alias_wwnn_unpacked_df = \
            alias_aggregated_df.groupby(by=zonemember_columns, as_index=False)['Wwnn_unpack'].agg(
                lambda x: 'alias_Wwnn_unpack' if x.notna().any() else np.nan)
        alias_wwnn_unpacked_summary_df = dfop.count_frequency(alias_wwnn_unpacked_df, count_columns=['Wwnn_unpack'])
        return alias_wwnn_summary_df, alias_wwnn_unpacked_summary_df


    def count_alias_with_duplicated_wwnp(alias_aggregated_df):
        """Function to count aliases with duplicated wwnp inside alias"""

        alias_wwnp_duplicated_df = \
            alias_aggregated_df.groupby(by=zonemember_columns, as_index=False)['wwnp_instance_number_per_alias'].agg(
                lambda x: 'alias_duplicated_wwnp' if (x > 1).any() else np.nan)
        alias_wwnp_duplicated_summary_df = \
            dfop.count_frequency(alias_wwnp_duplicated_df, count_columns=['wwnp_instance_number_per_alias'])
        return alias_wwnp_duplicated_summary_df

        
    def count_duplicated_alias(alias_aggregated_df):
        """Function to count duplicated aliases (aliases containing identical Wwnp in different aliases)"""

        alias_aggregated_cp_df = alias_aggregated_df.copy()
        mask_alias_duplicated = alias_aggregated_cp_df['alias_count'] > 1
        alias_aggregated_cp_df['alias_count'] = \
            np.where(mask_alias_duplicated, 'alias_duplicated', pd.NA)
        alias_duplicated_summary_df = dfop.count_frequency(alias_aggregated_cp_df, count_columns=['alias_count'])
        return alias_duplicated_summary_df


    def count_alias_port_zone_statistics(alias_aggregated_df):
        """Function to count mean, max, min values for defined ports, active ports number and 
        number zones alias applied in in aliases for each Fabric"""

        count_columns_lst = ['ports_per_alias', 'active_ports_per_alias', 'zone_number_alias_used_in']
        count_columns_dct = {k:k for k in count_columns_lst}
        alias_port_statistics_df = dfop.find_mean_max_min(alias_aggregated_duplicates_free_df, count_columns = count_columns_dct)
        return alias_port_statistics_df

    # count values frequencies in alias aggregated_df
    alias_quantity_summary_df = count_alias_quantity(alias_aggregated_duplicates_free_df)
    ports_status_summary_df = count_device_port_status(alias_aggregated_df)
    wwn_type_summary = count_wwn_type(alias_aggregated_df)
    # count total device ports vs device ports which have no aliases in Fabric
    active_vs_noalias_ports_summary_df = \
        active_vs_configured_ports(portshow_zoned_aggregated_df, configuration_type='alias')
    alias_wwnn_summary_df, alias_wwnn_unpacked_summary_df = count_alias_with_wwnn(alias_aggregated_df)
    alias_wwnp_duplicated_summary_df = count_alias_with_duplicated_wwnp(alias_aggregated_df)
    alias_duplicated_summary_df = count_duplicated_alias(alias_aggregated_df)
    alias_port_statistics_df = count_alias_port_zone_statistics(alias_aggregated_df)

    # merge all statistics DataFrames
    df_lst = [alias_quantity_summary_df, ports_status_summary_df, active_vs_noalias_ports_summary_df, wwn_type_summary, 
                alias_wwnn_summary_df, alias_wwnn_unpacked_summary_df, alias_wwnp_duplicated_summary_df, 
                alias_duplicated_summary_df, alias_port_statistics_df]
    alias_statistics_df = merge_df(df_lst, fillna_index=7)

    # move All row to the bottom
    mask_all = alias_statistics_df['Fabric_name'] == 'All'
    alias_statistics_all_df = alias_statistics_df.loc[mask_all].copy()
    alias_statistics_df = alias_statistics_df.loc[~mask_all].copy()
    alias_statistics_df = pd.concat([alias_statistics_df, alias_statistics_all_df])       

    return alias_statistics_df