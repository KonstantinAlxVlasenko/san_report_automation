"""Module to add IFL links to ISL"""


import numpy as np
import pandas as pd

import utilities.dataframe_operations as dfop


def concat_isl_ifl(isl_df, trunk_df, fcredge_df, switchshow_df, switch_params_aggregated_df):
    """Function to add IFL links information to ISL"""

    # add portIndex to IFL
    fcredge_df = fcredge_portindex_join(switchshow_df, fcredge_df)
    # number IFL links
    fcredge_df = number_ifl(fcredge_df, trunk_df)
    # add switch information to fcredge_df to concatenate it with isl_df
    fcredge_cp_df = fcredge_to_isl_compliance(fcredge_df, switch_params_aggregated_df)
    # add ifl to isl
    isl_df['IFL_number'] = np.nan
    fcredge_cp_df = fcredge_cp_df.reindex(columns=[*isl_df.columns, 'Connected_FID']).copy()
    fcredge_cp_df.rename(columns={'Connected_FID': 'Connected_Edge_FID'}, inplace=True)
    isl_df = pd.concat([isl_df, fcredge_cp_df], ignore_index=True)
    return isl_df, fcredge_df


def fcredge_portindex_join(switchshow_df, fcredge_df):
    """Adding portIndex to IFL link"""

    # raname switchName column to allign with isl_aggregated DataFrame
    switchshow_join_df = switchshow_df.rename(columns={'switchName': 'SwitchName'}).copy()
    # if Fabric Routing is ON
    if not fcredge_df.empty:
        fcredge_df['slot'].fillna('0', inplace=True)
        # add portIndex to fcredge
        port_index_lst = ['configname', 'chassis_name', 'chassis_wwn', 
                          'SwitchName', 'switchWwn', 'slot', 'port', 'portIndex']
        switchshow_portindex_df = switchshow_join_df.loc[:, port_index_lst].copy()
        fcredge_df = fcredge_df.merge(switchshow_portindex_df, how = 'left', on= port_index_lst[:-1])
    return fcredge_df


def number_ifl(fcredge_df, trunk_df):
    """Function to Number IFL links. Links inside trunking group have same IFL number"""

    if not fcredge_df.empty:
        # add trunk related columns
        fcredge_df = dfop.dataframe_fillna(fcredge_df, trunk_df, join_lst=['switchWwn', 'portIndex'], 
                                    filled_lst=['Trunking_GroupNumber', 'Master'])

        # filter master links only to perform numbering (trunkless link considered to be master)
        mask_not_trunk_member = fcredge_df['Trunking_GroupNumber'].isna()
        fcredge_df.loc[mask_not_trunk_member, 'Master'] = 'MASTER'
        mask_trunk_master = fcredge_df['Master'].str.contains('master', case=False, na=False)
        fcredge_master_df = fcredge_df.loc[mask_trunk_master].copy()
        # master link numbering
        fcredge_master_df['IFL_number'] = fcredge_master_df.groupby(by=['switchWwn'])['Master'].rank(method="first", ascending=True)
        # number all IFL links which are part of trunk group based on master link IFL number
        fcredge_df = dfop.dataframe_fillna(fcredge_df, fcredge_master_df, join_lst=['switchWwn', 'Trunking_GroupNumber'], filled_lst=['IFL_number'])
        # copy IFLs links number for links which are not part of trunk group
        fcredge_df = dfop.dataframe_fillna(fcredge_df, fcredge_master_df, join_lst=['switchWwn', 'portIndex'], filled_lst=['IFL_number'])
        fcredge_df['IFL_number'] = fcredge_df['IFL_number'].astype('int64', errors='ignore')
    return fcredge_df


def fcredge_to_isl_compliance(fcredge_df, switch_params_aggregated_df):
    """Function to add columns to fcredge_df to comply with isl_df columns.
    Add switchRole, FC_router, Connected_switchDID columns"""

    fcredge_cp_df = fcredge_df.copy()
    if not fcredge_cp_df.empty:
        # add switchRole, FC_router, Connected_switchDID
        switch_params_aggregated_cp_df = switch_params_aggregated_df.copy()
        switch_params_aggregated_cp_df['FC_router'] = switch_params_aggregated_cp_df['FC_Router']
        switch_params_aggregated_cp_df['Connected_switchWwn'] = switch_params_aggregated_cp_df['switchWwn']
        switch_params_aggregated_cp_df['Connected_switchDID'] = switch_params_aggregated_cp_df['fabric.domain']
        fcredge_cp_df = dfop.dataframe_fillna(fcredge_cp_df, switch_params_aggregated_cp_df, join_lst=['switchWwn'], 
                                    filled_lst=['switchRole', 'FC_router'])
        fcredge_cp_df = dfop.dataframe_fillna(fcredge_cp_df, switch_params_aggregated_cp_df, join_lst=['Connected_switchWwn'], 
                                    filled_lst=['Connected_switchDID'], remove_duplicates=True, drop_na=True)
        # tag IFL links to avoid its numbering as ISL
        fcredge_cp_df['ISL_number'] = 'ifl'
        # rename columns to correspond to isl_df columns
        fcredge_cp_df.rename(columns={'BB_Fabric_ID': 'FabricID',  'Flags': 'Parameters'}, inplace=True)
    return fcredge_cp_df