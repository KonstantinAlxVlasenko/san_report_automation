import numpy as np
import pandas as pd

from .dataframe_completing import dataframe_fillna

def count_bandwidth(df, speed_column: str, connection_grp_columns: list):
    """Function to count total bandwidth between each pair of switches 
    defined by connection_grp_columns (list of columns defining connection)"""
    
    # extract speed values
    df['Bandwidth_Gbps'] = df[speed_column].str.extract(r'(\d+)')
    df['Bandwidth_Gbps'] = df['Bandwidth_Gbps'].astype('int64', errors='ignore')
    # group links so all links between two switches are in one group
    # count total bandwidth for each group (connection)
    bandwidth_df = df.groupby(by=connection_grp_columns, dropna=False)['Bandwidth_Gbps'].sum()
    return bandwidth_df


def replace_wwnn(wwn_df, wwn_column: str, wwnn_wwnp_df, wwnn_wwnp_columns: list, fabric_columns: list=[]):
    """Function to replace wwnn in wwn_column (column with presumably mixed wwnn and wwnp values) 
    of wwn_df DataFrame with corresponding wwnp value if wwnn is present. wwnn_wwnp_df DataFrame contains strictly defined 
    wwnn and wwnp values in corresponding columns which passed as wwnn_wwnp_columns parameter.
    fabric_columns contains additional columns if required find wwnp for wwnn in certain fabric only."""
    
    wwnn_column, wwnp_column = wwnn_wwnp_columns
    join_columns = [*fabric_columns, wwnn_column]

    if wwnp_column in wwn_df.columns:
        wwn_df[wwnp_column] = np.nan

    # assume that all values in wwn_column are wwnns
    wwn_df[wwnn_column] = wwn_df[wwn_column]
    # find corresponding wwnp value from wwnn_wwnp_df for each presumed wwnn in wwn_df
    # rows with filled values in wwnp_column have confirmed wwnn value in  wwnn_column column of wwn_df
    wwn_df = dataframe_fillna(wwn_df, wwnn_wwnp_df, 
                                    join_lst=join_columns, 
                                    filled_lst=[wwnp_column], remove_duplicates=False)
    # when rows have empty values in wwnp_column mean wwn doesn't exist in fabric or it is wwnp
    wwn_df[wwnp_column].fillna(wwn_df[wwn_column], inplace=True)
    wwn_df.drop(columns=[wwnn_column], inplace=True)
    return wwn_df

