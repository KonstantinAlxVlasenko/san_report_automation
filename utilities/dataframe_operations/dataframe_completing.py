""" """

import numpy as np

def dataframe_join(left_df, right_df, columns_lst, columns_join_index=None):
    """
    Auxiliary function to add information from right DataFrame to left DataFrame
    for both parts of left DataFrame (with and w/o _Conneceted suffix columns).
    Function take as parameters left and right DataFrames, list with names in right DataFrame and 
    index. Join is performed on columns up to index 
    """

    right_join_df = right_df.loc[:, columns_lst].copy()
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


def dataframe_fillna(left_df, right_df, join_lst, filled_lst, remove_duplicates=True, drop_na=True):
    """
    Function to fill null values with values from another DataFrame with the same column names.
    Function accepts left Dataframe with null values, right DataFrame with filled values,
    list of columns join_lst used to join left and right DataFrames on,
    list of columns filled_lst where null values need to be filled. join_lst
    columns need to be present in left and right DataFrames. filled_lst must be present in right_df.
    If some columns from filled_lst missing in left_df it is added and the filled with values from right_df.
    If drop duplicate values in join columns of right DataFrame is not required pass remove_duplicates as False.
    If drop nan values in join columns in right DataFrame is not required pass drop_na as False.
    Function returns left DataFrame with filled null values in filled_lst columns 
    """

    # add missing columns to left_df from filled_lst if required
    left_df_columns_lst = left_df.columns.to_list()
    add_columns_lst = [column for column in filled_lst if column not in left_df_columns_lst]
    if add_columns_lst:
        left_df = left_df.reindex(columns = [*left_df_columns_lst, *add_columns_lst])

    if left_df[join_lst].empty:
        return left_df

    # cut off unnecessary columns from right DataFrame
    right_join_df = right_df.loc[:, join_lst + filled_lst].copy()
    # drop rows with null values in columns to join on
    if drop_na:
        right_join_df.dropna(subset=join_lst, inplace = True)
    # if required (deafult) drop duplicates values from join columns 
    # to avoid rows duplication in left DataDrame
    if remove_duplicates:
        right_join_df.drop_duplicates(subset=join_lst, inplace = True)
    # rename columns with filled values for right DataFrame
    filled_join_lst = [name+'_join' for name in filled_lst]
    right_join_df.rename(columns = dict(zip(filled_lst, filled_join_lst)), inplace = True)
    # left join left and right DataFrames on join_lst columns
    left_df = left_df.merge(right_join_df, how = 'left', on = join_lst)
    # for each columns pair (w/o (null values) and w _join prefix (filled values)
    for filled_name, filled_join_name in zip(filled_lst, filled_join_lst):
        # copy values from right DataFrame column to left DataFrame if left value ios null 
        
        # left_df[filled_name].fillna(left_df[filled_join_name], inplace = True)

        left_df[filled_name] = left_df[filled_name].fillna(left_df[filled_join_name])
        # drop column with _join prefix
        left_df.drop(columns = [filled_join_name], inplace = True)
    return left_df


def dataframe_fabric_labeling(df, switch_params_aggregated_df):
    """Function to label switches with fabric name and label"""

    # add Fabric labels from switch_params_aggregated_df Fataframe
    # columns labels reqiured for join operation
    switchparams_lst = ['configname', 'chassis_name', 'chassis_wwn', 'switchName', 'switchWwn',
                        'Fabric_name', 'Fabric_label']
    # create left DataFrame for join operation
    fabric_label_df = switch_params_aggregated_df.loc[:, switchparams_lst].copy()
    
    fabric_label_df.drop_duplicates(inplace=True)
    # portshow_aggregated_df and switchparams_join_df DataFrames join operation
    df_labeled = df.merge(fabric_label_df, how = 'left', on = switchparams_lst[:5])
    return df_labeled


def add_swclass_swtype_swweight(df, swclass_swtype_df, sw_columns):
    """Function to add switchClass, switchType to the df based on sw_columns"""

    # add switch class
    df = dataframe_fillna(df, swclass_swtype_df, join_lst=sw_columns, filled_lst=['switchClass', 'switchType'])
    # add switch class weight to sort switches
    add_swclass_weight(df)
    return df


def add_swclass_weight(swclass_df):
    """Function to add switch class weight column based on switch class column.
    Director has highest weight"""
    
    swclass_df['switchClass_weight'] = swclass_df['switchClass']
    switchClass_weight_dct = {'DIR': 1, 'ENTP': 2, 'MID': 3, 'ENTRY': 4, 'EMB': 5, 'EXT': 6}
    mask_assigned_switch_class = swclass_df['switchClass'].isin(switchClass_weight_dct.keys())
    swclass_df.loc[~mask_assigned_switch_class, 'switchClass_weight'] = np.nan
    swclass_df['switchClass_weight'].replace(switchClass_weight_dct, inplace=True)
    swclass_df['switchClass_weight'].fillna(7, inplace=True)