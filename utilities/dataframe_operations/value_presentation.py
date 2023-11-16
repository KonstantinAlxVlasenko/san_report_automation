""" """

import re
import warnings

import numpy as np
import pandas as pd

from .dataframe_presentation import swap_columns


def concatenate_columns(df, summary_column: str, merge_columns: list, sep=', ', drop_merge_columns=True):
    """Function to concatenate values in several columns (merge_columns) into summary_column 
    as comma separated values"""
    
    df = df.copy()
    # create summary column if not exist
    if summary_column not in df.columns:
        df[summary_column] = np.nan

    df['separator_symbol'] = sep
    merge_columns = [column for column in merge_columns if column in df.columns]
    
    if not merge_columns:
        return df

    for column in merge_columns:
        # value in summary column is empty
        mask_summary_note_empty = df[summary_column].isna()
        # value in current column is empty
        mask_current_note_empty = df[column].isna()
        """if value in summary column is empty take value from column to add (if it's not nan)
        if value in column to add is empty take value from summary note column (if it's not nan)
        if both values are empty use nan value
        if both values in summary and current columns exist then cancatenate them"""
        df[summary_column] = np.select(
            [mask_summary_note_empty, mask_current_note_empty, mask_summary_note_empty & mask_current_note_empty],
            [df[column], df[summary_column], np.nan],
            default=df[summary_column] + df['separator_symbol'] + df[column])
    # drop merge_columns
    if drop_merge_columns:
        df.drop(columns=merge_columns, inplace=True)
    df.drop(columns='separator_symbol', inplace=True)
    return df


def merge_columns(df, summary_column: str, merge_columns: list, sep=', ', drop_merge_columns=True, sort_summary=False):
    """Function to concatenate values in several columns (merge_columns) into summary_column 
    with separator. If drop flag is True all merged columns except summary column are dropped"""
    
    df.reset_index(drop=True, inplace=True)
    merge_columns = [column for column in merge_columns if column in df.columns]
    if not merge_columns:
        return df
    df[summary_column] = df[merge_columns].stack().groupby(level=0).agg(sep.join)
    # drop merge_columns
    if drop_merge_columns:
        drop_columns = [column for column in merge_columns if column != summary_column]
        df.drop(columns=drop_columns, inplace=True)

    if sort_summary:
        sort_cell_values(df, summary_column, sep=sep)
    return df


def extract_values_from_column(df, extracted_column: str, pattern_column_lst: list):
    """Function to extract values from column. df and column are location from which data extracted.
    pattern_column_lst is the list containing tuples with two elements. Fist one is the regex pattern 
    with groups to be extracted. Second is the list of columns (groups) in which extracted values
    should be stored. Number of groups in pattern and column names have to be equal within tuple
    but may differ for different tuples.
    pattern_columns_lst = [(pattern1, ['ColumnA', 'ColumnB', 'ColumnC']), 
                           (pattern2, [''ColumnA', 'ColumnD'])]
    """
    
    for pattern, columns in pattern_column_lst:
        # pattern contains groups but str.cotains used to identify mask
        # supress warning message
        warnings.filterwarnings("ignore", 'This pattern has match groups')
        warnings.filterwarnings("ignore", 'This pattern is interpreted as a regular expression, and has match groups')
        mask = df[extracted_column].str.contains(pattern, regex=True, na=False)
        df.loc[mask, columns] = df.loc[mask, extracted_column].str.extract(pattern).values
    return df


def remove_substring(df, column, pattern):
    """Function to remove substring from the string in column values
    applying regex pattern"""
    
    # get column name with values after substring removal
    cleaned_column = column + '_cleaned'
    while cleaned_column in df.columns:
        cleaned_column += '_cleaned'

    # extract values without substring
    df = extract_values_from_column(df, extracted_column=column, 
                                    pattern_column_lst=[(pattern, [cleaned_column])])
    # copy values which don't contain removed substring
    df[cleaned_column].fillna(df[column], inplace=True)
    # swap locations of the original column and column with removed substring
    df = swap_columns(df, column, cleaned_column)
    # drop original column
    df.drop(columns=column, inplace=True)
    # rename column with removed substring to original column name
    df.rename(columns={cleaned_column: column}, inplace=True)
    return df


def explode_columns(df, *args, sep=', '):
    """Function to split values in columns defined in args on separator and
    and present it as rows (explode)"""
    
    common_exploded_df = pd.DataFrame()
    # filter columns which are present in df and containing values
    exploded_columns = [column for column in args if column in df and df[column].notna().any()]
    for column in exploded_columns:
        mask_notna = df[column].notna()
        current_exploded_df = df.loc[mask_notna].copy()
        current_exploded_df[column] = current_exploded_df[column].str.strip()
        # explode values in column as separate rows to Exploded_values column
        current_exploded_df = current_exploded_df.assign(Exploded_values=current_exploded_df[column].str.split(sep)).explode('Exploded_values')
        # tag exploded column name
        current_exploded_df['Exploded_column'] = column
        # drop columns containing values to explode to avoid appearance in common exploded DataFrame
        current_exploded_df.drop(columns=exploded_columns, inplace=True)
        common_exploded_df = pd.concat([common_exploded_df, current_exploded_df], ignore_index=True)
    return common_exploded_df


def tag_value_in_column(df, column, tag, binding_char='_'):
    """Function to tag all notna values in DataFrame column with tag"""

    tmp_column = 'tag_column'
    # change temp column name if column in DataFrame
    while tmp_column in df.columns:
        tmp_column += '_'

    mask_value_notna = df[column].notna()
    df[tmp_column] = tag + binding_char
    df[column] = df[column].astype('Int64', errors='ignore').astype('str', errors='ignore')
    df[column] = df.loc[mask_value_notna, tmp_column] + df.loc[mask_value_notna, column]
    df.drop(columns=tmp_column, inplace=True)
    return df


def  convert_wwn(df, wwn_columns: list):
    """Function to convert Wwnn and Wwnp to regular represenatation (lower case with colon delimeter)"""

    for wwn_column in wwn_columns:
        if wwn_column in df.columns and df[wwn_column].notna().any():
            mask_wwn = df[wwn_column].notna()
            df.loc[mask_wwn, wwn_column] = df.loc[mask_wwn, wwn_column].apply(lambda wwn: ':'.join(re.findall('..', wwn)))
            df[wwn_column] = df[wwn_column].str.lower()
    return df


def sort_cell_values(df, *args, sep=', '):
    """Function to sort values in cells of columns (args)"""
    
    for column in args:
        if df[column].notna().any():
            mask_notna = df[column].notna()
            df[column] = df.loc[mask_notna, column].str.split(sep).apply(sorted).str.join(sep).str.strip(',')
    return df


# auxiliary lambda function to combine two columns in DataFrame
# it combines to columns if both are not null and takes second if first is null
# str1 and str2 are strings before columns respectively (default is whitespace between columns)
wise_combine = lambda series, str1='', str2=' ': \
    str1 + str(series.iloc[0]) + str2 + str(series.iloc[1]) \
        if pd.notna(series.iloc[[0,1]]).all() else series.iloc[1]
