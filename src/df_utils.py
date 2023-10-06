import polars as pl


################
# utility functions to use polars dataframes
################
# nicely defined function that takes care of a lot of heavy lifting when converting polars structs into columns and renaming accordingly.
def fmt_dict_cols(df: pl.DataFrame) -> pl.DataFrame:
    """
    formats dictionary cols, which are 'structs' in a polars df, into separate columns and renames accordingly.
    """
    for column in df.columns:
        if isinstance(df[column][0], dict):
            col_names = df[column][0].keys()
            # rename struct columns
            struct_df = df.select(
                pl.col(column).struct.rename_fields(
                    [f"{column}_{c}" for c in col_names]
                )
            )
            struct_df = struct_df.unnest(column)
            # add struct_df columns to df and
            df = df.with_columns(struct_df)
            # drop the df column
            df = df.drop(column)

    return df
