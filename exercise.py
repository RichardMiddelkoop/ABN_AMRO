import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from helper import getFiles

def get_input(debug = False):
    if debug:
        return ("/home/richard/ABN/codc-interviews/dataset_one.csv", "/home/richard/ABN/codc-interviews/dataset_two.csv", "United Kingdom; Netherlands")
    return getFiles()

def read_csv(file_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(file_path, header=True)
    return df

def filter_column_by_string(df, col_name, string, seperator = ", "):
    return df.where(col(col_name).isin(string.split(seperator)))

def select_columns_from_df(df ,*args):
    return df.select(*args)

def rename_column_from_dict(df, dict_of_col_names):
    for old_col_pattern, new_col in dict_of_col_names.items():
        regex = re.compile(old_col_pattern)
        renamed_cols = [col_name if not regex.match(col_name) else new_col for col_name in df.columns]
        df = df.toDF(*renamed_cols)
    return df

if __name__ == '__main__':
    # Get user input
    database_path_1, database_path_2, input_countries = get_input(True)

    # Data transformation
    df1 = select_columns_from_df(read_csv(database_path_1), "id", "email", "country")
    df1 = filter_column_by_string(df1, "country", input_countries, seperator="; ")
    df2 = select_columns_from_df(read_csv(database_path_2),"id", "btc_a","cc_t")
    df = df1.join(df2, df1.id == df2.id, "left").select(df1.id, "email", "country", "btc_a", "cc_t")
    df = rename_column_from_dict(df, {"id": "client_identifier", "btc_a":"bitcoin_address", "cc_t":"credit_card_type"})
    df.write.csv(os.getcwd() + "/client_data")