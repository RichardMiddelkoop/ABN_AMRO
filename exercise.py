import re
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from helper import getFiles
import logging
from logging.handlers import RotatingFileHandler

def get_input(debug = False):
    """
    Get input data for the program.

    If `debug` is True, returns mock input data.
    Otherwise, prompts the user to input file paths and countries.

    :param debug: Flag indicating whether to use mock input data (default is False).
    :type debug: bool
    :return: A tuple containing file paths and countries.
             If `debug` is True, returns mock data.
             Otherwise, prompts the user for input and returns the obtained values.
    :rtype: tuple
    """
    # Mock input data
    if debug:
        return ("/home/richard/ABN/codc-interviews/dataset_one.csv", 
                "/home/richard/ABN/codc-interviews/dataset_two.csv", 
                "United Kingdom; Netherlands")
    # Prompt user for input
    return getFiles()

def read_csv(file_path: str):
    """
    Read a CSV file into a Spark DataFrame.

    :param file_path: The path to the CSV file.
    :type file_path: str
    :return: A Spark DataFrame containing the data from the CSV file.
    :rtype: pyspark.sql.DataFrame
    """
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(file_path, header=True)
    return df

def filter_column_by_string(df, col_name, string, seperator = ", "):
    """
    Filter a DataFrame based on a string value contained in a specified column.

    :param df: The DataFrame to filter.
    :type df: pyspark.sql.DataFrame
    :param col_name: The name of the column to filter.
    :type col_name: str
    :param string: The string value to filter by.
    :type string: str
    :param separator: The separator used to split the string value (default is ", ").
    :type separator: str
    :return: A new DataFrame containing rows where the specified column matches the given string value.
    :rtype: pyspark.sql.DataFrame
    """
    return df.where(col(col_name).isin(string.split(seperator)))

def select_columns_from_df(df ,*args):
    """
    Select specific columns from a DataFrame.

    :param df: The DataFrame from which to select columns.
    :type df: pyspark.sql.DataFrame
    :param args: Variable number of column names to select.
    :type args: str
    :return: A new DataFrame containing only the selected columns.
    :rtype: pyspark.sql.DataFrame
    """
    return df.select(*args)

def rename_column_from_dict(df, dict_of_col_names):
    """
    Rename columns in a DataFrame based on a dictionary mapping old column names to new ones.

    :param df: The DataFrame to rename columns in.
    :type df: pyspark.sql.DataFrame
    :param dict_of_col_names: A dictionary mapping old column names to new ones.
    :type dict_of_col_names: dict
    :return: A new DataFrame with columns renamed according to the provided dictionary.
    :rtype: pyspark.sql.DataFrame
    """
    for old_col_pattern, new_col in dict_of_col_names.items():
        regex = re.compile(old_col_pattern)
        renamed_cols = [col_name if not regex.match(col_name) else new_col for col_name in df.columns]
        df = df.toDF(*renamed_cols)
    return df

if __name__ == '__main__':

    # Logging
    logging.basicConfig(
        handlers=[
            RotatingFileHandler(
            str(os.getcwd() + '/logs/file.log'),
            maxBytes=10240000,
            backupCount=5
            )
        ],
        level=logging.INFO,
        format='%(asctime)s %(levelname)s PID_%(process)d %(message)s'
    )
    


    # Get user input
    try:
        database_path_1, database_path_2, input_countries = get_input(False)
        logging.info(f'input recieved db1_path:{database_path_1} \t db2_path:{database_path_2} \t countries: {input_countries}')
    except Exception as e:
        logging.error(f'Input failed: {e}')

    # Data transformation
    df1 = select_columns_from_df(read_csv(database_path_1), "id", "email", "country")
    df1 = filter_column_by_string(df1, "country", input_countries, seperator="; ")
    df2 = select_columns_from_df(read_csv(database_path_2),"id", "btc_a","cc_t")
    df = df1.join(df2, df1.id == df2.id, "left").select(df1.id, "email", "country", "btc_a", "cc_t")
    df = rename_column_from_dict(df, {"id": "client_identifier", "btc_a":"bitcoin_address", "cc_t":"credit_card_type"})

    # Save user input
    try:
        df.write.mode("overwrite").csv(os.getcwd() + "/client_data")
        logging.info('Saved client data with succes')
    except Exception as e:
        logging.error(f'Saving data failed {e}')