from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row, SparkSession
from helper import getFiles

def get_input(debug = False):
    if debug:
        return ("/home/richard/ABN/codc-interviews/dataset_one.csv", "/home/richard/ABN/codc-interviews/dataset_two.csv", "United Kingdom; Netherlands")
    return getFiles()

def remove_personal_data(database_path: str):
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv(database_path, header=True)
    return df.select("id", "email", "country")

def filter_countries(df, input_countries):
    if input_countries != '':
        df = df.where(df.country.isin(input_countries.split("; ")))
    return df

if __name__ == '__main__':
    database_path_1, database_path_2, input_countries = get_input(True)
    df_email = remove_personal_data(database_path_1)
    df = filter_countries(df_email, input_countries)
    df.show()