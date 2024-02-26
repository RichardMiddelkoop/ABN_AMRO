from datetime import datetime, date
import pandas as pd
from pyspark.sql import Row, SparkSession
from helper import getFiles

def get_input(debug = False):
    if debug:
        return ("/home/richard/ABN/codc-interviews/dataset_one.csv", "/home/richard/ABN/codc-interviews/dataset_two.csv", "United Kingdom; Netherlands")
    return getFiles()



if __name__ == '__main__':
    database_path_1, database_path_2, input_countries = get_input(True)
    print(database_path_1, database_path_2, input_countries)
    