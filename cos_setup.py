#!/usr/bin/python

"""
This program to take the month-wise backup from PostgreSQL
and store it to IBM COS.
"""
__author__ = "Anshita Saxena"
__copyright__ = "(c) Copyright IBM 2020"
__credits__ = ["BAT DMS IBM Team"]
__email__ = "anshsa33@in.ibm.com"
__status__ = "Development"

# Import the required libraries
# Import the sys library to parse the arguments
import sys

# Import the parsing library
import configparser

# Import the logging library
import logging
import logging.config

# Import pandas for parquet conversion
import pandas as pd

# Initialising the configparser object to parse the properties file
CONFIG = configparser.ConfigParser()

# Import COS library
import ibm_boto3
from ibm_botocore.client import Config, ClientError

# Import Postgresql library
import psycopg2

# Set the logging criteria for the generated logs
LOGFILENAME = '/root/postgresql-backup-to-ibmcos/logs/cos_setup.log'
logging.config.fileConfig(fname='/root/postgresql-backup-ibmcos/conf/log_config.conf',
                          defaults={'logfilename': LOGFILENAME},
                          disable_existing_loggers=False)

# Get the logger specified in the file
logger = logging.getLogger(__name__)

# Specify the months along with the end date
MONTHDAYS = {'jan': '31', 'feb': '28', 'mar': '31', 'apr': '30', 'may': '31',
             'jun': '30', 'jul': '31', 'aug': '31', 'sep': '30', 'oct': '31',
             'nov': '30', 'dec': '31'}
# Specify the month as number
MONTHNUMBER = {'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04', 'may': '05',
               'jun': '06', 'jul': '07', 'aug': '08', 'sep': '09', 'oct': '10',
               'nov': '11', 'dec': '12'}


def set_env(p_app_config_file):
    """
    :param p_app_config_file:
    :return p_user, p_passwd, p_host, p_port, p_db, p_table,
    cos_endpoint, cos_api_key_id, cos_auth_endpoint,
    cos_resource_crn, bucket_name, path, month, year:
    """
    p_user = None
    p_passwd = None
    p_host = None
    p_port = None
    p_db = None
    p_table = None
    cos_endpoint = None
    cos_api_key_id = None
    cos_auth_endpoint = None
    cos_resource_crn = None
    bucket_name = None
    path = None
    month = None
    year = None
    try:
        # Reading configuration parameters from .ini file.
        CONFIG.read(p_app_config_file)
        # PostgreSQL Username
        p_user = CONFIG['ApplicationParams']['p_user']

        # PostgreSQL Password
        p_passwd = CONFIG['ApplicationParams']['p_passwd']

        # PostgreSQL Hostname
        p_host = CONFIG['ApplicationParams']['p_host']

        # PostgreSQL Port Number
        p_port = CONFIG['ApplicationParams']['p_port']

        # PostgreSQL Database Name
        p_db = CONFIG['ApplicationParams']['p_db']

        # PostgreSQL Table Name
        p_table = CONFIG['ApplicationParams']['p_table']

        # IBM Cloud Object Storage (COS) endpoint
        cos_endpoint = CONFIG['ApplicationParams']['cos_endpoint']

        # IBM COS api key id
        cos_api_key_id = CONFIG['ApplicationParams']['cos_api_key_id']

        # IBM COS authentication endpoint
        cos_auth_endpoint = CONFIG['ApplicationParams']['cos_auth_endpoint']

        # IBM COS resource crn
        cos_resource_crn = CONFIG['ApplicationParams']['cos_resource_crn']

        # IBM COS bucket name
        bucket_name = CONFIG['ApplicationParams']['bucket_name']

        """
        Path of the server directory for transforming dataframes into
        Parquet format
        """
        path = CONFIG['ApplicationParams']['path']

        # Month for which the backup needs to be taken
        month = CONFIG['ApplicationParams']['month']

        # Year for which the backup needs to be taken
        year = CONFIG['ApplicationParams']['year']

    except Exception as e:
        raise Exception('Exception encountered in set_env() while '
                        'setting up application configuration parameters.')

    return \
        p_user, p_passwd, p_host, p_port, p_db, p_table, cos_endpoint,\
        cos_api_key_id, cos_auth_endpoint, cos_resource_crn, bucket_name,\
        path, month, year


def extract_command_params(arguments):
    """
    Passing arguments from command line.
    """

    # There should be only one argument
    if len(arguments) != 2:
        raise Exception('Illegal number of arguments. '
                        'Usage: python3 cos_setup.py parameter.ini')

    app_config_file = arguments[1]
    return app_config_file


def create_connection(
                    p_user, p_passwd, p_host, p_port, p_db, cos_api_key_id,
                    cos_resource_crn, cos_auth_endpoint, cos_endpoint):
    global db_connection, cur, cos, cos_cli
    """
    Function to create connections to PostgreSQL and IBM COS.
    :param p_user:
    :param p_passwd:
    :param p_host:
    :param p_port:
    :param p_db:
    :param cos_api_key_id:
    :param cos_resource_crn:
    :param cos_auth_endpoint:
    :param cos_endpoint:
    """
    # Create connection to PostgreSQL
    db_connection = psycopg2.connect(user=p_user, password=p_passwd,
                                     host=p_host, port=p_port,
                                     database=p_db)
    cur = db_connection.cursor()

    # Create resource
    cos = ibm_boto3.resource("s3",
                             ibm_api_key_id=cos_api_key_id,
                             ibm_service_instance_id=cos_resource_crn,
                             ibm_auth_endpoint=cos_auth_endpoint,
                             config=Config(signature_version="oauth"),
                             endpoint_url=cos_endpoint
                             )

    # Create client
    cos_cli = ibm_boto3.client("s3",
                               ibm_api_key_id=cos_api_key_id,
                               ibm_service_instance_id=cos_resource_crn,
                               ibm_auth_endpoint=cos_auth_endpoint,
                               config=Config(signature_version="oauth"),
                               endpoint_url=cos_endpoint
                               )


def postgresql_process(p_table, event_date, filename):
    """
    Function to copy the data into CSV files.
    :param p_user:
    :param p_passwd:
    :param p_host:
    :param p_port:
    :param p_db:
    :param p_table:
    """
    logging.info("PostgreSQL start :")
    try:
        logging.info("=====Starting PostgreSQL Operation=====")
        # Open a file into write mode
        f_data_store = open(filename, "w")

        # SQL to copy the data from database to csv file
        copy_sql = "COPY (select * from " + p_table + " where event_date='" \
                   + event_date + "') TO STDOUT WITH CSV DELIMITER ',' HEADER"
        print("Database connection successful")
        cur.copy_expert(sql=copy_sql, file=f_data_store)

        # Closing the file
        f_data_store.close()
        logging.info("=====Ending PostgreSQL Operation=====")

    except Exception as e:
        logging.error('Exception message in main thread::::')
        logging.error(e)
        raise Exception('Exception message in main thread::::', e)


def cos_insertion(bucket_name, filename, path, year, month):
    """
    Function to transform CSV into Parquet
    and call upload_large_file function to upload data to
    IBM COS in parallel.
    :param bucket_name:
    :param filename:
    :param path:
    :param year:
    :param month:
    """
    try:
        logging.info("=====Starting uploading file=====")
        # Create a dataframe holding a data
        df = pd.read_csv(filename, dtype='unicode')

        # Cast the datatype of specific column
        df['service_id'] = df['service_id'].astype(str)

        # Create a parquet file
        parquet_filename = filename[0:-4] + '.parquet'
        parquet_path = path + '/' + parquet_filename

        # Store the parquet file
        df.to_parquet(parquet_filename)
        month_parquet_filename = year + '/' + month + '/' + parquet_filename

        # Call the function to upload this parquet to IBM COS
        upload_large_file(
            cos_cli, bucket_name, month_parquet_filename, parquet_path)
        logging.info("=====File is uploaded=====")

    except Exception as e:
        logging.error('Exception message in main thread::::')
        logging.error(e)
        raise Exception('Exception message in main thread::::', e)


def upload_large_file(cos_cli, bucket_name, item_name, file_path):
    """
    Function to upload data to IBM COS buckets in parallel.
    :param cos_cli:
    :param bucket_name:
    :param item_name:
    :param file_path:
    """
    print("Starting large file upload for {0} to bucket: {1}".format(
                                                                item_name,
                                                                bucket_name))

    # set the chunk size to 5 MB
    part_size = 1024 * 1024 * 5

    # set threadhold to 5 MB
    file_threshold = 1024 * 1024 * 5

    # set the transfer threshold and chunk size in config settings
    transfer_config = ibm_boto3.s3.transfer.TransferConfig(
        multipart_threshold=file_threshold,
        multipart_chunksize=part_size
    )

    # create transfer manager
    transfer_mgr = ibm_boto3.s3.transfer.TransferManager(
                                                    cos_cli,
                                                    config=transfer_config)

    try:
        # initiate file upload
        future = transfer_mgr.upload(file_path, bucket_name, item_name)

        # wait for upload to complete
        future.result()

        print("Large file upload complete!")
    except Exception as e:
        print("Unable to complete large file upload: {0}".format(e))
    finally:
        transfer_mgr.shutdown()


def process_data(
        p_user, p_passwd, p_host, p_port, p_db, p_table, cos_endpoint,
        cos_api_key_id, cos_auth_endpoint, cos_resource_crn, bucket_name,
        path, month, year):
    """
    Function to take the backup of all dates for a month.
    :param p_user:
    :param p_passwd:
    :param p_host:
    :param p_port:
    :param p_db:
    :param p_table:
    :param cos_endpoint:
    :param cos_api_key_id:
    :param cos_auth_endpoint:
    :param cos_resource_crn:
    :param bucket_name:
    :param path:
    :param month:
    :param year:
    :return:
    """

    try:
        logging.info("=====Starting Process=====")
        # Fetching month number from dictionary
        month_number = MONTHNUMBER[month]

        # Fetching days in a particular month from dictionary
        days = MONTHDAYS[month]
        if month_number == '02':
            # Handling leap year month
            if int(year) % 4 == 0 and int(year) % 100 != 0 \
                    or int(year) % 400 == 0:
                days = '29'

        # Calling create connection function for PostgreSQL and IBM COS
        create_connection(
                    p_user, p_passwd, p_host, p_port, p_db, cos_api_key_id,
                    cos_resource_crn, cos_auth_endpoint, cos_endpoint)

        for i in range(1, int(days) + 1):
            if len(str(i)) == 1:
                date = '0' + str(i)
            else:
                date = str(i)

            # Creating a date format
            event_date = year + '-' + month_number + '-' + date
            filename = event_date + '.csv'
            print(filename)

            # Calling the function to fetch the data
            postgresql_process(p_table, event_date, filename)

            """
            Calling the function to transform the data into Parquet
            and store it into IBM COS
            """
            cos_insertion(bucket_name, filename, path, year, month)
        logging.info("=====Finishing Process=====")
        # Closing PostgreSQL database connection
        cur.close()

    except Exception as e:
        logging.error('Exception message in main thread::::')
        logging.error(e)
        raise Exception('Exception message in main thread::::', e)


def main():
    """
    Usage: python3 cos_setup.py parameter.ini
    :return:
    """

    try:
        logging.info("==== Processing Started ====")
        # Extract command line parameters
        p_app_config_file = extract_command_params(sys.argv)

        # Set environment
        p_user, p_passwd, p_host, p_port, p_db, p_table, cos_endpoint, \
            cos_api_key_id, cos_auth_endpoint, cos_resource_crn, bucket_name,\
            path, month, year = set_env(p_app_config_file)

        # Process Data
        logging.info(cos_endpoint)
        logging.info(cos_api_key_id)
        logging.info(cos_auth_endpoint)
        logging.info(cos_resource_crn)
        process_data(
                p_user, p_passwd, p_host, p_port, p_db, p_table, cos_endpoint,
                cos_api_key_id, cos_auth_endpoint, cos_resource_crn,
                bucket_name, path, month, year)
        logging.info("==== Processing Ended ====")

    except Exception as e:
        logging.error('Exception message in main thread::::')
        logging.error(e)
        raise Exception('Exception message in main thread::::', e)


if __name__ == '__main__':
    main()
    logging.shutdown()
