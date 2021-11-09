# postgresql-backup-ibmcos
<div align="justify">
This project collects the monthly data from PostgreSQL and store the backup in IBM COS (S3 buckets). This solution helps for storing backups on cheap storage IBM Cloud Object Storage.
</div>

## Description
<p align="justify">
This project is based on storing backups. IBM Cloud Object Storage is a service offered by IBM for storing and accessing unstructured data. The object storage service can be deployed on-premise, as part of IBM Cloud Platform offerings, or in hybrid form. PostgreSQL, also known as Postgres, is a free and open-source relational database management system emphasizing extensibility and SQL compliance.
</p>

<p align="justify">
We have used python, which connects to PostgreSQL, fetch the data in CSV, transform it into Parquet format for data compression, and store it into IBM COS (S3) buckets.
</p>

<p align="justify">
This solution takes the monthly backups of PostgreSQL, and store data into S3 buckets in Parquet format. We have used the appropriate tags that can distinguish data according to year and month. This helps in the proper partitioning of data in buckets to run analytics.
</div>

## Advantages
1. Dependency on SAAS automated snapshots will be reduced.
2. Gain full access to storage point, i.e., can restore the data belonging to any month or year. No need to rely on snapshot points.
3. IBM Cloud also provides to access this data via IBM SQL Query instance and remote Jupyter Notebook.

## Environment Details
This code is running on RHEL (Red Hat Enterprize Linux) server. The required packages are listed in the `requirements.txt` file. The command to install the required packages is given below:

`pip3 install -r requirements.txt`

PostgreSQL is hosted on IBM Cloud and running as software as a service (SAAS). It is a highly available database service. IBM Cloud Object Storage (COS) is a highly available instance that runs on the **Pay-as-you-use** model.

`conf` directory consists of two files:
1. `log_config.conf`: This configuration file holds the format for writing logs.
2. `parameter.ini`: This configuration file holds the parameter that needs to be configured, i.e., PostgreSQL connection details, IBM COS connection details.

`logs` directory holds the `cos_setup.log` file where the logs will be written.

## Working Details
The code runs every month starting to take the backup of the previous month. This code runs in Production. The running command for this code is:

`nohup python3 cos_setup.py conf/parameter.ini &`

<div align="justify">
nohup is a POSIX command which means "no hang up". Its purpose is to execute a command such that it ignores the HUP signal and therefore does not stop when the user logs out. Output that would normally go to the terminal goes to a file called nohup.out, if it has not already been redirected. Hence, the logs are collected at two places: defined log file which collects the logs in the formatted way and nohup.out which collected the terminal logs.
</div>

All connection details, schema name and table name are modified to maintain confidentiality.

## Highlights
1. Integration of PostgreSQL
2. Integration of IBM COS
3. Transformation from .csv to .parquet format using Pandas
4. Dependency of SAAS snapshots is removed
5. Utilization of Analytics directly on COS without restoration of data into PostgreSQL

## Demo Screenshots
IBM COS bucket after taking the PostgreSQL Backup:
![IBM COS Screenshot](https://github.com/Anshita1Saxena/postgresql-backup-ibmcos/blob/main/demo-image/IBM%20COS%20Screenshot.JPG)

IBM COS bucket having Parquet Files:
![IBM COS Screenshot Parquet](https://github.com/Anshita1Saxena/postgresql-backup-ibmcos/blob/main/demo-image/IBM%20COS%20Screenshot%20Parquet.JPG)
