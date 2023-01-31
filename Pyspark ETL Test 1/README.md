# Data Engineer Programming Test

File Ingestion with Spark
This project aims to ingest flat files from different source applications, perform small transformations (mainly format-related), and store the output in Apache Parquet format in HDFS.

Usage
You can use PySpark or Spark Scala to generate the solution.

Features
The solution must have enough flexibility to process both fixed-width and delimited text files.
The solution must take a configuration file as input, which contains the following parameters:

    a. Source file path of the file to be ingested.
    b. Destination path where the file will be written.
    c. Metadata required to process the raw file (e.g. delimiter, header, columns, column widths).
    d. Metadata required to generate the parquet file (e.g. partitions, number of files, compression).
    e. Allow the addition of columns to the data frame with predefined values.

The solution must be able to run on any computer locally with as few dependencies as possible.

Test Sets
Two files are provided for testing purposes.

nyse_2012.csv is a comma-delimited file with a header. The output parquet must be partitioned by an extra column called partition_date, the value of which is given by the parameter.
nyse_2012_ts.csv is a fixed-width file without a header and with the following column lengths:

|column|data type|lenght|
| ------------- | ------------- | ------------- |
|stock|string|6|
|transaction_date|timestamp|24|
|open_price|float|7|
|close_price|float|7|
|max_price|float|7|
|min_price|float|7|
|variation|float|7|

The output parquet must be partitioned by the stock column.

Evaluation Criteria
Consider:

Quality of the code developed.
Maintainability.
Clarity and ease of understanding.
Performance and scalability.
Pragmatism.

# Solution
ETL script to process csv files and fixed-size column files with Spark.

The Script was developed in version'3.0.1' of spark.

Two sample configuration files are provided(**All keys are required for the correct execution of the script**)
```
config_csv.json
{
    "source": "input/nyse_2012.csv",
    "target": "output/csv/",
    "metadata": {
        "extension": "csv",
        "header": true,
        "sep": ",",
        "encoding": "utf-8",
        "inferSchema": true,
        "columns": [
                    {
                      "name": "stock",
                      "type": "string"
                    },
                    {
                      "name": "transaction_date",
                      "type": "date",
                      "dateFormat": "dd-MMM-yyyy"
                    },
                    {
                      "name": "open_price",
                      "type": "string"
                    },
                    {
                      "name": "close_price",
                      "type": "string"
                    },
                    {
                      "name": "max_price",
                      "type": "string"
                    },
                    {
                      "name": "min_price",
                      "type": "string"
                    },
                    {
                      "name": "variation",
                      "type": "string"
                    }
                  ]
    },
    "parquet": {
        "partition_by": "partition_date",
        "number_of_files": 4,
        "compression": "gzip",
        "savemode":"overwrite"
    },
    "add_columns": {
        "partition_date": 20200807
    }
}

```

```
config_txt.json
{
    "source": "input/nyse_2012_ts.txt",
    "target": "output/txt/",
    "metadata": {
        "extension": "text",
        "header": false,
        "encoding": "utf-8",
        "columns": [
            {
                "name": "stock",
                "Datatype": "string",
                "lenght": 6
            },
            {
                "name": "transaction_date",
                "Datatype": "timestamp",
                "lenght": 24
            },
            {
                "name": "open_price",
                "Datatype": "float",
                "lenght": 7
            },
            {
                "name": "close_price",
                "Datatype": "float",
                "lenght": 7
            },
            {
                "name": "max_price",
                "Datatype": "float",
                "lenght": 7
            },
            {
                "name": "min_price",
                "Datatype": "float",
                "lenght": 7
            },
            {
                "name": "variation",
                "Datatype": "float",
                "lenght": 7
            }
        ]
    },
    "parquet": {
        "partition_by": "stock",
        "number_of_files": 1,
        "compression": "gzip",
        "savemode": "overwrite"
    },
    "add_columns": {
    }
}
```

# RUN SCRIPT
To run the Script: spark-submit --deploy-mode client  Santander_ETL.py config_csv.json or spark-submit --deploy-mode client Santander_ETL.py config_txt.json

# Q&A
Question: What are the peculiarities of the Overwrite writing mode, in partitioned data frames, for Spark versions prior to 2.3? What changed from that version?

The mode overwrites mode, truncates all information contained in the specified path before executing the write instruction.
Starting with version 2.3, the dynamic writing mode was added, which allows only overwriting the partitions contained in the data frame to be saved, without deleting the previously stored partitions.

To activate this mode, the spark session must be configured as follows:

spark.conf.set ("spark.sql.sources.partitionOverwriteMode", "dynamic")
