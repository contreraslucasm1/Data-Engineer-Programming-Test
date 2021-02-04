# Data-Engineer-Programming-Test

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
