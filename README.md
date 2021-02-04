# Data-Engineer-Programming-Test
# Sr. Data Engineer Programming Test

Suponga que se presenta la necesidad de ingestar archivos planos de distintos aplicativos origen, realizarles pequeñas transformaciones (mayoritariamente de formato) y guardarlo en formato Apache Parquet en HDFS.

Dada esta necesidad se pide generar una solución utilizando Pyspark o Spark Scala que contemple las siguientes características:

1.- Debe tener la flexibilidad suficiente para procesar archivos de texto ancho fijo y delimitados.

2.- Como input debe tomar un archivo de configuración que contenga los siguientes parámetros:

	a.- El path de origen del archivo a ser ingestado;
	b.- El path destino donde se escribirá ese archivo;
	c.- La metadata necesaria para procesar el archivo crudo (e.g. delimitador, header, columnas, anchos de columnas);
	d.- La metadata necesaria para generar el archivo en parquet que crea necesarias (e.g. particiones, cantidad de archivos, compresión);
	e.- Permitir el agregado de columnas al data frame con valores predefinidos.

Se solicita que lo desarrollado sea capaz de ser ejecutado en cualquier computadora de forma local, de ser posible, con la menor cantidad de dependencias.

### Sets de prueba
Se proveen dos archivos para realizar *pruebas*.

nyse_2012.csv, delimitado por comas, con cabecera. El parquet de salida debe estar particionado por una columna extra, llamada partition_date y cuyo valor viene dado por parámetro.

nyse_2012_ts.csv, de ancho fijo, sin cabecera y con las siguientes longitudes de columnas:

|columna|tipo de dato|longitud|
| ------------- | ------------- | ------------- |
|stock|string|6|
|transaction_date|timestamp|24|
|open_price|float|7|
|close_price|float|7|
|max_price|float|7|
|min_price|float|7|
|variation|float|7|

El parquet de salida debe estar particionado por la columna stock.


Pregunta:
Qué peculiaridades tiene el modo de escritura Overwrite, en data frames particionados, para versiones de Spark previas a 2.3? Qué cambió a partir de dicha versión?

### Criterios de evaluación
Tener en cuenta:
* Calidad del código desarrollado.
* Mantenibilidad.
* Claridad y facilidad de entendimiento.
* Performance y escalabilidad.
* Pragmatismo.

(Los criterios de evaluación son guías para brindar mayor claridad al candidato en términos de cómo su trabajo será calificado, esta evaluación se centrará en dichos conceptos pero no necesariamente se limitará a los mismos)

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
