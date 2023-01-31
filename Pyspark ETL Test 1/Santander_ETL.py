from pyspark.sql import SparkSession
import json
import sys
import pyspark.sql.functions as fspark
from pyspark.sql.types import StructType, StructField, StringType, BinaryType, BooleanType, DecimalType, DoubleType, \
    FloatType, ByteType, IntegerType, LongType, ShortType


def init_spark():
    """Initialize the spark session and configure
        set('spark.sql.legacy.timeParserPolicy', 'LEGACY') is used to keep
        compatibility with Spark 2.4 date formatting.
        set('spark.sql.sources.partitionOverwriteMode', 'dynamic') is used to ensure safe overwrite mode.
    :return: spark session object
    """
    spark = SparkSession \
        .builder.appName("Data Engineer Programming Test") \
        .getOrCreate()
    spark.conf.set('spark.sql.legacy.timeParserPolicy', 'LEGACY')
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def _map_field_type(f_type):
    struct_fields = {
        "string": StringType(),
        "binary": BinaryType(),
        "boolean": BooleanType(),
        "date": StringType(),
        "timestamp": StringType(),
        "decimal": DecimalType(),
        "double": DoubleType(),
        "float": FloatType(),
        "number": StringType(),
        "byte": ByteType(),
        "integer": IntegerType(),
        "long": LongType(),
        "short": ShortType(),
        None: StringType()
    }
    return struct_fields.get(f_type, StringType())


def GenerateSchema(schema, extension):
    """Schema constructor from a string
    
    :param  schema: schema in json format provided by config file
            extension: extension of the file
    :return: schema contained in StructType Object
    """
    fields = []
    if extension != 'text':
        for col in schema:
            name = col.get("name")
            data_type = _map_field_type(col.get("type"))
            nullable = col.get("null", True)
            field = StructField(name, data_type, nullable)
            fields.append(field)
    else:
        fields.append(StructField('data_column', StringType(), True))
    return StructType(fields)


def split_colstxt(df, cols):
    """Function that splits fixed-size columns and casts to corresponding data type
    
    :param df: Spark data frame containing the texts to split
    :param cols: column schemes with name, data type and length
    :return: new Spark data frame with the columns divided according to the 'cols' parameter 
    """
    i = 1
    for c in cols:
        if (c.get('Datatype') != 'timestamp'):
            df = df.withColumn(c.get('name'),
                               fspark.substring(df.data_column, i, c.get('lenght'))
                               .cast(c.get('Datatype')))
        else:
            df = df.withColumn(c.get('name'),
                               fspark.to_timestamp
                               (fspark.substring(df.data_column, i, c.get('lenght')),
                                "dd/MM/yyyy HH:mm:ss"))
        i = i + c.get('lenght')
        for c in df.dtypes:
            if ((c[1]) == "string"):
                df = df.withColumn(c[0], fspark.trim(df[c[0]]))
    return df


def extract(spark, source, metadata):
    """Function that generates spark Dataframe according to the parameters of the config file
    
    :param spark: Spark Session
    :param source: metadata to build Spark dataframe provided in config file
    :return: Spark data frame created according to config parameters
    """
    print("Start of extraction")
    df = spark \
        .read \
        .format(metadata.get('extension')) \
        .option("delimiter", metadata.get('sep')) \
        .option("header", metadata.get('header')) \
        .option("inferschema", metadata.get('inferSchema')) \
        .option("encoding", metadata.get('encoding')) \
        .schema(GenerateSchema(metadata.get('columns'), metadata.get('extension'))) \
        .load(source)

    if metadata.get('extension') == 'text':
        df = split_colstxt(df, metadata.get('columns'))
        df = df.select([c for c in df.columns if c not in {'data_column'}])

    print("Loaded data")
    return df


def transform(df, cols):
    """Function that adds columns to the dataframe indicated in the config file
    
    :param df: Spark Dataframe
    :param cols: Predefined values to add as columns in the dataframe
    :return: Spark data frame with the columns added
    """
    print('Init Transform')
    for add in cols.keys():
        df = df.withColumn(add, fspark.lit(cols[add]))
        print("Column ", add, "was added to the data frame")
    print('Transform end')
    return df


def load(df, options):
    """Function that saves the data frame information in parquet format
    
    :param df: Spark Dataframe
    :param options:Parameter that defines the saving options provided by the configuration file
    """
    print("Data upload start")
    df.repartition(options.get("number_of_files")) \
        .write.mode(options.get('savemode')) \
        .option("compression", options.get("compression")) \
        .partitionBy(options.get('partition_by')) \
        .option("format", 'parquet') \
        .save(config.get('target'))
    print("Data upload finished")


######################################MAIN############################################

with open(sys.argv[1]) as json_data:
    config = json.load(json_data)

assert "source" in config.keys(), """
                    config file should have a 'source' key with the path needed to read a file
                    """
assert "target" in config.keys(), """
                    config file should have a 'target' key with the path needed to save the dataframe
                    """
assert "metadata" in config.keys(), """
                    config file should have a 'metadata' key with the options params needed to read the source
                    """
assert "parquet" in config.keys(), """
                    config file should have a 'parquet' key with the options params needed to save the dataframe
                    """

spark = init_spark()

df = extract(spark, config.get('source'), config.get('metadata'))
df.show()
df = transform(df, config.get('add_columns'))
df.show()
load(df, config.get('parquet'))
