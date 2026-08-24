from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
    IntegerType,
    ArrayType,
)


properties_schema = StructType(
    [
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("updated", LongType(), True),
        StructField("tz", StringType(), True),
        StructField("url", StringType(), True),
        StructField("detail", StringType(), True),
        StructField("felt", IntegerType(), True),
        StructField("cdi", DoubleType(), True),
        StructField("mmi", DoubleType(), True),
        StructField("alert", StringType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("net", StringType(), True),
        StructField("code", StringType(), True),
        StructField("ids", StringType(), True),
        StructField("sources", StringType(), True),
        StructField("types", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("dmin", DoubleType(), True),
        StructField("rms", DoubleType(), True),
        StructField("gap", IntegerType(), True),
        StructField("magType", StringType(), True),
        StructField("type", StringType(), True),
        StructField("title", StringType(), True),
    ]
)
geometry_schema = StructType(
    [StructField("coordinates", ArrayType(DoubleType()), True)]
)

feature_schema = StructType(
    [
        StructField("id", StringType()),
        StructField("properties", properties_schema),
        StructField("geometry", geometry_schema),
    ]
)

catalog = spark.conf.get("catalog")

volume_path = f"/Volumes/{catalog}/bronze/earthquake_data"


@dp.view(name="earthquake_data_vw")
def earthquake_data_temp():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(volume_path)
    )
    df_parsed = df.withColumn(
        "parsed_data", from_json(col("features"), ArrayType(feature_schema))
    )
    df_exploded = df_parsed.select(explode(col("parsed_data")).alias("feature"))

    df_select = df_exploded.select(
        col("feature.id").alias("id"),
        col("feature.properties.mag").alias("mag"),
        col("feature.properties.place").alias("place"),
        col("feature.properties.time").alias("time"),
        col("feature.properties.updated").alias("updated"),
        col("feature.properties.tz").alias("tz"),
        col("feature.properties.url").alias("url"),
        col("feature.properties.detail").alias("detail"),
        col("feature.properties.felt").alias("felt"),
        col("feature.properties.cdi").alias("cdi"),
        col("feature.properties.mmi").alias("mmi"),
        col("feature.properties.alert").alias("alert"),
        col("feature.properties.status").alias("status"),
        col("feature.properties.tsunami").alias("tsunami"),
        col("feature.properties.sig").alias("sig"),
        col("feature.properties.net").alias("net"),
        col("feature.properties.code").alias("code"),
        col("feature.properties.ids").alias("ids"),
        col("feature.properties.sources").alias("sources"),
        col("feature.properties.types").alias("types"),
        col("feature.properties.nst").alias("nst"),
        col("feature.properties.dmin").alias("dmin"),
        col("feature.properties.rms").alias("rms"),
        col("feature.properties.gap").alias("gap"),
        col("feature.properties.magType").alias("magType"),
        col("feature.properties.type").alias("type"),
        col("feature.properties.title").alias("title"),
        col("feature.geometry.coordinates").alias("coordinates"),
    )
    df_final = df_select.withColumn(
        "time", from_unixtime(col("time") / 1000).cast("timestamp")
    ).withColumn("_load_timestamp", current_timestamp())

    return df_final


dp.create_streaming_table(name="earthquake_data")

dp.create_auto_cdc_flow(
    target="earthquake_data",
    source="earthquake_data_vw",
    keys=["id"],
    sequence_by="_load_timestamp",
    stored_as_scd_type="1"
)
    

