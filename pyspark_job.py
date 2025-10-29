"""
PySpark ETL job for processing finance data
=========================================

This script contains the logic originally developed in a Jupyter notebook to
process finance flow data stored on Amazon S3 using Apache Spark within an
AWS Glue job.  The notebook-specific magic commands have been removed to
produce a standalone Python module that can be executed as part of a Glue
ETL job or tested locally.  Bucket names and paths should be reviewed and
parametrised according to your own environment before deployment.
"""

import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import when, col, concat_ws, lit, to_date, StringType, DoubleType
from pyspark.sql.types import DateType

# Create Spark and Glue contexts
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# -----------------------------------------------------------------------------
# Data ingestion
# -----------------------------------------------------------------------------

# Read various CSV files from the bronze layer on S3.  Replace these
# bucket names and prefixes with values appropriate for your environment.

flujo_mixto_df = spark.read.option("header", True).option("delimiter", ";").csv(
    "s3://<your-bronze-bucket>/Analytics/SAP/Flujo_de_caja/Flujo_Mixto/"
)
flujo_mixto_df = flujo_mixto_df.withColumnRenamed("Cta.mayor", "Cta mayor")

# Perform a series of transformations analogous to the original notebook
flujo_mixto_df = flujo_mixto_df \
    .withColumn('Cta mayor - Copia', col('Cta mayor')) \
    .withColumn("D/H - Copia", col("D/H")) \
    .withColumn("Nulo - Copia", col("Nulo"))

flujo_mixto_df = flujo_mixto_df.withColumn(
    "Nulo - Copia",
    when(col("Nulo - Copia").isNull(), "").otherwise(col("Nulo - Copia").cast("string"))
)

flujo_mixto_df = flujo_mixto_df.withColumn(
    "LLave FM",
    concat_ws(
        "",  # no separator
        col("Cta mayor - Copia").cast("string"),
        col("D/H - Copia").cast("string"),
        col("Nulo - Copia")
    )
)

# Continue with other datasets as in the original notebook
listado_empresas_df = spark.read.option("header", True).option("delimiter", ";").csv(
    "s3://<your-bronze-bucket>/Analytics/SAP/Flujo_de_caja/Listado_Empresas/"
)

dato_maestro_deudor_df = spark.read.option("header", True).option("delimiter", ";").csv(
    "s3://<your-bronze-bucket>/Analytics/SAP/Flujo_de_caja/Dato_Maestro_Deudor/"
)

dato_maestro_deudor_df = dato_maestro_deudor_df.withColumn("Cliente", col("Cliente").cast(DoubleType()))
dato_maestro_deudor_df = dato_maestro_deudor_df.withColumnRenamed("Nº ident.fis.1", "Nº ident fis 1")
dato_maestro_deudor_df = dato_maestro_deudor_df.withColumnRenamed("cod. Empresa", "cod Empresa")

# Additional cleaning and joining operations would follow here.  Refer to the
# original notebook for the complete transformation logic.

def main():
    """Entry point for running the ETL pipeline."""
    # Example summarisation step
    flujo_mixto_df.groupBy("Cta mayor - Copia").count().show()
    # Additional processing and writing to the silver/gold layers would go here
    print("Job completed")


if __name__ == "__main__":
    main()
