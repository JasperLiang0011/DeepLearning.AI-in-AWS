#!/usr/bin/env python
# coding: utf-8

# # ETL Operation Development via Pyspark

# In[ ]:


from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import *

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


# In[ ]:


# Input: extract data from RDS

datasource = glueContext.create_dynamic_frame.from_catalog(
    database="mel_housing_db",
    table_name="mel_housing_raw").toDF()


# In[ ]:


# Transformation: data cleaning

transformed_data = datasource.select(
    col("listing_id").cast("string"),
    regexp_replace(col("price"),"[^0-9]","").cast("double").alias("price_aud"),
    split(col("location"),",")[0].alias("suburb"),
    col("bedrooms").cast("integer")
).filter(lambda x:x["price_aud"] > 100000) # filter outlier
aggregated_data = transformed_data.groupBy("suburb").sum("price_aud")


# In[ ]:


# Output: Write to S3 processing layer
glueContext.write_dynamic_frame.from_options(
    frame=DynamicFrame.fromDF(transformed_data,glueContext,"result"),
    connection_type="s3",
    format="parquet",
    connection_options={
        "path":"s3://etl-bucket/stage/mel_housing",
        "partitionKeys":["suburb"]
    }
)


# # Redshift configuratiion

# In[ ]:


glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=DynamicFrame.fromDF(transformed_data,glueContext,"redshift_output"),
    catalog_connection="redshift_conn",
    connection_options={
        "dbtable":"house_listings",
        "databse":"analytics_db"
    }
)

