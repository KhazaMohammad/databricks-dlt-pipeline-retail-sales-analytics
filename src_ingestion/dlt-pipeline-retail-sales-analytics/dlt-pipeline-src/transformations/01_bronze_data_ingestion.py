import dlt
from pyspark.sql.functions import *

# spark conf

#python variables
raw_ingestion_layer_path = spark.conf.get("raw_ingestion_layer_path")


# BRONZE LAYER INGESTION
# train file
@dlt.table(
    table_properties={"quality": "bronze"},
    comment="raw data ingestion into bronze layer for train file"
    )
def bronze_train():
    df_raw_train = (spark
              .read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(f"{raw_ingestion_layer_path}/train.csv")
              )
    return df_raw_train 



#test file
@dlt.table(comment="raw data ingestion into bronze layer for test file")
def bronze_test():
    df_raw_test = (spark
              .read
              .format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load(f"{raw_ingestion_layer_path}/test.csv")
              )
    return df_raw_test 



# ---- STORES ----
@dlt.table(comment="Raw store metadata")
def bronze_stores():
    df_raw_stores = (spark
                     .read
                     .format("csv")
                     .option("header", "true")
                     .option("inferSchema", "true")
                     .load(f"{raw_ingestion_layer_path}/stores.csv")
    )
    return df_raw_stores



# ---- OIL ----
@dlt.table(comment="Raw oil prices")
def bronze_oil():
   df_raw_oil =  (spark
                  .read
                  .format("csv")
                  .option("header", "true")
                  .option("inferSchema", "true")
                  .load(f"{raw_ingestion_layer_path}/oil.csv")
    )
   return df_raw_oil



# ---- HOLIDAYS ----
@dlt.table(comment="Raw holidays and events")
def bronze_holidays():
    df_raw_holidays = (spark
                       .read
                       .format("csv")
                       .option("header", "true")
                       .option("inferSchema", "true")
                       .load(f"{raw_ingestion_layer_path}/holidays_events.csv")
    )
    return df_raw_holidays



# ---- TRANSACTIONS ----
@dlt.table(comment="Raw transactions per store/day")
def bronze_transactions():
    df_raw_transactions =  (spark
                            .read
                            .format("csv")
                            .option("header", "true")
                            .option("inferSchema", "true")
                            .load(f"{raw_ingestion_layer_path}/transactions.csv")
    )
    return df_raw_transactions