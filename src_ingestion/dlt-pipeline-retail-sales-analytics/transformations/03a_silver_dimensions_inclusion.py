import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T

# Disable incompatible view check for batch
spark.conf.set("pipelines.incompatibleViewCheck.enabled", "false")


########################
# STORES AS SCD-2 enabled and a Dimension
# Declare table for SCD-2
dlt.create_target_table("silver_store_dim")

@dlt.table(comment="Batch view of store changes")
def silver_store_dim_changes():
    return (
        dlt.read("bronze_stores")  # batch read
        .select("store_nbr", "type", "cluster", "city", "state")
        .withColumn("effective_start_date", F.current_date())
        .withColumn("effective_end_date", F.lit(None).cast(T.DateType()))
        .withColumn("is_current", F.lit(True))
    )

# Apply SCD-2 changes to the batch target
dlt.apply_changes(
    name="silver_store_dim_changes_apply",
    target="silver_store_dim",
    source="silver_store_dim_changes",
    keys=["store_nbr"],
    sequence_by="effective_start_date",
    stored_as_scd_type=2
)


########################
# TRAINS
# trains dimension (static)
# Format to proper datatypes
# Filter only records where sales > 0
@dlt.table(comment="trains dimension")
def silver_train_dim():
    return (
        dlt.read("bronze_train")
        .withColumn("id", F.col("id").cast("int"))
        .withColumn("date", F.to_date("date"))
        .withColumn("store_nbr", F.col("store_nbr").cast("int"))
        .withColumn("unit_sales", F.col("sales").cast("double"))
        .filter(F.col("unit_sales") >= 0)
        .withColumnRenamed("onpromotion", "on_promotion")
    )


# Format to proper datatypes
# Filter only records where price is Not null
@dlt.table(comment="Cleaned and transformed oil data into Silver Layer")
def silver_oil_dim():
    return(
        dlt.read("bronze_oil")
        .withColumn("date", F.to_date("date"))
        .withColumn("oil_price", F.col("dcoilwtico").cast("double"))
        .filter(F.col("oil_price").isNotNull())
    )


# Format to proper datatypes
@dlt.table(comment="Cleaned transactions data")
def silver_transactions_dim():
    return (
        dlt.read("bronze_transactions")
        .withColumn("date", F.to_date("date"))
        .withColumn("store_nbr", F.col("store_nbr").cast("int"))
        .withColumn("transactions", F.col("transactions").cast("int"))
    )


# Format to proper datatypes
@dlt.table(comment="Cleaned test data for forecast evaluation")
def silver_test_dim():
    return (
        dlt.read("bronze_test")
        .withColumn("date", F.to_date("date"))
        .withColumn("store_nbr", F.col("store_nbr").cast("int"))
        .withColumnRenamed("onpromotion", "on_promotion")
    )



# Format to proper datatypes
@dlt.table(comment="Cleaned and transformed holidays and events dimension")
def silver_holidays_dim():
    return (
        dlt.read("bronze_holidays")
        .withColumn("date", F.to_date("date"))
    )
































