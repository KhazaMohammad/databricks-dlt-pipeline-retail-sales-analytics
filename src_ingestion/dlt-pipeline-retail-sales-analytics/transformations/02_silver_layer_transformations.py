import dlt
from pyspark.sql import functions as F

# Format to proper datatypes
# Filter only records where sales > 0
@dlt.table(comment="Cleaned and transformed train sales data into Silver Layer")
def silver_train():
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
def silver_oil():
    return(
        dlt.read("bronze_oil")
        .withColumn("date", F.to_date("date"))
        .withColumn("oil_price", F.col("dcoilwtico").cast("double"))
        .filter(F.col("oil_price").isNotNull())
    )


# Format to proper datatypes
@dlt.table(comment="Cleaned store metadata")
def silver_stores():
    return (
        dlt.read("bronze_stores")
        .withColumn("store_nbr", F.col("store_nbr").cast("int"))
        .withColumn("city", F.initcap("city"))
        .withColumn("state", F.upper("state"))
    )


# Format to proper datatypes
@dlt.table(comment="Cleaned transactions data")
def silver_transactions():
    return (
        dlt.read("bronze_transactions")
        .withColumn("date", F.to_date("date"))
        .withColumn("store_nbr", F.col("store_nbr").cast("int"))
        .withColumn("transactions", F.col("transactions").cast("int"))
    )


# Format to proper datatypes
@dlt.table(comment="Cleaned test data for forecast evaluation")
def silver_test():
    return (
        dlt.read("bronze_test")
        .withColumn("date", F.to_date("date"))
        .withColumn("store_nbr", F.col("store_nbr").cast("int"))
        .withColumnRenamed("onpromotion", "on_promotion")
    )



# Format to proper datatypes
@dlt.table(comment="Cleaned test data for forecast evaluation")
def silver_holidays():
    return (
        dlt.read("bronze_holidays")
        .withColumn("date", F.to_date("date"))
    )


















