import dlt
from pyspark.sql import functions as F

# Aggregates unit sales, transactions, promo sales.
# Keeps store attributes from silver_store_dim.
# Ready for dashboarding queries.

@dlt.table(table_properties={"quality": "gold"},comment="Gold layer: monthly KPIs per store including test data")
def gold_store_monthly_kpi():
    # Read train fact table
    train_fact = dlt.read("fact_sales")

    # Read test dimension
    test_fact = dlt.read("silver_test_dim")

    # Read store info
    stores = dlt.read("silver_stores")

    # Join test data with store info
    test_fact = (test_fact.join(stores, on="store_nbr", how="left")
                        .withColumn("unit_sales", F.lit(None).cast("double"))
                        .withColumn("transactions", F.lit(None).cast("int"))
                        .withColumn("oil_price", F.lit(None).cast("double")) 
                        .withColumnRenamed("type", "store_type")
    )

    # Union train and test
    combined_fact = train_fact.select("store_nbr", "date", "unit_sales", "transactions",
                                      "on_promotion", "store_type", "cluster", "city", "state", "oil_price") \
        .unionByName(
            test_fact.select("store_nbr", "date", "unit_sales", "transactions",
                             "on_promotion", "store_type", "cluster", "city", "state", "oil_price")
        )

    # Aggregate monthly KPIs per store
    return (
        combined_fact.withColumn("month", F.date_trunc("month", F.col("date")))
        .groupBy("store_nbr", "month", "store_type", "cluster", "city", "state")
        .agg(
            F.sum("unit_sales").alias("monthly_sales"),
            F.sum("transactions").alias("monthly_transactions"),
            F.avg("oil_price").alias("avg_monthly_oil_price"),
            F.sum(F.when(F.col("on_promotion") == True, F.col("unit_sales")).otherwise(0)).alias("promo_monthly_sales")
        )
        .orderBy("store_nbr", "month")
    )


# Holiday effect analysis on sales
@dlt.table(table_properties={"quality": "gold"},comment="Gold layer: aggregated sales during holidays")
def gold_sales_holiday():
    fact = dlt.read("fact_sales")
    return (
        fact.filter(F.col("holiday_type").isNotNull())
        .groupBy("holiday_type", "locale", "date")
        .agg(
            F.sum("unit_sales").alias("total_sales"),
            F.sum("transactions").alias("total_transactions"),
            F.avg("oil_price").alias("avg_oil_price")
        )
        .orderBy("date")
    )


# Promotional impact per store
@dlt.table(table_properties={"quality": "gold"},comment="Gold layer: promotional impact per store")
def gold_promo_impact():
    fact = dlt.read("fact_sales")

    return (
        fact.groupBy("store_nbr")
        .agg(
            F.sum("unit_sales").alias("total_sales"),
            F.sum(F.when(F.col("on_promotion") == True, F.col("unit_sales")).otherwise(0)).alias("promo_sales"),
            (
                F.sum(F.when(F.col("on_promotion") == True, F.col("unit_sales")).otherwise(0)) / 
                F.sum("unit_sales")
            ).alias("promo_sales_ratio")
        )
        .orderBy("store_nbr")
    )
