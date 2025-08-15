import dlt
from pyspark.sql import functions as F

@dlt.table(table_properties={"quality": "gold"},comment="Gold flattened table: monthly KPIs per store including test data")
def flattened_gold_store_monthly_kpi():
    # Read silver tables
    train = dlt.read("silver_train")
    test = dlt.read("silver_test")
    stores = dlt.read("silver_stores").withColumnRenamed("type", "store_type")
    transactions = dlt.read("silver_transactions")
    oil = dlt.read("silver_oil")
    holidays = dlt.read("silver_holidays").withColumnRenamed("type", "holiday_type")

    # Combine train and test for KPI calculation
    sales = train.unionByName(test, allowMissingColumns=True)

    # Join with other dimensions
    df = sales.join(stores, on="store_nbr", how="left") \
              .join(transactions, on=["store_nbr", "date"], how="left") \
              .join(oil, on="date", how="left") \
              .join(holidays, on="date", how="left")

    # Extract year-month for aggregation
    df = df.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))

    # Aggregate metrics per store per month
    df_monthly = df.groupBy("store_nbr", "year_month", "store_type", "cluster", "city", "state") \
        .agg(
            F.sum("unit_sales").alias("monthly_sales"),
            F.avg("unit_sales").alias("avg_daily_sales"),
            F.max("unit_sales").alias("max_daily_sales"),
            F.sum("transactions").alias("monthly_transactions"),
            F.avg("transactions").alias("avg_daily_transactions"),
            F.avg("oil_price").alias("avg_oil_price"),
            F.sum(F.when(F.col("on_promotion")==True,1).otherwise(0)).alias("promo_days"),
            F.sum(F.when(F.col("holiday_type").isNotNull(),1).otherwise(0)).alias("holiday_days")
        )

    return df_monthly




# ---------------------------------------------------------
# Flattened Gold Table: Sales & Holiday Info
# ---------------------------------------------------------
@dlt.table(table_properties={"quality": "gold"},comment="Flattened gold table: sales and holiday info per store and month")
def flattened_gold_sales_holiday():
    # Read silver tables
    train = dlt.read("silver_train")
    stores = dlt.read("silver_stores").withColumnRenamed("type", "store_type")
    holidays = dlt.read("silver_holidays").withColumnRenamed("type", "holiday_type")

    # Join train with store and holiday info
    df = train.join(stores, on="store_nbr", how="left") \
              .join(holidays, on="date", how="left")

    # Extract year-month for aggregation
    df = df.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))

    # Aggregate sales and holiday metrics per store per month
    df_monthly = df.groupBy("store_nbr", "year_month", "store_type", "cluster", "city", "state") \
        .agg(
            F.sum("unit_sales").alias("monthly_sales"),
            F.avg("unit_sales").alias("avg_daily_sales"),
            F.max("unit_sales").alias("max_daily_sales"),
            F.sum(F.when(F.col("on_promotion")==True, 1).otherwise(0)).alias("promo_days"),
            F.sum(F.when(F.col("holiday_type").isNotNull(), 1).otherwise(0)).alias("holiday_days")
        )

    return df_monthly


# ---------------------------------------------------------
# Flattened Gold Table: Promotional Impact per Store
# ---------------------------------------------------------
@dlt.table(table_properties={"quality": "gold"},comment="Gold layer: promotional impact per store using flattened silver data")
def flattened_gold_promo_impact():
    # Read flattened silver train data
    sales = dlt.read("silver_train")

    # Aggregate promotional KPIs per store
    promo_kpis = sales.groupBy("store_nbr") \
        .agg(
            F.sum("unit_sales").alias("total_sales"),
            F.sum(F.when(F.col("on_promotion") == True, F.col("unit_sales")).otherwise(0)).alias("promo_sales"),
            (
                F.sum(F.when(F.col("on_promotion") == True, F.col("unit_sales")).otherwise(0)) / 
                F.sum("unit_sales")
            ).alias("promo_sales_ratio")
        ) \
        .orderBy("store_nbr")

    return promo_kpis
