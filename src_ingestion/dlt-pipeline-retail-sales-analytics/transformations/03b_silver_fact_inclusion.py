import dlt
from pyspark.sql import functions as F

@dlt.table(comment="Fact table for sales combining dimensions and deriving measures")
def fact_sales():
    #dimensions
    stores = dlt.read("silver_store_dim").filter(F.col("is_current") == True)
    transactions = dlt.read("silver_transactions_dim")
    train = dlt.read("silver_train_dim")
    oil = dlt.read("silver_oil_dim")
    holidays = dlt.read("silver_holidays_dim")

    # Join train data (sales) with dimensions
    fact_df = (
        train.alias("t")
        .join(stores.select("store_nbr", "type", "cluster", "city", "state").alias("s"),on="store_nbr", how="left")
        .join(transactions.alias("tr"), on=["store_nbr", "date"], how="left")
        .join(oil.alias("o"), on="date", how="left")
        .join(holidays.alias("h"), on="date", how="left")
        .select(
            F.col("t.id").alias("sale_id"),
            F.col("t.date"),
            F.col("t.store_nbr"),
            F.col("s.type").alias("store_type"),
            F.col("s.cluster"),
            F.col("s.city"),
            F.col("s.state"),
            F.col("t.unit_sales"),
            F.col("t.on_promotion"),
            F.col("tr.transactions"),
            F.col("o.oil_price"),
            F.col("h.type").alias("holiday_type"),
            F.col("h.locale"),
            F.col("h.locale_name"),
            F.col("h.description"),
            F.col("h.transferred")
        )
    )

    return fact_df
