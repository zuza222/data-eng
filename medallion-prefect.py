from prefect import task, flow
import random
import os
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

from common import base_path
BRONZE_ROOT = f"{base_path}/bronze"
SILVER_ROOT = f"{base_path}/silver"
GOLD_ROOT = f"{base_path}/gold"

# Create folders
for layer in ['bronze', 'silver', 'gold']:
    os.makedirs(f"{base_path}/{layer}", exist_ok=True)

spark_conf = (
    SparkConf()
    .set("spark.driver.memory", "2g")
    .set("spark.jars.packages", "org.apache.hadoop:hadoop-client:3.3.4,io.delta:delta-spark_2.12:3.2.0")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

sc = SparkContext.getOrCreate(spark_conf)
spark = SparkSession(sc)

banks = ['BankA', 'BankB', 'BankC', 'BankD']


@task
def create_users():
    data = [{"user_id": i, "user_name": f"user_{i}", "credit_score": random.randint(300, 850)} for i in range(100)]
    df = spark.createDataFrame(data)
    path = f"{BRONZE_ROOT}/user"
    df.write.format("delta").mode("overwrite").save(path)
    return path

@task
def create_cards():
    users = [{"user_id": i} for i in range(100)]
    data = [{"card_id": i, "user_id": random.choice(users)["user_id"], "bank_name": random.choice(banks)} for i in range(200)]
    df = spark.createDataFrame(data)
    path = f"{BRONZE_ROOT}/card"
    df.write.format("delta").mode("overwrite").save(path)
    return path

@task
def create_transactions():
    cards = [{"card_id": i} for i in range(200)]
    data = [{
        "transaction_id": i,
        "card_from": random.choice(cards)["card_id"],
        "card_to": random.choice(cards)["card_id"],
        "amount": round(random.uniform(10, 1000), 2),
        "timestamp": f"2024-0{random.randint(1,9)}-{random.randint(10,28)}"
    } for i in range(5000)]
    df = spark.createDataFrame(data)
    path = f"{BRONZE_ROOT}/transactions"
    df.write.format("delta").mode("overwrite").save(path)
    return path

@task
def transform_silver(user_path, card_path, txn_path):
    user_df = spark.read.format("delta").load(user_path)
    card_df = spark.read.format("delta").load(card_path)
    txn_df = spark.read.format("delta").load(txn_path)

    enriched = (
        txn_df
        .join(card_df.withColumnRenamed("card_id", "card_from"), "card_from")
        .withColumnRenamed("bank_name", "sender_bank")
        .join(card_df.withColumnRenamed("card_id", "card_to"), "card_to")
        .withColumnRenamed("bank_name", "receiver_bank")
        .select("transaction_id", "amount", "timestamp", "sender_bank", "receiver_bank")
    )

    enriched.write.format("delta").mode("overwrite").save(f"{SILVER_ROOT}/transactions")

    pivot = (
        enriched
        .groupBy("sender_bank")
        .pivot("receiver_bank", banks)
        .agg(F.sum("amount"))
        .na.fill(0)
    )

    out_path = f"{SILVER_ROOT}/pivoted"
    pivot.write.format("delta").mode("overwrite").save(out_path)
    return out_path

@task
def generate_gold(pivot_path):
    pivot_df = spark.read.format("delta").load(pivot_path)
    stack_expr = "stack(4, " + ", ".join([f"'{b}', `{b}`" for b in banks]) + ") as (receiver_bank, total_amount)"
    stacked = pivot_df.selectExpr("sender_bank", stack_expr)
    window = Window.partitionBy("sender_bank").orderBy(F.desc("total_amount"))
    ranked = stacked.withColumn("rank", F.row_number().over(window))
    result = ranked.filter("rank == 1").drop("rank")
    result.write.format("delta").mode("overwrite").save(f"{GOLD_ROOT}/top_counteragent")
    return f"{GOLD_ROOT}/top_counteragent"

@flow(name="medallion-flow")
def medallion_flow():
    user_path = create_users()
    card_path = create_cards()
    txn_path = create_transactions()
    pivot_path = transform_silver(user_path, card_path, txn_path)
    gold_path = generate_gold(pivot_path)
    print("✅ Flow complete! Gold report at:", gold_path)

if __name__ == "__main__":
    medallion_flow()