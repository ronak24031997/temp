# Databricks notebook source
# MAGIC %sql
# MAGIC drop table one;
# MAGIC CREATE TABLE workspace.default.one (
# MAGIC   timestamp TIMESTAMP,
# MAGIC   customerid STRING,
# MAGIC   asset STRING,
# MAGIC   geohash STRING,
# MAGIC   latitude DOUBLE,
# MAGIC   longitude DOUBLE,
# MAGIC   eventtype string)
# MAGIC USING delta

# COMMAND ----------

# Input event class
class AssetEvent:
    def __init__(self, timestamp, customerid=None, asset=None, geohash=None):
        self.timestamp = timestamp
        self.customerid = customerid
        self.asset = asset
        self.geohash = geohash

# Output event class
class GeohashEvent:
    def __init__(self, asset, customerid, geohash=None, enter_time=None, exit_time=None, duration=0.0):
        self.asset = asset
        self.customerid = customerid
        self.geohash = geohash
        self.enter_time = enter_time
        self.exit_time = exit_time
        self.duration = duration

# State class
class DwellGeohashState:
    def __init__(self, timestamp=None, geohash=None):
        self.timestamp = timestamp
        self.geohash = geohash


# COMMAND ----------

from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from datetime import datetime
from itertools import groupby

# Helper functions
def is_geohash_equal(expected_geohash, actual_geohash):
    return expected_geohash == actual_geohash

def update_dwell_geohashes(asset, input_events, state: GroupState):
    # Sort events by timestamp
    values = sorted(input_events, key=lambda event: event.timestamp)
    initial_state = {}

    # Initialize or retrieve the existing state
    asset_state = state.get() if state.exists else initial_state
    output_events = []

    for customer_id, customer_events in groupby(values, lambda event: event.customerid):
        if customer_id is None:
            continue

        customer_events = list(customer_events)
        for asset_event in customer_events:
            current_state = asset_state.get(customer_id, DwellGeohashState())

            if current_state.timestamp:
                if asset_event.timestamp > current_state.timestamp:
                    if is_geohash_equal(asset_event.geohash, current_state.geohash):
                        asset_state[customer_id] = DwellGeohashState(current_state.timestamp, current_state.geohash)
                    else:
                        new_event = GeohashEvent(
                            asset=asset,
                            customerid=customer_id,
                            geohash=current_state.geohash,
                            enter_time=current_state.timestamp,
                            exit_time=asset_event.timestamp,
                            duration=(asset_event.timestamp - current_state.timestamp).total_seconds() / 60.0
                        )
                        output_events.append(new_event)
                        asset_state[customer_id] = DwellGeohashState(asset_event.timestamp, asset_event.geohash)
            else:
                asset_state[customer_id] = DwellGeohashState(asset_event.timestamp, asset_event.geohash)

    # Update the state
    state.update(asset_state)

    # Generate dwell events for the current state
    for customer_id, dwell_state in asset_state.items():
        if dwell_state.timestamp:
            dwell_event = GeohashEvent(
                asset=asset,
                customerid=customer_id,
                geohash=dwell_state.geohash,
                enter_time=dwell_state.timestamp,
                exit_time=None,
                duration=(datetime.now() - dwell_state.timestamp).total_seconds() / 60.0
            )
            output_events.append(dwell_event)

    return iter(output_events)


# COMMAND ----------

# Import required libraries
from pyspark.sql.streaming.state import GroupStateTimeout
from pyspark.sql.functions import col, round, max as spark_max
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from datetime import datetime

# Define versions function
def versions(dot_limit=-1, _limit=9, overlap=0):
    return (
        [f"v1.{i}" for i in range(dot_limit + 1 - overlap)] +
        [f"v1.{i}" for i in range(dot_limit + 1 - overlap, dot_limit + 1)] +
        [f"v1_{i}" for i in range(dot_limit + 1, _limit + 1)]
    )

# Initialize widgets
stage = "dev"  # Replace with dbutils.widgets.get("stage")
fact_asset_table_version = "v1_4"  # Replace with dbutils.widgets.get("fact_asset_table_version")
target_table_version = "v1_1"  # Replace with dbutils.widgets.get("target_table_version")
start_streaming_date = "2022-09-01 00:00:00"  # Replace with dbutils.widgets.get("start_date")

# Catalog and tables
catalog = f"default."
fact_assets_history_table = catalog + "one"
# fact_dwell_geohash_history_table = catalog + "fact_dwell_geohash_history_table"
# fact_dwell_geohash_most_recent_table = catalog + "fact_dwell_geohash_most_recent_table"

# Checkpoint location
checkpoint = f"/path/to/checkpoints/{stage}/fact_dwell_by_geohash_{target_table_version}"  # Replace with the actual path

# Read streaming data
assets_df = (
    spark.readStream.format("delta")
    .option("ignoreChanges", "true")
    .table(fact_assets_history_table)
    .filter(
        col("timestamp").isNotNull() &
        col("asset").isNotNull() &
        col("customerid").isNotNull() &
        col("latitude").isNotNull() &
        col("longitude").isNotNull()
    )
    .filter((col("timestamp") >= start_streaming_date) & (col("eventtype") != "CLM"))
    .select("asset", "timestamp", "customerid", "geohash")
    .distinct()
)

# Process the data
asset_dwell_by_geohash_df = (
    assets_df.groupBy("asset")
    .applyInPandas(update_dwell_geohashes, schema="asset STRING, customerid STRING, geohash STRING, enter_time TIMESTAMP, exit_time TIMESTAMP, duration DOUBLE")
)


# COMMAND ----------

asset_dwell_by_geohash_df.display()

# COMMAND ----------


# Write the output
asset_dwell_by_geohash_df.writeStream.format("delta")\
.option("checkpointLocation", checkpoint)\
.outputMode("append")\
.foreachBatch(lambda batch_df, batch_id: process_batch(batch_df))\
.start().awaitTermination()

def process_batch(batch_df):
    if not batch_df.isEmpty():
        total_rows = batch_df.count()
        print(f"Total Rows: {total_rows}")

        dwell_geohash_history_df = batch_df.filter(col("exit_time").isNotNull())
        dwell_geohash_most_recent_df = batch_df.filter(col("exit_time").isNull())

        # Update history table
        dwell_geohash_history_df.write.format("delta").mode("append").saveAsTable(fact_dwell_geohash_history_table)

        # Update most recent table
        if DeltaTable.isDeltaTable(spark, fact_dwell_geohash_most_recent_table):
            existing_table = DeltaTable.forName(fact_dwell_geohash_most_recent_table)
            existing_df = existing_table.toDF()

            updated_df = (
                existing_df.join(dwell_geohash_history_df, ["customerid", "asset", "geohash"], "left_anti")
                .union(dwell_geohash_most_recent_df)
            )

            updated_df.write.format("delta").mode("overwrite").saveAsTable(fact_dwell_geohash_most_recent_table)
        else:
            dwell_geohash_most_recent_df.write.format("delta").mode("overwrite").saveAsTable(fact_dwell_geohash_most_recent_table)
