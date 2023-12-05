# Databricks notebook source
# MAGIC %md # Imports

# COMMAND ----------

import pandas as pd
from typing import Iterator
from pyspark.sql.functions import window
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, ArrayType, TimestampType, FloatType, IntegerType, StringType, LongType
from datetime import datetime

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC truncate table hive_metastore.demo_dlt_integrals.raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L1", "Wind_Speed", "2024-01-01 12:11:00.000000", 10.0);
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L1", "Wind_Speed", "2024-01-01 12:12:00.000000", 20.0);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L1", "Wind_Speed", "2024-01-01 12:14:00.000000", 40.0);
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L1", "Wind_Speed", "2024-01-01 12:19:00.000000", 30.0);
# MAGIC -- Key Group 2, will close the watermark for Group 1 as its +10 mins after the interval
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L1", "Wind_Speed", "2024-01-01 12:40:00.000000", 1.0);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- Key Group 1, this records get dropped by the watermark as its output has already been emitted
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L1", "Wind_Speed", "2024-01-01 12:18:00.000000", 35.0);
# MAGIC
# MAGIC -- Key Group 2, the group is still open to new records.
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L1", "Wind_Speed", "2024-01-01 12:41:00.000000", 6.0);
# MAGIC
# MAGIC -- Key Group 3, data is not necessarily in-order in source table
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L2", "Oil_Temp", "2024-01-01 12:48:00.000000", 110.0);
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L2", "Oil_Temp", "2024-01-01 12:41:00.000000", 100.0);
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L2", "Oil_Temp", "2024-01-01 12:45:00.000000", 95.0 );
# MAGIC
# MAGIC -- Key Group 4, only 1 records, so this values becomes time-weighted over the interval
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L3", "Humidity", "2024-01-01 12:50:00.000000", 32.5);
# MAGIC
# MAGIC -- Key Group 5, these records cause the earlier time intervals to expire
# MAGIC insert into hive_metastore.demo_dlt_integrals.raw VALUES("L3", "Wind_Speed", "2024-01-01 13:30:00.000000", 50.0);

# COMMAND ----------

# MAGIC %md # My Version

# COMMAND ----------

# ApplyInPandasWithState Function
def stateful_time_weighted_average_bp(key, df_pds: Iterator[pd.DataFrame], state: GroupState) -> pd.DataFrame:
  """Calculate stateful time weighted average for a time interval + location + sensor combination
  Parameters:
  ----------
  key: tuple of Numpy data types
    The grouping keys for ApplyInPandasWithState, e.g. location_id, sensor, timestamp_10min_interval
    E.g. Location_id = L1, sensor = Windspeed, timestamp_..._interval = "12-12:10"
  df_pdfs: Iterator[pd.DataFrame]
    Iterator of Pandas DataFrames. 
  state: GroupState
    State of the current key group

  Yields:
  --------
  time_weighted_row: Pandas Row 
    Row object with schema: location_id string, sensor string, timestamp timestamp, value float, count_rows int
  """

  # Read Grouping Keys
  (location_id, sensor, timestamp_window) = key

  print(f"::: State Key: {key}")

  # Time interval for group, in minutes
  interval_mins = (timestamp_window['end'] - timestamp_window['start']).seconds / 60
  print(f"::: Interval Mins Calculated: {interval_mins}")

  # If state has timed out, emit this row
  if state.hasTimedOut:
    # Instantiate arrays with "buffered" state (all observations so far)
    (count,) = state.get
    
    # Clean up state
    state.remove()
      
    # Call function to compute time weighted average
    # time_weighted_row = pd_time_weighted_average(location_id, sensor, timestamp_10min_interval, values_arr, timestamps_arr)

    # NOTE ON PRINT STATEMENTS: run the DLT pipeline with a single node (zero workers) to see print statements in Driver Logs UI
    print("IN TIMEOUT LOOP")
    print(f". EMITTING ROW FOR KEY: {location_id}, {sensor}, {timestamp_window}")
    print(f". CURRENT WATERMARK OF STREAM: {datetime.fromtimestamp(state.getCurrentWatermarkMs()/1000)}")
    
    # Return resulting row to calling stream
    yield pd.DataFrame({"location_id":[location_id], "sensor":[sensor], "timestamp_window": [timestamp_window], "count_rows":[count]})

  else:   
    count = sum(map(lambda pdf: len(pdf), df_pds))
    if state.exists:
      (old_count, ) = state.get
      count += old_count
 
    state.update((count,))
    
    # Determine upper bound of timestamps in this interval using grouping TS values
    interval_upper_timestamp = int(timestamp_window['end'].timestamp() * 1000)
    print("IN UPDATE LOOP")
    print(f". FOR KEY: {location_id}, {sensor}, {timestamp_window}")
    print(f". INTERVAL UPPER TIMESTAMP: {timestamp_window['end']}")

    # Reset timeout timestamp to upper limit of interval. When the watermark has passed this interval, it will emit a record.
    # E.g. if this is the 12:10 interval including all records between 12:00->12:10, timeout will be once the watermark passes 12:10
    timeout = int(interval_upper_timestamp)

    # To rather include a buffer before new rows get emitted, in the case you are modifying the watermark, uncomment below
    # timeout = int(interval_upper_timestamp+(60000 * interval_mins))

    # Catch edge cases for watermark, e.g. where equal to epoch or boundary
    if timeout <= state.getCurrentWatermarkMs():
      timeout = int(state.getCurrentWatermarkMs()+(60000 * interval_mins))

    print(f". SETTING TIMEOUT TIMESTAMP: {datetime.fromtimestamp(timeout/1000)}")

    state.setTimeoutTimestamp(timeout)

    print(f". CURRENT WATERMARK OF STREAM: {datetime.fromtimestamp(state.getCurrentWatermarkMs()/1000)}")



def dlt_integrals_bp():
  # output_schema defines the Spark dataframe type that is returned from stateful_time_weighted_average
  output_schema = StructType([
    StructField("location_id", StringType(), True),
    StructField("sensor", StringType(), True),
    StructField("timestamp_window", StructType([
                                      StructField("start", TimestampType()),
                                      StructField("end", TimestampType())
                                    ])),
    StructField("count_rows", IntegerType(), True)
  ])

  # state_schema persists between microbatches, and is used to "buffer" our observations
  state_schema = StructType([
    StructField('count', LongType(), True)
  ])
  
  # Read source data from the first DLT table we created in this notebook
  df = spark.readStream.table("hive_metastore.demo_dlt_integrals.raw")

  # Determine which timestamp interval each record belongs to
  # This is effectively a ceil() function, but handles boundary conditions
  # df = (df.withColumn("timestamp_10min_interval",
  #                                 when(
  #                                   (ceil(unix_timestamp(col("timestamp")) / 600) * 600).cast("timestamp") == col("timestamp"),
  #                                   (ceil(unix_timestamp(col("timestamp") + expr("INTERVAL 1 seconds")) / 600) * 600).cast("timestamp")
  #                                       )
  #                                 .otherwise(
  #                                   (ceil(unix_timestamp(col("timestamp")) / 600) * 600).cast("timestamp")
  #                                   )
  #                                 )
  # )

  # Apply watermark to our stream based on time interval, then groupBy.Apply() logic
  grp = (df
        .withColumn('timestamp_window', window('timestamp', '10 minutes'))
        .withWatermark('timestamp','10 minutes')
        .groupBy('location_id', 'sensor', 'timestamp_window')
        .applyInPandasWithState(
            func = stateful_time_weighted_average_bp, 
            outputStructType = output_schema,
            stateStructType  = state_schema,
            outputMode = "append",
            timeoutConf = GroupStateTimeout.EventTimeTimeout
          )
  )

  return grp

# COMMAND ----------

display(dlt_integrals_bp())

# COMMAND ----------

# MAGIC %md
# MAGIC # Pandas Function

# COMMAND ----------

def pd_time_weighted_average_bp(location_id, sensor
, timestamp_window, values_arr, timestamps_arr, interval_mins=10):
    """Calculated time-weighted average by constructing a Pandas DF from a buffered list of values and timestamps. This function is called by the Stateful Streaming aggregator.
    Parameters:
    ----------
    location_id
    sensor
    timestamp_10min_interval:
      The grouping keys for ApplyInPandasWithState, e.g. location_id, sensor, timestamp_10min_interval
      E.g. Location_id = L1, sensor = Windspeed, timestamp_..._interval = "12-12:10"
    values_arr: List
      List of values buffered for this state
    timestamps_arr: List
      List of timestamps buffered for this sate

    Yields:
    --------
    time_weighted_row: Pandas Row 
      Row object with schema: location_id string, sensor string, timestamp timestamp, value float, count_rows int
    """

    # Reconstruct Pandas DataFrame using inputs

    df_pd = pd.DataFrame({
      'location_id':location_id,
      'sensor':sensor,
      'value': values_arr,
      'timestamp': timestamps_arr
      })

    interval_mins = (timestamp_window['end'] - timestamp_window['start']).seconds / 60
    print(f"Interval_in_mins: {interval_mins}")
    #display(df_pd)

    # Sort dataframe by timestamp column ascending
    df_pd = df_pd.sort_values(by='timestamp', ascending=True)

    # Get first row and use that as the starting value; Set timestamp to start of interval
    opening_row = df_pd.iloc[[0],:].copy()
    #previous_interval_ts = opening_row['timestamp_10min_interval'] - pd.to_timedelta(interval_mins, unit='m')
    opening_row['timestamp'] = timestamp_window['start']

    # Get latest row for group, make a copy, then reset timestamp to the END of the interval, then append back
    closing_row = df_pd.iloc[[-1],:].copy()
    closing_row['timestamp'] = timestamp_window['end'] 

    # Concat rows back together
    df_agg = pd.concat([opening_row, df_pd, closing_row], ignore_index=True)

    # Get count of rows used in calculating this aggregates
    count_rows = df_pd.shape[0]

    # Calculate difference between current timestamp and start of next one, fill nulls
    df_agg['time_diff'] = df_agg['timestamp'].diff(periods=-1).dt.total_seconds()/60*-1
    df_agg['time_diff'] = df_agg['time_diff'].fillna(0).astype(int)
      
    # Calculate weighted value as: value x time_diff
    # This is the area within each "Rieman sum"
    df_agg['weighted_value'] = df_agg['value'] * df_agg['time_diff'] 
      
    # Divide the area under the value curve by the number of mins in the interval
    time_interval_weighted_avg  = df_agg['weighted_value'].sum()/interval_mins

    # Condense to one row to be returned by function, then rename timestamp columns
    # df_agg = df_agg.groupby(['location_id', 'sensor', 'timestamp_window']).agg(
    #     value      = pd.NamedAgg(column="time_interval_weighted_avg", aggfunc="first"),
    #     count_rows = pd.NamedAgg(column="count_rows", aggfunc="max")
    # ).reset_index()

    # # Rename timestamp column
    # df_agg = df_agg.rename(columns={"timestamp_10min_interval": "timestamp"})

    # Return single Pandas Dataframe row with agg
    return pd.DataFrame({'location_id':[location_id], 
                         'sensor':[sensor],
                         'timestamp_window': [timestamp_window],
                         'time_interval_weighted_avg': [time_interval_weighted_avg],
                         'count_rows': [count_rows] })

# COMMAND ----------

function_results = pd_time_weighted_average_bp(
  location_id="L1", 
  sensor="Wind_Speed", 
  timestamp_window={"start": pd.to_datetime('2024-01-01 12:10:00'), "end":pd.to_datetime('2024-01-01 12:20:00')}, 
  values_arr= [10.0, 20.0, 30.0, 40.0], 
  timestamps_arr= [
    pd.to_datetime("2024-01-01 12:11:00"),  
    pd.to_datetime("2024-01-01 12:12:00"),
    pd.to_datetime("2024-01-01 12:19:00"),  
    pd.to_datetime("2024-01-01 12:14:00")
    ], 
  interval_mins=10)

#print(function_results)
display(function_results)

# COMMAND ----------


