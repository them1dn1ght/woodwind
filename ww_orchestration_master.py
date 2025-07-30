# Databricks notebook source
# MAGIC %md ###Import python libraries

# COMMAND ----------

import json
import requests
import time
import math
import os
from cron_converter import Cron
from datetime import datetime, date
from pytz import timezone
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from collections import Counter

# COMMAND ----------

# MAGIC %md ###Load Configuration

# COMMAND ----------

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
curr_dir = "/Workspace/" + nb_path[0:nb_path.rfind('/')+1]

with open(curr_dir + 'config.json', 'r') as config_file:
    config_json = json.loads(config_file.read())

# COMMAND ----------

workspaceUrl = spark.conf.get("spark.databricks.workspaceUrl")

tenant_id = config_json.get("tenant_id")

workspaces = config_json.get("workspaces")
curr_ws = next(item for item in workspaces if item["url"] == workspaceUrl)
ws_type = curr_ws.get("wksp_type")
env = curr_ws.get("env")

service_principals = config_json.get("service_principals")
curr_sp = next(item for item in service_principals if item["wksp_type"] == ws_type and item["env"] == env)
client_id = curr_sp.get("client_id")
secret_scope = curr_sp.get("secret_scope")
secret_key = curr_sp.get("secret_key")

dd = config_json.get("date_dim")
curr_dd = next(item for item in dd if item["wksp_type"] == ws_type)
dt_dim = curr_dd.get("date_dim")

external_metadata = config_json.get("external_metadata")
curr_ext_md = next(item for item in external_metadata if item["wksp_type"] == ws_type and item["env"] == env)
ext_metadata = curr_ext_md.get("ext_metadata")

metadata_checkpoint = config_json.get("metadata_checkpoint")
curr_md_cp = next(item for item in metadata_checkpoint if item["wksp_type"] == ws_type and item["env"] == env)
md_cp = curr_md_cp.get("metadata_checkpoint")

metadata_table = config_json.get("metadata_table")
curr_md_table = next(item for item in metadata_table if item["wksp_type"] == ws_type and item["env"] == env)
metadata = curr_md_table.get("metadata")

log_location = config_json.get("log_location")
curr_log = next(item for item in log_location if item["wksp_type"] == ws_type and item["env"] == env)
log = curr_log.get("log")

# COMMAND ----------

vol_root = md_cp[0:md_cp.rfind('/')+1]
md_cp_fn = md_cp[md_cp.rfind('/')+1:]
new_md_cp_fn = md_cp_fn.replace('.','_temp.')
tmp_md_cp = vol_root + new_md_cp_fn

dbutils.fs.cp(md_cp,tmp_md_cp)

metadata_fn = metadata[metadata.rfind('/')+1:]
new_metadata_fn = metadata_fn.replace('.','_temp.')
tmp_metadata = vol_root + new_metadata_fn

dbutils.fs.cp(metadata,tmp_metadata)

# COMMAND ----------

# MAGIC %md ###Create Special Dates

# COMMAND ----------

naive = datetime.now()
tz = timezone("US/Eastern")

aware = tz.localize(naive)
offset = aware.utcoffset().total_seconds()

curr_dt = datetime.fromtimestamp(math.floor(naive.timestamp() + offset)).date()
date_dim = spark.read.csv(dt_dim, header=True, inferSchema=True)

fcl_per_index = date_dim.filter(date_dim.CLDR_DT == curr_dt).select(expr("CASE WHEN FCL_DAY_OF_PER_C < 3 THEN FCL_PER_INDEX - 1 ELSE FCL_PER_INDEX END"))
fcl_per_index_int = fcl_per_index.collect()[0][0]

fcl_qtr_index = date_dim.filter(date_dim.CLDR_DT == curr_dt).select(expr("CASE WHEN FCL_DAY_OF_QTR_C < 3 THEN FCL_QTR_INDEX - 1 ELSE FCL_QTR_INDEX END"))
fcl_qtr_index_int = fcl_qtr_index.collect()[0][0]


fcl_start = date_dim.filter(date_dim.CLDR_DT == curr_dt).select(to_unix_timestamp("FCL_PER_BGN_DT").alias("FCL_PER_BGN_EPOCH"))
fcl_start_epoch = fcl_start.collect()[0][0]

fcl_qtr_start = date_dim.filter(date_dim.CLDR_DT == curr_dt).select(to_unix_timestamp("FCL_QTR_BGN_DT").alias("FCL_QTR_BGN_EPOCH"))
fcl_qtr_start_epoch = fcl_qtr_start.collect()[0][0]

fcl_per_first_mon = date_dim.filter((date_dim.FCL_PER_INDEX == fcl_per_index_int) & (date_dim.FCL_DAY_OF_PER_C == 3)).select(to_unix_timestamp("CLDR_DT").alias("FCL_PER_MON_1_EPOCH"))
fcl_per_first_mon_epoch = fcl_per_first_mon.collect()[0][0]

fcl_qtr_first_mon = date_dim.filter((date_dim.FCL_QTR_INDEX == fcl_qtr_index_int) & (date_dim.FCL_DAY_OF_QTR_C == 3)).select(to_unix_timestamp("CLDR_DT").alias("FCL_QTR_MON_1_EPOCH"))
fcl_qtr_first_mon_epoch = fcl_qtr_first_mon.collect()[0][0]

adj_fcl_start_epoch = math.floor(fcl_start_epoch - offset)
adj_fcl_qtr_start_epoch = math.floor(fcl_qtr_start_epoch - offset)
adj_fcl_per_first_mon_epoch = math.floor(fcl_per_first_mon_epoch - offset)
adj_fcl_qtr_first_mon_epoch = math.floor(fcl_qtr_first_mon_epoch - offset)

# COMMAND ----------

# MAGIC %md ###Functions and UDF registrations

# COMMAND ----------

def union_dicts(list1, list2):
    result = []
    for dict1 in list1:
        found = False
        for dict2 in list2:
            if dict1 == dict2:
                found = True
                break
        if not found:
            result.append(dict1)

    result.extend(list2)
    return result

@udf(returnType=LongType())
def udf_prev_cron(x,y):
    naive = datetime.now()
    tz = timezone(y)

    aware = tz.localize(naive)
    offset = aware.utcoffset().total_seconds()

    naive_epoch = time.mktime(naive.timetuple())
    aware_epoch = math.floor(naive_epoch + offset)
    aware_naive = datetime.fromtimestamp(aware_epoch)

    cron_epoch = time.mktime(Cron(x).schedule(start_date=aware_naive).prev().timetuple())
    adj_cron_epoch = math.floor(cron_epoch - offset)
    return adj_cron_epoch

@udf(returnType=LongType())
def udf_next_cron(x,y):
    naive = datetime.now()
    tz = timezone(y)

    aware = tz.localize(naive)
    offset = aware.utcoffset().total_seconds()

    naive_epoch = time.mktime(naive.timetuple())
    aware_epoch = math.floor(naive_epoch + offset)
    aware_naive = datetime.fromtimestamp(aware_epoch)

    cron_epoch = time.mktime(Cron(x).schedule(start_date=aware_naive).next().timetuple())
    adj_cron_epoch = math.floor(cron_epoch - offset)
    return adj_cron_epoch


spark.udf.register("udf_prev_cron", udf_prev_cron)
spark.udf.register("udf_next_cron", udf_next_cron)

# COMMAND ----------

# MAGIC %md ###API authentication and setup

# COMMAND ----------

p = requests.post(f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token', files=(
        ('client_id', (None, client_id)),
        ('grant_type', (None, 'client_credentials')),
        ('scope', (None, '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default')),
        ('client_secret', (None, dbutils.secrets.get(scope = secret_scope, key = secret_key)))
        )
        )

json_data = json.loads(p.text)

access_token = json_data['access_token']

# COMMAND ----------

auth_string = "Bearer " + access_token
headers =  {"Authorization": auth_string, "Content-Type": "application/json"}

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build Itinerary

# COMMAND ----------

filtered_json = [{'job_id': 999999999999999, 'creator_user_name': 'none', 'settings': {'name': 'schema_placeholder', 'email_notifications': {'no_alert_for_skipped_runs': False}, 'timeout_seconds': 0, 'max_concurrent_runs': 1, 'tags': {'state': 'paused', 'cron': '* * * * *', 'tz': 'none', 'tdep': 'none', 'tdep2': 'none', 'sdep': 'none', 'sdep2': 'none', 'cdep': 'none', 'cdep2': 'none', 'spec': 'none', 'onfail': 'none'}, 'format': 'MULTI_TASK'}, 'created_time': 1702926855}]

job_list_url = f"https://{workspaceUrl}/api/2.1/jobs/list?expand_tasks=false"

next_page_token = ""

i = True
while i == True:
  job_list_response = requests.get(url=(job_list_url + next_page_token), headers=headers)
  if job_list_response.status_code == 200:
        try:
            loop_json = json.loads(job_list_response.text)['jobs']
            filtered_json = filtered_json + loop_json
            i = json.loads(job_list_response.text)['has_more']
            if i == True:
                next_page_token = "&page_token=" + job_list_response.json().get('next_page_token')
        except:
            i = False
  else:
      raise Exception(
          f"List jobs API returned {job_list_response.status_code}\n"
          f"{job_list_response.text}")

# COMMAND ----------

itinerary_schema = StructType([
   StructField("created_time", LongType(), True),
   StructField("creator_user_name", StringType(), True),
   StructField("job_id", LongType(), True),
   StructField("settings", StructType([      
                                       StructField("name", StringType(), True),
                                       StructField("email_notifications", StructType([
                                           StructField("no_alert_for_skipped_runs", BooleanType(), True)]), True),
                                       StructField("timeout_seconds", LongType(), True),
                                       StructField("max_concurrent_runs", LongType(), True),
                                       StructField("tags", StructType([
                                           StructField("state", StringType(), True),
                                           StructField("cron", StringType(), True),
                                           StructField("tz", StringType(), True),
                                           StructField("tdep", StringType(), True),
                                           StructField("tdep2", StringType(), True),
                                           StructField("sdep", StringType(), True),
                                           StructField("sdep2", StringType(), True),
                                           StructField("cdep", StringType(), True),
                                           StructField("cdep2", StringType(), True),
                                           StructField("spec", StringType(), True),
                                           StructField("onfail", StringType(), True)]), True),
                                       StructField("format", StringType(), True)
                ])
        )
    ])

tags_df = spark.createDataFrame(filtered_json, schema=itinerary_schema)

# COMMAND ----------

parsed_tags = tags_df.select("job_id", "created_time", col("settings.name").alias("pipeline"),col("settings.max_concurrent_runs").cast("integer").alias("max_concurrent_runs") ,"settings.tags.*").filter((isnull("state") == False) & (col("pipeline") != "schema_placeholder"))

# COMMAND ----------

itinerary_raw_filled = parsed_tags.fillna("* * * * *","cron").fillna("UTC","tz").fillna("None","spec").fillna("hold","onfail")

# COMMAND ----------

itinerary_raw_conc = itinerary_raw_filled.select("job_id", "created_time", "pipeline", "max_concurrent_runs","cron", when(col("spec") != "None", "US/Eastern").otherwise(col("tz")).alias("tz"), when(col("sdep2").isNotNull(), concat_ws(',',col("sdep"),col("sdep2"))).otherwise(col("sdep")).alias("sdep"), "state", when(col("tdep2").isNotNull(), concat_ws(',',col("tdep"),col("tdep2"))).otherwise(col("tdep")).alias("tdep"), when(col("cdep2").isNotNull(), concat_ws(',',col("cdep"),col("cdep2"))).otherwise(col("cdep")).alias("cdep"), "spec", "onfail")

# COMMAND ----------

itinerary = itinerary_raw_conc.select("job_id","created_time",col("pipeline").alias("itinerary_pipeline"),"state","max_concurrent_runs",col("cron").alias("min_exec_cron"),"tz","spec","onfail",concat_ws(",",split(col("tdep"),",")).alias("trigger_dependencies"),concat_ws(",",split(col("sdep"),",")).alias("status_dependencies"),concat_ws(",",split(col("cdep"),",")).alias("completed_dependencies")).withColumn("prev_min_exec_time", udf_prev_cron(col("min_exec_cron"),col("tz"))).withColumn("next_min_exec_time", udf_next_cron(col("min_exec_cron"),col("tz"))).withColumn("prev_fcl_per_start_time", lit(adj_fcl_start_epoch)).withColumn("prev_fcl_per_first_mon", lit(adj_fcl_per_first_mon_epoch)).withColumn("prev_fcl_qtr_start_time", lit(adj_fcl_qtr_start_epoch)).withColumn("prev_fcl_qtr_first_mon", lit(adj_fcl_qtr_first_mon_epoch))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Databricks Pipeline Metadata via API

# COMMAND ----------

try:
  if os.path.exists(tmp_md_cp):
    with open(tmp_md_cp, 'r') as f:
        chkpnt_data = f.read()
  last_checkpoint = int(chkpnt_data)
  meta_type = "incremental"
except:
  meta_type = "full"

meta_type

# COMMAND ----------

pipeline_metadata_schema = StructType([
    StructField("job_id", LongType(), True),
    StructField("source", StringType(), True),
    StructField("lock", StringType(), True),
    StructField("status", StringType(), True),
    StructField("pipeline", StringType(), True),
    StructField("start_time", LongType(), True),
    StructField("end_time", LongType(), True)
  ])

pipeline_metadata = spark.read.format("csv").option("header", "true").schema(pipeline_metadata_schema).load(tmp_metadata)

# COMMAND ----------

#Include running jobs in incrememntal metadata
running_jobs_df = pipeline_metadata.filter(col("status") == "RUNNING")
running_job_ids = [row["job_id"] for row in running_jobs_df.select("job_id").collect()]

#Include failed jobs in incrememntal metadata
failed_jobs_df = pipeline_metadata.filter(col("status") == "FAILED")
failed_job_ids = [row["job_id"] for row in failed_jobs_df.select("job_id").collect()]

# COMMAND ----------

job_ids = parsed_tags.select(
    "job_id",
    col("max_concurrent_runs").alias("value")
).toPandas().to_dict(orient='records')

concurr_job_ids = [d for d in job_ids if d["value"] > 1]

# COMMAND ----------

checkpoint = int(time.time()) * 1000

# COMMAND ----------

if meta_type == "full":
  runs_dicts = []

  for i in range(len(job_ids)):
      jobId = job_ids[i]["job_id"]
      run_list_url = f"https://{workspaceUrl}/api/2.1/jobs/runs/list?job_id={jobId}&limit=1"

      run_list_response = requests.get(url=run_list_url, headers=headers)
      run_list_response_json = run_list_response.json()
      
      try:
        job_id = run_list_response_json.get("runs")[0].get("job_id")
        state = run_list_response_json.get("runs")[0].get("state").get("life_cycle_state")
        try:
          status = run_list_response_json.get("runs")[0].get("state").get("result_state")
        except:
          status = run_list_response_json.get("runs")[0].get("state").get("life_cycle_state")
        start_time = run_list_response_json.get("runs")[0].get("start_time")
        end_time = run_list_response_json.get("runs")[0].get("end_time")
        runs_dicts.append({"job_id":job_id, "state":state, "status":status, "start_time":start_time, "end_time":end_time})
      except:
        pass
else:
  run_list_url = f"https://{workspaceUrl}/api/2.1/jobs/runs/list?start_time_from={last_checkpoint}"

  next_page_token = ""
  run_list_json = []

  i = True
  while i == True:
    run_list_response = requests.get(url=(run_list_url + next_page_token), headers=headers)
    if run_list_response.status_code == 200:
          try:
              loop_json = json.loads(run_list_response.text)['runs']
              run_list_json = run_list_json + loop_json
              i = json.loads(run_list_response.text)['has_more']
              if i == True:
                  next_page_token = "&page_token=" + run_list_response.json().get('next_page_token')
          except:
              i = False
    else:
        raise Exception(
            f"List job runs API returned {run_list_response.status_code}\n"
            f"{run_list_response.text}")

  runs_dicts = []

  for i in range(len(run_list_json)):
    job_id = run_list_json[i]["job_id"]
    state = run_list_json[i]["state"]["life_cycle_state"]
    try:
      status = run_list_json[i]["state"]["result_state"]
    except:
      status = run_list_json[i]["state"]["life_cycle_state"]
    start_time = run_list_json[i]["start_time"]
    end_time = run_list_json[i]["end_time"]
    runs_dicts.append({"job_id":job_id, "state":state, "status":status, "start_time":start_time, "end_time":end_time})

      

# COMMAND ----------

running_dicts = []

for i in range(len(running_job_ids)):
  jobId = running_job_ids[i]
  run_list_url = f"https://{workspaceUrl}/api/2.1/jobs/runs/list?job_id={jobId}&limit=1"

  run_list_response = requests.get(url=run_list_url, headers=headers)
  run_list_response_json = run_list_response.json()
  
  try:
    job_id = run_list_response_json.get("runs")[0].get("job_id")
    state = run_list_response_json.get("runs")[0].get("state").get("life_cycle_state")
    try:
      status = run_list_response_json.get("runs")[0].get("state").get("result_state")
    except:
      status = run_list_response_json.get("runs")[0].get("state").get("life_cycle_state")
    start_time = run_list_response_json.get("runs")[0].get("start_time")
    end_time = run_list_response_json.get("runs")[0].get("end_time")
    running_dicts.append({"job_id":job_id, "state":state, "status":status, "start_time":start_time, "end_time":end_time})
  except:
    pass

# COMMAND ----------

failed_dicts = []

for i in range(len(failed_job_ids)):
  jobId = failed_job_ids[i]
  run_list_url = f"https://{workspaceUrl}/api/2.1/jobs/runs/list?job_id={jobId}&limit=1"

  run_list_response = requests.get(url=run_list_url, headers=headers)
  run_list_response_json = run_list_response.json()
  
  try:
    job_id = run_list_response_json.get("runs")[0].get("job_id")
    state = run_list_response_json.get("runs")[0].get("state").get("life_cycle_state")
    try:
      status = run_list_response_json.get("runs")[0].get("state").get("result_state")
    except:
      status = run_list_response_json.get("runs")[0].get("state").get("life_cycle_state")
    start_time = run_list_response_json.get("runs")[0].get("start_time")
    end_time = run_list_response_json.get("runs")[0].get("end_time")
    failed_dicts.append({"job_id":job_id, "state":state, "status":status, "start_time":start_time, "end_time":end_time})
  except:
    pass

# COMMAND ----------

terminal_lc_states = ["TERMINATED","SKIPPED","INTERNAL_ERROR","BLOCKED"]
concurr_runs_dicts = []

for i in range(len(concurr_job_ids)):
    dictItem = concurr_job_ids[i]
    jobId = concurr_job_ids[i]["job_id"]
    cRuns = concurr_job_ids[i]["value"]
    loop_dicts = []

    crun_list_url = f"https://{workspaceUrl}/api/2.1/jobs/runs/list?job_id={jobId}&limit={cRuns}&active_only=true"

    crun_list_response = requests.get(url=crun_list_url, headers=headers)
    crun_list_response_json = crun_list_response.json()
    try:
        for j in range(len(crun_list_response_json.get("runs"))):
            job_id = crun_list_response_json.get("runs")[j].get("job_id")
            state = crun_list_response_json.get("runs")[j].get("state").get("life_cycle_state")
            start_time = crun_list_response_json.get("runs")[j].get("start_time")
            loop_dicts.append({"job_id":job_id, "state":state, "start_time":start_time,})
    except:
        pass
    running_concurr_job_ids = [d for d in loop_dicts if d["state"] not in terminal_lc_states]
    cjobs = Counter(job["job_id"] for job in running_concurr_job_ids)
    cjobs_running = [{"job_id": key, "value": value} for key, value in cjobs.items()]
    try:
        concurr_runs_dicts.append({"job_id":jobId, "jobs_open":(cRuns-cjobs_running[0]["value"])})
    except:
        pass

# COMMAND ----------

lock_release = [d["job_id"] for d in concurr_runs_dicts if d["jobs_open"] > 0]

# COMMAND ----------

ancillary_dicts = union_dicts(running_dicts, failed_dicts)
complete_dicts = union_dicts(runs_dicts, ancillary_dicts)

# COMMAND ----------

db_metadata_schema = StructType([
   StructField("job_id", LongType(), True),
   StructField("state", StringType(), True),
   StructField("status", StringType(), True),
   StructField("start_time", LongType(), True),
   StructField("end_time", LongType(), True)
    ])

db_metadata = spark.createDataFrame((Row(**x) for x in complete_dicts), schema=db_metadata_schema).withColumn("source", lit("azure_databricks"))

# COMMAND ----------

aug_db_metadata = db_metadata.join(itinerary_raw_filled, db_metadata.job_id == itinerary_raw_filled.job_id,"inner").select(db_metadata.job_id, db_metadata.source, db_metadata.state, db_metadata.status, itinerary_raw_filled.pipeline, floor(db_metadata.start_time/1000).alias("start_time"), floor(db_metadata.end_time/1000).alias("end_time"))

# COMMAND ----------

max_endtime_db_metadata = aug_db_metadata.groupBy("job_id").agg(max("end_time").alias("max_end_time"))

aug_db_metadata_alias = aug_db_metadata.alias("a")
max_endtime_db_metadata_alias = max_endtime_db_metadata.alias("b")

aug_db_metadata2 = aug_db_metadata_alias.join(
    max_endtime_db_metadata_alias,
    (col("a.job_id") == col("b.job_id")) & (col("a.end_time") == col("b.max_end_time")),
    "inner"
).select(
    col("a.job_id"),
    col("a.source"),
    col("a.state"),
    col("a.status"),
    col("a.pipeline"),
    col("a.start_time"),
    col("a.end_time")
)

# COMMAND ----------

max_starttime_db_metadata = aug_db_metadata2.groupBy("job_id").agg(max("start_time").alias("max_start_time"))

aug_db_metadata2_alias = aug_db_metadata2.alias("a")
max_starttime_db_metadata_alias = max_starttime_db_metadata.alias("b")

aug_db_metadata3 = aug_db_metadata2_alias.join(
    max_starttime_db_metadata_alias,
    (col("a.job_id") == col("b.job_id")) & (col("a.start_time") == col("b.max_start_time")),
    "inner"
).select(
    col("a.job_id"),
    col("a.source"),
    col("a.state"),
    col("a.status"),
    col("a.pipeline"),
    col("a.start_time"),
    col("a.end_time")
)

# COMMAND ----------

aug_db_metadata4 = aug_db_metadata3.select("job_id", "source", when(col("state").isin(terminal_lc_states),lit("0")).otherwise(lit("1")).alias("lock"), when(col("state").isin(terminal_lc_states),col("status")).otherwise(col("state")).alias("status"), "pipeline", "start_time", "end_time")

# COMMAND ----------

final_db_metadata = aug_db_metadata4.distinct().select("job_id", "source", when(col("job_id").isin(lock_release),lit("0")).otherwise(col("lock")).alias("lock"), "status", "pipeline", "start_time", "end_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load External Pipeline Metadata

# COMMAND ----------

attempts = 0

while attempts < 5:
    try:
        ext_pipeline_metadata = spark.read.parquet(ext_metadata)
        ext_pipeline_metadata = ext_pipeline_metadata.select(lit(99999999999999).alias("job_id"), "source", "lock", "status", "pipeline", to_unix_timestamp(col("start_time").cast("timestamp")).alias("start_time"), to_unix_timestamp(col("end_time").cast("timestamp")).alias("end_time"))
        break
    except:
        attempts += 1
        time.sleep(10)
else:
    dbutils.notebook.exit('Issue with reading metadata files.')

#Cache the dataframe
ext_pipeline_metadata.cache()

# Perform actions that trigger computation and caching
ext_pipeline_metadata.count()

# COMMAND ----------

combined_metadata = final_db_metadata.union(ext_pipeline_metadata)

# COMMAND ----------

merged_metadata = (
    pipeline_metadata.alias("a")
    .join(combined_metadata.alias("b"), ["pipeline", "source"], how="outer")
    .select(
        coalesce("b.job_id", "a.job_id").alias("job_id"),
        "source",
        coalesce("b.lock", "a.lock").alias("lock"),
        coalesce("b.status", "a.status").alias("status"),
        "pipeline",
        coalesce("b.start_time", "a.start_time").alias("start_time"),
        coalesce("b.end_time", "a.end_time").alias("end_time"),
    )
)

# COMMAND ----------

with open(metadata, 'w') as f:
    f.write(merged_metadata.toPandas().to_csv(index=False))

# COMMAND ----------

with open(md_cp, 'w') as f:
    f.write(str(checkpoint))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Itinerary and Pipeline Metadata

# COMMAND ----------

itinerary_meta1 = itinerary.join(merged_metadata, trim(itinerary.itinerary_pipeline) == merged_metadata.pipeline,"leftouter") \
    .select(itinerary.job_id,"created_time","itinerary_pipeline","state",col("lock").cast("integer").alias("lock_flag"),col("status").alias("pipeline_status"),"tz","spec","onfail",col("start_time").alias("last_start_time"),col("end_time").alias("last_end_time"),explode_outer(split(itinerary.trigger_dependencies, ',')).alias("trigger_dependency"),"status_dependencies","completed_dependencies","prev_min_exec_time","next_min_exec_time","prev_fcl_per_start_time","prev_fcl_per_first_mon","prev_fcl_qtr_start_time","prev_fcl_qtr_first_mon") \
    .select("job_id","created_time","itinerary_pipeline","state","lock_flag","pipeline_status","tz","spec","onfail","last_start_time","last_end_time","trigger_dependency",explode_outer(split(itinerary.status_dependencies, ',')).alias("status_dependency"),"completed_dependencies","prev_min_exec_time","next_min_exec_time","prev_fcl_per_start_time","prev_fcl_per_first_mon","prev_fcl_qtr_start_time","prev_fcl_qtr_first_mon") \
    .select("job_id","created_time","itinerary_pipeline","state","lock_flag","pipeline_status","tz","spec","onfail","last_start_time","last_end_time","trigger_dependency","status_dependency",explode_outer(split(itinerary.completed_dependencies, ',')).alias("completed_dependency"),"prev_min_exec_time","next_min_exec_time","prev_fcl_per_start_time","prev_fcl_per_first_mon","prev_fcl_qtr_start_time","prev_fcl_qtr_first_mon")

itinerary_meta2 = itinerary_meta1.join(merged_metadata, trim(itinerary_meta1.trigger_dependency) == merged_metadata.pipeline,"leftouter") \
    .select(itinerary_meta1.job_id,"created_time","itinerary_pipeline","state","lock_flag","pipeline_status","tz","spec","onfail","last_start_time","last_end_time","prev_min_exec_time","next_min_exec_time","prev_fcl_per_start_time","prev_fcl_per_first_mon","prev_fcl_qtr_start_time","prev_fcl_qtr_first_mon","trigger_dependency",col("start_time").alias("tdep_start_time"),col("end_time").alias("tdep_end_time"),col("status").alias("tdep_status"),"status_dependency","completed_dependency")

itinerary_meta3 = itinerary_meta2.join(merged_metadata, trim(itinerary_meta2.status_dependency) == merged_metadata.pipeline,"leftouter") \
    .select(itinerary_meta2.job_id,"created_time","itinerary_pipeline","state","lock_flag","pipeline_status","tz","spec","onfail","last_start_time","last_end_time","prev_min_exec_time","next_min_exec_time","prev_fcl_per_start_time","prev_fcl_per_first_mon","prev_fcl_qtr_start_time","prev_fcl_qtr_first_mon","trigger_dependency","tdep_start_time","tdep_end_time","tdep_status","status_dependency",col("status").alias("sdep_status"),"completed_dependency")

itinerary_meta4 = itinerary_meta3.join(merged_metadata, trim(itinerary_meta3.completed_dependency) == merged_metadata.pipeline,"leftouter") \
    .select(itinerary_meta3.job_id,"created_time",col("itinerary_pipeline").alias("pipeline"),"state","lock_flag","pipeline_status","tz","spec","onfail","last_start_time","last_end_time","prev_min_exec_time","next_min_exec_time","prev_fcl_per_start_time","prev_fcl_per_first_mon","prev_fcl_qtr_start_time","prev_fcl_qtr_first_mon","trigger_dependency","tdep_start_time","tdep_end_time","tdep_status","status_dependency","sdep_status","completed_dependency",col("start_time").alias("cdep_start_time"),col("end_time").alias("cdep_end_time"))

final = itinerary_meta4.fillna(0,"lock_flag") \
    .select("job_id","pipeline","state","lock_flag","pipeline_status","tz","spec","onfail", when(col("last_start_time").isNull(),lit(floor(col("created_time")/1000))).otherwise(col("last_start_time")).alias("last_start_time"), when(col("last_end_time").isNull(),lit(0)).otherwise(col("last_end_time")).alias("last_end_time"),"prev_min_exec_time","next_min_exec_time","prev_fcl_per_start_time","prev_fcl_per_first_mon","prev_fcl_qtr_start_time","prev_fcl_qtr_first_mon",when(col("trigger_dependency") != "", col("trigger_dependency")).otherwise(None).alias("trigger_dependency"),"tdep_start_time","tdep_end_time","tdep_status",when(col("status_dependency") != "", col("status_dependency")).otherwise(None).alias("status_dependency"),"sdep_status",when(col("completed_dependency") != "", col("completed_dependency")).otherwise(None).alias("completed_dependency"),"cdep_start_time","cdep_end_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate Pipeline Readiness

# COMMAND ----------

ready = final.withColumn("readiness", when((
    (final.last_start_time < final.prev_min_exec_time) & 
    (((final.last_start_time < final.prev_fcl_per_start_time) & (final.spec == "fy_per_d1")) | ((final.last_start_time < final.prev_fcl_per_first_mon) & (final.spec == "fy_per_1st_monday")) | \
        ((final.last_start_time < final.prev_fcl_qtr_start_time) & (final.spec == "fy_qtr_d1")) | ((final.last_start_time < final.prev_fcl_qtr_first_mon) & (final.spec == "fy_qtr_1st_monday")) | (final.spec == "None")) & 
    (((final.last_start_time < final.tdep_end_time) & (final.tdep_status == "SUCCESS")) | (isnull(final.trigger_dependency))) & 
    ((isnull(final.status_dependency)) | (final.sdep_status == "SUCCESS")) & 
    ((final.last_start_time < final.cdep_end_time) | (isnull(final.completed_dependency))) & 
    (final.lock_flag == 0) & 
    ((final.pipeline_status != "FAILED") | (isnull(final.pipeline_status)) | ((final.pipeline_status == "FAILED") & (final.onfail == "pass"))) & 
    (final.state == "active")
    ),0).otherwise(1))

# COMMAND ----------

ready_readiness_agg = ready.groupBy("job_id").agg(sum("readiness").alias("ready"))

ready_agg_final = ready_readiness_agg.select(ready_readiness_agg.job_id,"ready").filter(col("ready") == 0).select("job_id")

# COMMAND ----------

readyList = [row.asDict() for row in ready_agg_final.collect()]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Log and Call Ready Pipelines Based on Environment
# MAGIC - dev - log only
# MAGIC - qa - log only
# MAGIC - prod - call & log

# COMMAND ----------

log_file_name = naive.strftime("%Y%m%d_%H%M%S")
run_url = f"https://{workspaceUrl}/api/2.1/jobs/run-now"

if env == "dev":
    print(f"Writing log entry to '{log}{log_file_name}.csv'...")
    with open(f'{log}{log_file_name}.csv', 'w') as f:
        f.write(ready.withColumn("timestamp", lit(naive)).toPandas().to_csv(index=False, sep='|'))
elif env == "qa":
    print(f"Writing log entry to '{log}{log_file_name}.csv'...")
    with open(f'{log}{log_file_name}.csv', 'w') as f:
        f.write(ready.withColumn("timestamp", lit(naive)).toPandas().to_csv(index=False, sep='|'))
elif env == "prod":
    for i in range(len(readyList)):
        job_id = readyList[i]["job_id"]
        print(f"Orchestrating job {job_id}...")
        job_run = {
            "job_id": job_id
            }
        r = requests.post(run_url, headers=headers, data=json.dumps(job_run, indent=4))
        json_data = json.loads(r.text)
        print(json_data)
    print(f"Writing log entry to '{log}{log_file_name}.csv'...")
    with open(f'{log}{log_file_name}.csv', 'w') as f:
        f.write(ready.withColumn("timestamp", lit(naive)).toPandas().to_csv(index=False, sep='|'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Up Temp Files and Old Logs

# COMMAND ----------

dbutils.fs.rm(tmp_md_cp)
dbutils.fs.rm(tmp_metadata)

# COMMAND ----------

log_cutoff = checkpoint - 2592000000

files = dbutils.fs.ls(log)
filenames = [f.name for f in files if f.size > 0 and f.modificationTime <= log_cutoff]

for i in range(len(filenames)):
    print(f"Removing {log}{filenames[i]}...")
    dbutils.fs.rm(f"{log}{filenames[i]}")