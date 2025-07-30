# Databricks notebook source
# MAGIC %md ###Install non-runtime libraries

# COMMAND ----------

# MAGIC %pip install azure-common==1.1.28 azure-core==1.30.1 azure-identity==1.15.0 azure-mgmt-core==1.4.0 azure-mgmt-datafactory==6.0.0 azure-mgmt-resource==23.0.1

# COMMAND ----------

# MAGIC %md ###Create widgets

# COMMAND ----------

dbutils.widgets.text("job_type","")
dbutils.widgets.text("job_name","")
dbutils.widgets.text("wait_on_completion","true")
dbutils.widgets.text("parameters","")
dbutils.widgets.text("conductor_job_id","")
dbutils.widgets.text("conductor_job_run_id","")

# COMMAND ----------

job_type = dbutils.widgets.get("job_type")
job_name = dbutils.widgets.get("job_name")
woc = dbutils.widgets.get("wait_on_completion")
parameters = dbutils.widgets.get("parameters")
conductor_job_id = dbutils.widgets.get("conductor_job_id")
conductor_job_run_id = dbutils.widgets.get("conductor_job_run_id")

# COMMAND ----------

# MAGIC %md ###Load python libraries

# COMMAND ----------

if job_type.casefold() == "adf":
    import ast
    import json
    import requests
    import time
    from datetime import datetime
    from pyspark.sql.types import *
    import sys
    import os
    import time
    sys.path.append(os.path.abspath('/Workspace/Repos/Utilities/dsp_utilities/azure_resource_clients'))
    from adf_pipeline_client import ADFPipelineClient
else:
    import ast
    import json
    import requests
    import time
    from datetime import datetime
    from pyspark.sql.types import *
    import sys
    import os
    import time

# COMMAND ----------

# MAGIC %md ###Set Parameters

# COMMAND ----------

if parameters == '':
    parameters = '{}'
parameters_dict = ast.literal_eval(parameters)

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

data_factories = config_json.get("azure_data_factories")
curr_data_factory = next(item for item in data_factories if item["wksp_type"] == ws_type and item["env"] == env)
resource_group_name = curr_data_factory.get("resource_group_name")
factory_name = curr_data_factory.get("factory_name")

conductor_log_location = config_json.get("conductor_log_location")
cond_log = next(item for item in conductor_log_location if item["wksp_type"] == ws_type and item["env"] == env)
log = cond_log.get("conductor_log")

# COMMAND ----------

# MAGIC %md ###Define db rest endpoints

# COMMAND ----------

run_url = f"https://{workspaceUrl}/api/2.1/jobs/run-now"
run_get_url = f"https://{workspaceUrl}/api/2.1/jobs/runs/get"
list_url = f"https://{workspaceUrl}/api/2.1/jobs/list?expand_tasks=false&name={job_name}"

# COMMAND ----------

# MAGIC %md ###Execute Job call

# COMMAND ----------

start_time = int(time.time()) * 1000

# COMMAND ----------

if job_type.casefold() == "adf":
    if woc.casefold() == "false":
        adf_pipeline = ADFPipelineClient( env= env, resource_group_name= resource_group_name, factory_name= factory_name, pipeline_name= job_name, parameters= parameters_dict )

        run_id = adf_pipeline.create_run().run_id
        print("run_id "+run_id)

    else:
        adf_pipeline = ADFPipelineClient( env= env, resource_group_name= resource_group_name, factory_name= factory_name, pipeline_name= job_name, parameters= parameters_dict )

        run_id = adf_pipeline.create_run().run_id
        print("run_id "+run_id)

        try:
            terminal_lc_states = ["Succeeded","Failed","Cancelled"]
            print("Checking run status", end="")
            while True:
                print(".", end="")
                adf_pipeline_status = adf_pipeline.get_run_status(run_id).status
                if (adf_pipeline_status in terminal_lc_states):
                    print("Terminal state reached")
                    break
                time.sleep(5)
        except:
            pass

        print(adf_pipeline_status)

        if adf_pipeline_status == "Failed":
            raise Exception("ADF pipeline failed. Please check the factory for details.")
        
elif job_type.casefold() == "db":
    if woc.casefold() == "false":
        p = requests.post(f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token', files=(
                ('client_id', (None, client_id)),
                ('grant_type', (None, 'client_credentials')),
                ('scope', (None, '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default')),
                ('client_secret', (None, dbutils.secrets.get(scope = secret_scope, key = secret_key)))
                )
                )

        json_data = json.loads(p.text)

        access_token = json_data['access_token']

        auth_string = "Bearer " + access_token
        headers =  {"Authorization": auth_string, "Content-Type": "application/json"}

        g = requests.get(list_url, headers=headers)
        job_id = json.loads(g.text)['jobs'][0]['job_id']
        job_run = {
            "job_id": job_id,
            "job_parameters": parameters_dict
                }

        r = requests.post(run_url, headers=headers, data=json.dumps(job_run, indent=4))

        try:
            run_id = json.loads(r.text)['run_id']
        except:
            pass

        print("pipeline " + job_name + " running.....")
        print("run_id " + str(run_id))
        print(f"https://{workspaceUrl}/jobs/{str(job_id)}/runs/{str(run_id)}")

    else:
        p = requests.post(f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token', files=(
                ('client_id', (None, client_id)),
                ('grant_type', (None, 'client_credentials')),
                ('scope', (None, '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default')),
                ('client_secret', (None, dbutils.secrets.get(scope = secret_scope, key = secret_key)))
                )
                )

        json_data = json.loads(p.text)

        access_token = json_data['access_token']

        auth_string = "Bearer " + access_token
        headers =  {"Authorization": auth_string, "Content-Type": "application/json"}

        g = requests.get(list_url, headers=headers)
        job_id = json.loads(g.text)['jobs'][0]['job_id']
        job_run = {
            "job_id": job_id,
            "job_parameters": parameters_dict
                }

        r = requests.post(run_url, headers=headers, data=json.dumps(job_run, indent=4))

        try:
            run_id = json.loads(r.text)['run_id']
        except:
            pass

        print("pipeline " + job_name + " running.....")
        print("run_id " + str(run_id))
        print(f"https://{workspaceUrl}/jobs/{str(job_id)}/runs/{str(run_id)}")

        try:
            terminal_lc_states = ["TERMINATED","SKIPPED","INTERNAL_ERROR","BLOCKED"]
            print("Checking run status", end="")
            basetime = datetime.now()
            while True:
                print(".", end="")
                new_basetime = (datetime.now() - basetime).total_seconds()
                status_url = run_get_url + "?run_id=" + str(run_id)
                response = requests.get(status_url, headers=headers)
                if (json.loads(response.text)['state']['life_cycle_state'] in terminal_lc_states):
                    print("Terminal state reached")
                    break
                if new_basetime > 1800:
                    print("jwt token refreshed", end="")
                    p = requests.post(f'https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token', files=(
                            ('client_id', (None, client_id)),
                            ('grant_type', (None, 'client_credentials')),
                            ('scope', (None, '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default')),
                            ('client_secret', (None, dbutils.secrets.get(scope = secret_scope, key = secret_key)))
                            )
                            )
                    json_data = json.loads(p.text)
                    access_token = json_data['access_token']
                    auth_string = "Bearer " + access_token
                    headers =  {"Authorization": auth_string, "Content-Type": "application/json"}
                    basetime = datetime.now()
                time.sleep(5)
        except:
            pass
        
        status_url = run_get_url + "?run_id=" + str(run_id)
        response = requests.get(status_url, headers=headers)

        lc_state = json.loads(response.text)['state']['life_cycle_state']
        result_state = json.loads(response.text)['state']['result_state']

        print(lc_state + " --> " + result_state)

        if result_state != "SUCCESS":
            raise Exception("Databricks job failed. Please check the workspace for details.")

# COMMAND ----------

end_time = int(time.time()) * 1000

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prepare Log Entry

# COMMAND ----------

log_entry_array = [start_time,end_time,conductor_job_id,conductor_job_run_id,job_type.casefold(),job_name,str(run_id),woc.casefold(),parameters]
log_entry = ['"'+'"|"'.join(str(item) for item in log_entry_array)+'"']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Conductor Log

# COMMAND ----------

yrmo = datetime.now().strftime("%Y%m")

attempts = 0

while attempts < 5:
    try:
        if os.path.exists(f'{log}{yrmo}_conductor_log.txt'):
            with open(f'{log}{yrmo}_conductor_log.txt', 'r') as f:
                old = f.read()
            with open(f'{log}{yrmo}_conductor_log.txt', 'w') as f:
                new = old + '\n' + log_entry[0]
                f.write(new)
        else:
            with open(f'{log}{yrmo}_conductor_log.txt', 'w') as f:
                f.write("start_time|end_time|conductor_job_id|conductor_job_run_id|job_type|job_name|job_run_id|wait_on_completion|parameters")
                new = '\n' + log_entry[0]
                f.write(new)
        break
    except:
        attempts += 1
        time.sleep(5)
else:
    dbutils.notebook.exit('Error writing conductor log entry.')