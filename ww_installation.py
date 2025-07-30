# Databricks notebook source
# MAGIC %md ##Install Libraries

# COMMAND ----------

import requests
import json

# COMMAND ----------

# MAGIC %md ##Load configuration and set service principal variables based on current environment

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

orch_jobs_viewers = config_json.get("orch_jobs_viewers")
curr_view_group_name = next(item for item in orch_jobs_viewers if item["wksp_type"] == ws_type)
view_group = curr_view_group_name.get("view_group")

# COMMAND ----------

# MAGIC %md ##Retrieve JWT token for SP authentication

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

# MAGIC %md ##API Setup

# COMMAND ----------

api_headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {access_token}",
}

# COMMAND ----------

job_list_url = f"https://{workspaceUrl}/api/2.1/jobs/list"
job_delete_url = f"https://{workspaceUrl}/api/2.1/jobs/delete"
cluster_list_url = f"https://{workspaceUrl}/api/2.0/clusters/list?can_use_client=NOTEBOOKS"
cluster_delete_url = f"https://{workspaceUrl}/api/2.0/clusters/permanent-delete"
cluster_create_url = f"https://{workspaceUrl}/api/2.0/clusters/create"
install_lib_url = f"https://{workspaceUrl}/api/2.0/libraries/install"
job_create_url = f"https://{workspaceUrl}/api/2.1/jobs/create"
job_reset_url = f"https://{workspaceUrl}/api/2.1/jobs/reset"
job_permissions_url = f"https://{workspaceUrl}/api/2.0/permissions/jobs/"
pin_cluster_url = f"https://{workspaceUrl}/api/2.0/clusters/pin"
unpin_cluster_url = f"https://{workspaceUrl}/api/2.0/clusters/unpin"

# COMMAND ----------

# MAGIC %md ##API Calls

# COMMAND ----------

# MAGIC %md ###List all jobs

# COMMAND ----------

jobs_dicts = []
offset = 0

i = True
while i == True:
    list_jobs_url = f"{job_list_url}?limit=25&offset={offset}"
    list_jobs_response = requests.get(url=list_jobs_url, headers=api_headers)
    if list_jobs_response.status_code == 200:
        list_jobs_response_json = list_jobs_response.json()
        for job in list_jobs_response_json.get("jobs"):
            job_id = job.get("job_id")
            job_name = job.get("settings").get("name")
            jobs_dicts.append({"job_id":job_id, "job_name":job_name})
        offset += 25
        i = json.loads(list_jobs_response.text)['has_more']
    else:
        raise Exception(
            f"List Jobs API returned {list_jobs_response.status_code}\n"
            f"{list_jobs_response.json().get('message')}")

# COMMAND ----------

# MAGIC %md ###Filter to orchestration jobs

# COMMAND ----------

K = "job_name"
search_list = ["__ww_composer","__ww_conductor"]

orchestration_jobs = list(filter(lambda sub: sub[K] in search_list, jobs_dicts))

# COMMAND ----------

composer_job_id = next(item for item in orchestration_jobs if item["job_name"] == "__ww_composer").get("job_id")
conductor_job_id = next(item for item in orchestration_jobs if item["job_name"] == "__ww_conductor").get("job_id")

# COMMAND ----------

# MAGIC %md ###Set orchestration job permissions

# COMMAND ----------

orch_permissions_json = {
    'access_control_list': [
        {
            'service_principal_name': client_id,
            'permission_level': 'IS_OWNER'
        },
        {
            'group_name': 'admins',
            'permission_level': 'CAN_MANAGE'
        },
        {
            'group_name': view_group,
            'permission_level': 'CAN_VIEW'
        }
        ]
    }

# COMMAND ----------

# MAGIC %md ###Update __ww_composer

# COMMAND ----------

composer_json = {
  "name": "__ww_composer",
  "email_notifications": {
      "on_failure": [
          "kschaefe@steelcase.com"
      ],
      "no_alert_for_skipped_runs": True
  },
  "webhook_notifications": {},
  "notification_settings": {
  "no_alert_for_skipped_runs": True,
  "no_alert_for_canceled_runs": True
  },
  "timeout_seconds": 0,
  "schedule": {
  "quartz_cron_expression": "0 0/10 0-20 * * ?",
  "timezone_id": "America/New_York",
  "pause_status": "PAUSED"
  },
  "max_concurrent_runs": 1,
  "tasks": [
  {
    "task_key": "ww_orchestration_master",
    "run_if": "ALL_SUCCESS",
    "notebook_task": {
      "notebook_path": "/Workspace/Repos/Woodwind/dsp_woodwind/ww_orchestration_master",
      "source": "WORKSPACE"
    },
    "job_cluster_key": "Job_cluster",
    "libraries": [
      {
        "pypi": {
          "package": "cron-converter==1.0.2"
        }
      }
    ],
    "timeout_seconds": 0,
    "email_notifications": {},
    "notification_settings": {
      "no_alert_for_skipped_runs": False,
      "no_alert_for_canceled_runs": False,
      "alert_on_last_attempt": False
    },
    "webhook_notifications": {}
  }
  ],
  "job_clusters": [
  {
    "job_cluster_key": "Job_cluster",
    "new_cluster": {
      "cluster_name": "",
      "spark_version": "14.3.x-scala2.12",
      "spark_conf": {
        "spark.master": "local[*, 4]",
        "spark.databricks.cluster.profile": "singleNode"
      },
      "azure_attributes": {
        "first_on_demand": 1,
        "availability": "ON_DEMAND_AZURE",
        "spot_bid_max_price": -1
      },
      "node_type_id": "Standard_F4",
      "driver_node_type_id": "Standard_F4",
      "custom_tags": {
        "ResourceClass": "SingleNode"
      },
      "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
      },
      "enable_elastic_disk": True,
      "data_security_mode": "SINGLE_USER",
      "runtime_engine": "STANDARD",
      "num_workers": 0
    }
  }
  ],
  "queue": {
  "enabled": True
  }
  }

# COMMAND ----------

p6 = requests.post(url=job_create_url, headers=api_headers, data=json.dumps(composer_json))

print(p6.text)

# COMMAND ----------

# MAGIC %md ###Assign permissions to __ww_composer

# COMMAND ----------

composer_perm_url = job_permissions_url + str(composer_job_id)

p7 = requests.put(url=composer_perm_url, headers=api_headers, data=json.dumps(orch_permissions_json))

print(p7.text)

# COMMAND ----------

# MAGIC %md ###Update __ww_conductor

# COMMAND ----------

conductor_json = {
  "name": "__ww_conductor",
  "email_notifications": {
    "no_alert_for_skipped_runs": False
  },
  "webhook_notifications": {},
  "timeout_seconds": 0,
  "max_concurrent_runs": 20,
  "tasks": [
    {
      "task_key": "wait_check",
      "run_if": "ALL_SUCCESS",
      "condition_task": {
        "op": "EQUAL_TO",
        "left": "{{job.parameters.wait_on_completion}}",
        "right": "true"
      },
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": False,
        "no_alert_for_canceled_runs": False,
        "alert_on_last_attempt": False
      },
      "webhook_notifications": {}
    },
    {
      "task_key": "run_job",
      "depends_on": [
        {
          "task_key": "wait_check",
          "outcome": "true"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Repos/Woodwind/dsp_woodwind/ww_job_caller",
        "source": "WORKSPACE"
      },
      "job_cluster_key": "Job_cluster",
      "max_retries": 0,
      "min_retry_interval_millis": 900000,
      "retry_on_timeout": False,
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": False,
        "no_alert_for_canceled_runs": False,
        "alert_on_last_attempt": False
      },
      "webhook_notifications": {}
    },
    {
      "task_key": "run_job_serverless",
      "depends_on": [
        {
          "task_key": "wait_check",
          "outcome": "false"
        }
      ],
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Repos/Woodwind/dsp_woodwind/ww_job_caller",
        "source": "WORKSPACE"
      },
      "max_retries": 0,
      "min_retry_interval_millis": 900000,
      "retry_on_timeout": False,
      "disable_auto_optimization": True,
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": False,
        "no_alert_for_canceled_runs": False,
        "alert_on_last_attempt": False
      },
      "webhook_notifications": {}
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "Job_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "spark_conf": {
          "spark.master": "local[*, 4]",
          "spark.databricks.cluster.profile": "singleNode"
        },
        "azure_attributes": {
          "first_on_demand": 1,
          "availability": "ON_DEMAND_AZURE",
          "spot_bid_max_price": -1
        },
        "node_type_id": "Standard_F4",
        "driver_node_type_id": "Standard_F4",
        "custom_tags": {
          "ResourceClass": "SingleNode"
        },
        "enable_elastic_disk": True,
        "data_security_mode": "SINGLE_USER",
        "runtime_engine": "STANDARD",
        "num_workers": 0
      }
    }
  ],
  "tags": {
    "ResourceClass": "SingleNode",
    "state": "exclude"
  },
  "queue": {
    "enabled": True
  },
  "parameters": [
    {
      "name": "job_type",
      "default": ""
    },
    {
      "name": "job_name",
      "default": ""
    },
    {
      "name": "wait_on_completion",
      "default": ""
    },
    {
      "name": "parameters",
      "default": ""
    }
  ]
}

# COMMAND ----------

p8 = requests.post(url=job_create_url, headers=api_headers, data=json.dumps(conductor_json))

print(p8.text)

# COMMAND ----------

# MAGIC %md ###Assign permissions to __ww_conductor

# COMMAND ----------

conductor_perm_url = job_permissions_url + str(conductor_job_id)

p9 = requests.put(url=conductor_perm_url, headers=api_headers, data=json.dumps(orch_permissions_json))

print(p9.text)