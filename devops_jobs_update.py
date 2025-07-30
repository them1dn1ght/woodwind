import sys
import requests

token = sys.argv[1]
workspaceUrl = sys.argv[2]

api_headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}

#Submit one time update job
submit_url = f"https://{workspaceUrl}/api/2.1/jobs/runs/submit"

payload = "{\"run_name\": \"ww_update_jobs\",\"tasks\": [{\"task_key\": \"ww_update_jobs\",\"run_if\": \"ALL_SUCCESS\",\"notebook_task\": {\"notebook_path\": \"/Repos/Woodwind/dsp_woodwind/ww_update_jobs\",\"source\": \"WORKSPACE\"},\"timeout_seconds\": 0,\"email_notifications\": {}}]}"

p = requests.post(url=submit_url , headers=api_headers, data=payload)
print(p.text)