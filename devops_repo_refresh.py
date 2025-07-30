import sys
import requests

token = sys.argv[1]
workspaceUrl = sys.argv[2]
repo = sys.argv[3]

api_headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {token}",
}

#Refresh repo
refresh_url = f"https://{workspaceUrl}/api/2.0/repos/{repo}"

print(f"Refreshing repo: {repo}")
payload = "{\"branch\": \"master\"}"
p = requests.patch(url=refresh_url, headers=api_headers, data=payload)
print(p.text)