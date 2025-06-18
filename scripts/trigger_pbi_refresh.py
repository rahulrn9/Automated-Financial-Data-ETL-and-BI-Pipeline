import os
import msal
import requests

def trigger_pbi_refresh():
    TENANT = os.environ["PBI_TENANT_ID"]
    CLIENT = os.environ["PBI_CLIENT_ID"]
    SECRET = os.environ["PBI_CLIENT_SECRET"]
    WORKSPACE = os.environ["PBI_WORKSPACE_ID"]
    DATASET = os.environ["PBI_DATASET_ID"]

    authority = f"https://login.microsoftonline.com/{TENANT}"
    app = msal.ConfidentialClientApplication(CLIENT, authority=authority, client_credential=SECRET)
    token = app.acquire_token_for_client(scopes=["https://analysis.windows.net/powerbi/api/.default"])
    access = token.get("access_token")
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{WORKSPACE}/datasets/{DATASET}/refreshes"
    headers = {"Authorization": f"Bearer {access}"}
    resp = requests.post(url, headers=headers, json={"notifyOption":"MailOnFailure"})
    resp.raise_for_status()
    print("Power BI refresh triggered")