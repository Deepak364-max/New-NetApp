# List down all the Volumes using REST API
import requests
import urllib3

urllib3.disable_warnings()

def list_vol():
    username = "admin"
    password = "Netapp1!"
    url = "https://192.168.0.101/api/storage/volumes"
    
    header = {
        "accept": "application/json",
        "content-type": "appication/json"
     }
    response = requests.get(url,auth=(username, password),headers=header,verify=False, timeout=10)
    print(response.status_code)
    print(response.json())


list_vol()