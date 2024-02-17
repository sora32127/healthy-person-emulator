from pprint import pprint
from typing import Dict, List, Set, TypedDict, Union
import json

import boto3
import requests
import base64


dynamoDB = boto3.resource(service_name="dynamodb")
table = dynamoDB.Table(name="hpe_content")

def get_secret() -> Dict[str, str]:
    secret_manager_client = boto3.client(service_name="secretsmanager")
    secret = secret_manager_client.get_secret_value(SecretId="wordpress-access-key")
    secret_dict = json.loads(secret["SecretString"])
    return {
        "username": secret_dict["read_only_user_username"],
        "password": secret_dict["read_only_user_pass"],
    }

def get_data(secrets: Dict[str,str]) -> List[Dict[str, str]]:
    # input:None -> Output:List of JSON
    # Output:最新の記事10個についてのURLと記事タイトルのJSONリスト

    site_url: str = "https://healthy-person-emulator.org"
    api_endpoint: str = "/wp-json/wp/v2/posts"
    username: str = secrets["username"]
    password: str = secrets["password"]

    token = base64.b64encode(f"{username}:{password}".encode()).decode()

    headers = {
        'Authorization': f'Basic {token}'
    }

    response: requests.Response = requests.get(url=site_url + api_endpoint, timeout=10, headers=headers)

    data: List[Dict[str, str]] = response.json()
    returndata: List[Dict[str, str]] = [
        {"title": post["title"]["rendered"], "url": post["link"]} for post in data
    ]
    pprint(returndata)
    return returndata


def scan_table() -> Set[str]:
    response: List[Dict[str, str]] = table.scan()["Items"]
    titles = set([i["title"] for i in response])
    return titles


def check_duplication(
    data: List[Dict[str, str]], titles: Set[str]
) -> List[Dict[str, str]]:
    ans: List[Dict[str, str]] = [i for i in data if i["title"] not in titles]
    return ans


def insert(data: List[Dict[str, str]]) -> None:
    if (data is None) or (len(data) == 0):
        print("Nothing inserted")
        return
    else:
        for i in data:
            item_of_i: Dict[str, str] = {"title": i["title"], "url": i["url"]}
            table.put_item(Item=item_of_i)
    return


def lambda_handler(event, context) -> None:
    data: List[Dict[str, str]] = get_data(get_secret())
    titles: Set[str] = scan_table()
    ans: List[Dict[str, str]] = check_duplication(data=data, titles=titles)
    insert(data=ans)


if __name__ == "__main__":
    lambda_handler(None, None)