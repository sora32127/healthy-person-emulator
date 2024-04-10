from typing import Dict, List, Set
import json

import boto3
from botocore.exceptions import ClientError
import psycopg2


dynamoDB = boto3.resource(service_name="dynamodb")
table = dynamoDB.Table(name="hpe_content_buffer")

def get_secret():

    secret_name = "SUPABASE_CONNECTION_SECRET"
    region_name = "ap-northeast-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)


def get_data(secrets: Dict[str,str]) -> List[Dict[str, str]]:
    conn = psycopg2.connect(
        dbname=secrets["dbname"],
        user=secrets["username"],
        password=secrets["password"],
        host=secrets["host"],
        port=secrets["port"]
    )
    cur = conn.cursor()
    cur.execute("SELECT post_id, post_title FROM dim_posts order by post_date_gmt desc limit 10;")
    data = cur.fetchall()
    cur.close()

    ans: List[Dict[str, str]] = [
        {
            "post_id": i[0],
            "post_title": i[1],
            "post_url": f"https://healthy-person-emulator.org/archives/{i[0]}"
        }
        for i in data]
    return ans


def scan_table() -> Set[str]:
    response: List[Dict[str, str]] = table.scan()["Items"]
    unique_post_ids = set([i["post_id"] for i in response])
    return unique_post_ids


def check_duplication(
    data: List[Dict[str, str]], unique_post_ids: Set[str]
) -> List[Dict[str, str]]:
    ans: List[Dict[str, str]] = [i for i in data if i["post_id"] not in unique_post_ids]
    return ans


def insert(data: List[Dict[str, str]]) -> None:
    if (data is None) or (len(data) == 0):
        print("Nothing inserted")
        return
    else:
        for i in data:
            table.put_item(Item=i)
    return


def lambda_handler(event, context) -> None:
    data: List[Dict[str, str]] = get_data(get_secret())
    unique_post_ids: Set[str] = scan_table()
    ans: List[Dict[str, str]] = check_duplication(data=data, unique_post_ids=unique_post_ids)
    insert(data=ans)


if __name__ == "__main__":
    lambda_handler(None, None)