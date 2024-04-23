from typing import Dict, List, Set
import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from supabase import create_client, Client
import logging

dynamoDB = boto3.resource(service_name="dynamodb")
table = dynamoDB.Table(name="hpe_content_buffer")
logger = logging.getLogger()


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
    yesterday_datetime = datetime.now() - timedelta(days=4)

    client: Client = create_client(secrets["SUPABASE_URL"], secrets["SUPABASE_SERVICE_ROLE_KEY"])
    response = client.table("dim_posts").select("post_id, post_title").gte("post_date_jst", yesterday_datetime).execute()
    ans: List[Dict[str, str]] = [
        {
            "post_id": i["post_id"],
            "post_title": i["post_title"],
            "post_url": f"https://healthy-person-emulator.org/archives/{i['post_id']}"
        }
        for i in response.data]
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
    try:
        data: List[Dict[str, str]] = get_data(get_secret())
        unique_post_ids: Set[str] = scan_table()
        ans: List[Dict[str, str]] = check_duplication(data=data, unique_post_ids=unique_post_ids)
        insert(data=ans)
        logger.setLevel("INFO")
        logger.info(f"Successfully inserted, {len(ans)} items inserted.")
    except Exception as e:
        logger.setLevel("ERROR")
        logger.error(f"Error occurred: {e}")
        raise e


if __name__ == "__main__":
    lambda_handler(None, None)