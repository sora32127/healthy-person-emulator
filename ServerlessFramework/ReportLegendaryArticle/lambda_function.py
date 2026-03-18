from google.cloud import bigquery
import boto3
import json
from google.oauth2 import service_account
import tweepy
import urllib.request
import logging

logger = logging.getLogger()

API_BASE_URL = "https://healthy-person-emulator.org"

def get_bigquery_credentials():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="BIGQUERY_ACCESS_CREDENTIAL")
    secrets = json.loads(secret_value["SecretString"])
    credentials = service_account.Credentials.from_service_account_info(secrets)
    return credentials

def get_legendary_article_data(credentials):
    client = bigquery.Client(credentials=credentials)
    query = """
        SELECT
            *
        FROM
            `healthy-person-emulator.dbt_sora32127.report_new_legend_posts`
    """
    res = client.query(query)
    ans = []
    for row in res:
        row_dict = {
            'post_id': row['post_id'],
            'post_title': row['post_title'],
            'post_url': f"https://healthy-person-emulator.org/archives/{row['post_id']}",
        }
        ans.append(row_dict)
    return ans

def get_api_key():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="HPE_INTERNAL_API")
    secrets = json.loads(secret_value["SecretString"])
    return secrets["INTERNAL_API_KEY"]

def update_tags_via_api(legendary_article_data, api_key):
    for article in legendary_article_data:
        try:
            body = json.dumps({"postId": article["post_id"], "tagId": 575}).encode()
            req = urllib.request.Request(
                f"{API_BASE_URL}/api/internal/add-tag-to-post",
                data=body,
                headers={"X-API-Key": api_key, "Content-Type": "application/json", "User-Agent": "HPE-Internal-Lambda/1.0"},
                method="POST"
            )
            with urllib.request.urlopen(req) as res:
                res.read()
        except Exception as e:
            print(e)
            raise e


def get_twitter_credentials():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="hpe-twitter-bot-tokens")
    secrets = json.loads(secret_value["SecretString"])
    return secrets

def post_tweet(legendary_article_data, secrets):
    for article in legendary_article_data:
        title = article["post_title"]
        url = article["post_url"]
        text = f"[殿堂入り] : {title} 健常者エミュレータ事例集 \n{url}"
        consumer_key = secrets["CK"]
        consumer_secret = secrets["CS"]
        access_token = secrets["AT"]
        access_token_secret = secrets["ATS"]

        client = tweepy.Client(
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            access_token=access_token,
            access_token_secret=access_token_secret,
        )
        client.create_tweet(text=text)

def lambda_handler(event, context):
    try:
        bigquery_credentials = get_bigquery_credentials()
        legendary_article_data = get_legendary_article_data(bigquery_credentials)
        api_key = get_api_key()
        update_tags_via_api(legendary_article_data, api_key)
        twitter_credentials = get_twitter_credentials()
        post_tweet(legendary_article_data, twitter_credentials)
        logger.setLevel("INFO")
        logger.info(f"Legendary articles are successfully updated.{len(legendary_article_data)} articles.")
    except Exception as e:
        logger.setLevel("ERROR")
        logger.error(e)
        raise e


if __name__ == '__main__':
    lambda_handler(None, None)
