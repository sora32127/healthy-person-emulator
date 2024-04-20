from google.cloud import bigquery
import boto3
import json
from google.oauth2 import service_account
import tweepy

def get_credentials():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="BIGQUERY_ACCESS_CREDENTIAL")
    secrets = json.loads(secret_value["SecretString"])
    credentials = service_account.Credentials.from_service_account_info(secrets)
    return credentials

def get_weekly_summary_data(credentials):
    client = bigquery.Client(credentials=credentials)
    query = """
        SELECT
            *
        FROM
            `healthy-person-emulator.dbt_sora32127.report_weekly_summary`
    """
    res = client.query(query)
    ans = []
    for row in res:
        row_dict = {
            'post_id': row['post_id'],
            'post_title': row['post_title'],
            'post_date_jst': row['post_date_jst'].isoformat(),
            'vote_count': row['vote_count']
        }
        ans.append(row_dict)
    
    return ans

def create_tweet_text(weekly_summary_data):
    tweet_text = "【今週の人気投稿】\n"
    for i in range(min(3, len(weekly_summary_data))):
        post = weekly_summary_data[i]
        tweet_text += f"\n{i+1} : {post['post_title']} \nhttps://healthy-person-emulator.org/archives/{post['post_id']}\n"
    return tweet_text

def get_twitter_credentials():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="hpe-twitter-bot-tokens")
    secrets = json.loads(secret_value["SecretString"])
    return secrets

def post_tweet(tweet_text):
    secrets = get_twitter_credentials()
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

    client.create_tweet(text=tweet_text)

def lambda_handler(event, context):
    credentials = get_credentials()
    weekly_summary_data = get_weekly_summary_data(credentials)
    tweet_text = create_tweet_text(weekly_summary_data)
    post_tweet(tweet_text)

if __name__ == "__main__":
    lambda_handler(None, None)

