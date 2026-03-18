import json
import boto3
import urllib.request
from logging import getLogger

logger = getLogger()
logger.setLevel("INFO")

API_BASE_URL = "https://healthy-person-emulator.org"

def get_secret():
    secretmanager = boto3.client('secretsmanager')
    secret = secretmanager.get_secret_value(SecretId='HPE_INTERNAL_API')
    return json.loads(secret['SecretString'])

def get_random_article(api_key):
    req = urllib.request.Request(
        f"{API_BASE_URL}/api/internal/posts-for-pickup",
        headers={"X-API-Key": api_key}
    )
    with urllib.request.urlopen(req) as res:
        data = json.loads(res.read())
    return data['posts'][0]

def mark_picked_up(api_key, post_id):
    body = json.dumps({"postId": post_id}).encode()
    req = urllib.request.Request(
        f"{API_BASE_URL}/api/internal/mark-picked-up",
        data=body,
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req) as res:
        res.read()

def publish_to_sns(article):
    message = {
        "post_title": article['postTitle'],
        "post_url": f"https://healthy-person-emulator.org/archives/{article['postId']}",
        "og_url": article['ogpImageUrl'],
        "message_type": "random"
    }
    sns = boto3.client('sns')
    sns.publish(TopicArn='arn:aws:sns:ap-northeast-1:662924458234:healthy-person-emulator-socialpost', Message=json.dumps(message))

def lambda_handler(event, context):
    try:
        secret = get_secret()
        api_key = secret['INTERNAL_API_KEY']
        article = get_random_article(api_key)
        mark_picked_up(api_key, article['postId'])
        publish_to_sns(article)
        logger.info(f"Article {article['postId']} picked up")
    except Exception as e:
        logger.error(e)
        raise e

if __name__ == '__main__':
    lambda_handler(None, None)
