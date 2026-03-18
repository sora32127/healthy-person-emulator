import json
import logging
import urllib.request
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

API_BASE_URL = "https://healthy-person-emulator.org"

def get_secret():
    secretmanager = boto3.client('secretsmanager')
    secret = secretmanager.get_secret_value(SecretId='HPE_INTERNAL_API')
    return json.loads(secret['SecretString'])

def get_message(message):
    post_id = message["post_id"]
    social_type = message["social_type"]
    social_post_id = message["social_post_id"]
    return post_id, social_type, social_post_id

def save_sns_ids_to_db(api_key, post_id, social_type, social_post_id):
    update_data = {"postId": post_id}
    if social_type == "twitter":
        update_data["tweetId"] = social_post_id
    elif social_type == "bluesky":
        update_data["blueskyPostUri"] = social_post_id
    elif social_type == "misskey":
        update_data["misskeyNoteId"] = social_post_id

    body = json.dumps(update_data).encode()
    req = urllib.request.Request(
        f"{API_BASE_URL}/api/internal/update-social-ids",
        data=body,
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req) as res:
        res.read()
    logger.info(f"{social_type}_id is saved to db. post_id: {post_id}, id: {social_post_id}")

def lambda_handler(event, context):
    try:
        secret = get_secret()
        api_key = secret['INTERNAL_API_KEY']
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        post_id, social_type, social_post_id = get_message(message)
        save_sns_ids_to_db(api_key, post_id, social_type, social_post_id)
    except Exception as e:
        print(e)
        logger.error(e)
        raise e
    return
