import boto3
import json
import logging
import supabase

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def get_message(message):
    post_id = message["post_id"]
    social_type = message["social_type"]
    social_post_id = message["social_post_id"]
    return post_id, social_type, social_post_id

def get_credentials_of_db():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="SUPABASE_CONNECTION_SECRET")
    secrets = json.loads(secret_value["SecretString"])
    return secrets

def save_sns_ids_to_db(post_id, social_type, social_post_id):
    credentials_of_db = get_credentials_of_db()
    client = supabase.create_client(credentials_of_db["SUPABASE_URL"], credentials_of_db["SUPABASE_SERVICE_ROLE_KEY"])
    try:
        if social_type == "twitter":
            client.table("dim_posts").update({"tweet_id_of_first_tweet": social_post_id}).eq("post_id", post_id).execute()
            logger.info(f"tweet_id is saved to db. post_id: {post_id}, tweet_id: {social_post_id}")
        elif social_type == "bluesky":
            client.table("dim_posts").update({"bluesky_post_uri_of_first_post": social_post_id}).eq("post_id", post_id).execute()
            logger.info(f"bluesky_post_uri is saved to db. post_id: {post_id}, bluesky_post_uri: {social_post_id}")
        elif social_type == "misskey":
            client.table("dim_posts").update({"misskey_note_id_of_first_note": social_post_id}).eq("post_id", post_id).execute()
            logger.info(f"misskey_note_id is saved to db. post_id: {post_id}, misskey_note_id: {social_post_id}")
    except Exception as e:
        logger.error(e)
        raise e
    return

def lambda_handler(event, context):
    try:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        post_id, social_type, social_post_id = get_message(message)
        save_sns_ids_to_db(post_id, social_type, social_post_id)
        logger.info(f"sns_ids are saved to db. post_id: {post_id}, social_type: {social_type}, social_post_id: {social_post_id}")
    except Exception as e:
        print(e)
        logger.error(e)
        raise e
    return
