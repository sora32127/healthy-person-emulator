from atproto import Client, models
import requests
import json
import boto3
from logging import getLogger

import supabase

logger = getLogger()

def download_image(s3_url):
    response = requests.get(s3_url).content
    return response

def get_bluesky_credentials():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="hpe-bluesky-bot-tokens")
    secrets = json.loads(secret_value["SecretString"])
    return secrets

def create_post_text(post_title, message_type):
    if message_type == "new":
        text = f"【新規記事】 : {post_title}"
    elif message_type == "legendary":
        text = f"【殿堂入り】 : {post_title}"
    elif message_type == "random":
        text = f"【ランダム】 : {post_title}"
    else:
        raise ValueError("Unknown message type")
    return text

def get_message(event):
    if type(event["Records"][0]["Sns"]["Message"]) == str:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
    elif type(event["Records"][0]["Sns"]["Message"]) == dict:
        message = event["Records"][0]["Sns"]["Message"]
    else:
        raise ValueError("Message type is not str or dict")
    return message

def get_infomation_from_message(message):
    post_title = message["post_title"]
    post_url = message["post_url"]
    og_url = message["og_url"]
    message_type = message["message_type"]
    post_id = message["post_id"]
    return post_title, post_url, og_url, message_type, post_id

def get_credentials_of_db():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="SUPABASE_CONNECTION_SECRET")
    secrets = json.loads(secret_value["SecretString"])
    return secrets


def save_post_uri_to_db(bluesky_post_uri, message_type, post_id):
    if message_type != "new":
        logger.info(f"message_type is {message_type}, not new. bluesky_post_uri: {bluesky_post_uri} is not saved to db.")
        return
    credentials_of_db = get_credentials_of_db()
    client = supabase.create_client(credentials_of_db["SUPABASE_URL"], credentials_of_db["SUPABASE_SERVICE_ROLE_KEY"])
    client.table("dim_posts").update({"bluesky_post_uri_of_first_post": bluesky_post_uri}).eq("post_id", post_id).execute()
    logger.info(f"bluesky_post_uri: {bluesky_post_uri} is saved to db. post_id: {post_id}")
    return

def lambda_handler(event, context):
    try:
        message = get_message(event)
        post_title, post_url, og_url, message_type, post_id = get_infomation_from_message(message)

        secrets = get_bluesky_credentials()

        image_data = download_image(og_url)
        bluesky_client = Client(base_url='https://bsky.social')
        bluesky_client.login(secrets["useraddress"], secrets["password"])

        thumbnail = bluesky_client.upload_blob(image_data)

        post_text = create_post_text(post_title, message_type)

        embed = models.AppBskyEmbedExternal.Main(
            external=models.AppBskyEmbedExternal.External(
                title=post_title,
                uri=post_url,
                thumb=thumbnail.blob,
                description="",
            )
        )

        post = bluesky_client.send_post(text=post_text,embed=embed)
        bluesky_post_uri = post.uri
        save_post_uri_to_db(bluesky_post_uri, message_type, post_id)
        logger.info(f"post_title: {post_title} is successfully posted to BlueSky. post_uri: {bluesky_post_uri}")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise e
    
if __name__ == "__main__":
    test_event = {
        "Records": [
            {
                "Sns": {
                    "Message": json.dumps(
                        {
                            "post_title": "無神論者の火",
                            "post_url": "https://healthy-person-emulator.org/archives/23576",
                            "og_url": "https://healthy-person-emulator-public-assets.s3-ap-northeast-1.amazonaws.com/23576.jpg",
                            "message_type": "new",
                            "post_id": 23576
                        }
                    )
                }
            }
        ]
    }
    lambda_handler(test_event, None)