from atproto import Client, models
import requests
import json
import boto3

def download_image(s3_url):
    response = requests.get(s3_url).content
    return response

def get_bluesky_credentials():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="hpe-bluesky-bot-tokens")
    secrets = json.loads(secret_value["SecretString"])
    return secrets

def create_post_text(post_title):
    text = f"【新規記事】 : {post_title}"
    return text

def lambda_handler(event, context):
    if type(event["Records"][0]["Sns"]["Message"]) == str:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
    elif type(event["Records"][0]["Sns"]["Message"]) == dict:
        message = event["Records"][0]["Sns"]["Message"]
    else:
        raise ValueError("Message type is not str or dict")

    post_title = message["post_title"]
    post_url = message["post_url"]
    og_url = message["og_url"]
    secrets = get_bluesky_credentials()
    image_data = download_image(og_url)
    bluesky_client = Client(base_url='https://bsky.social')
    bluesky_client.login(secrets["useraddress"], secrets["password"])

    thumbnail = bluesky_client.upload_blob(image_data)

    post_text = create_post_text(post_title)

    embed = models.AppBskyEmbedExternal.Main(
        external=models.AppBskyEmbedExternal.External(
            title=post_title,
            uri=post_url,
            thumb=thumbnail.blob,
            description="",
        )
    )

    post = bluesky_client.send_post(text=post_text,embed=embed)
    return