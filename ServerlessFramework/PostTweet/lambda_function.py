import json
import tweepy
import requests
import logging
import boto3

logger = logging.getLogger()

def get_twitter_credentials():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="hpe-twitter-bot-tokens")
    secrets = json.loads(secret_value["SecretString"])
    return secrets

def post_tweet(post_text, secrets):
    consumer_key = secrets["CK"]
    consumer_secret = secrets["CS"]
    access_token = secrets["AT"]
    access_token_secret = secrets["ATS"]

    auth = tweepy.OAuth1UserHandler(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )
    client = tweepy.Client(
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        access_token=access_token,
        access_token_secret=access_token_secret,
    )
    api = tweepy.API(auth)
    media = api.media_upload(
        filename="/tmp/og_image.jpg"
    )  # apiv1とv2を併用している
    tweet = client.create_tweet(text=post_text, media_ids=[media.media_id])

def download_image(url):
    response = requests.get(url).content
    with open("/tmp/og_image.jpg", "wb") as f:
        f.write(response)

def get_infomation_from_message(message):
    post_title = message["post_title"]
    post_url = message["post_url"]
    og_url = message["og_url"]
    message_type = message["message_type"]
    return post_title, post_url, og_url, message_type

def create_post_text(post_title, post_url, message_type):
    type_prefix = {
        "new": "新規記事",
        "legendary": "殿堂入り",
        "random": "ランダム"
    }
    if message_type not in type_prefix:
        raise ValueError("Unknown message type")
    
    return f"[{type_prefix[message_type]}] : {post_title} 健常者エミュレータ事例集\n{post_url}"

def lambda_handler(event, context):
    try:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        post_title, post_url, og_url, message_type = get_infomation_from_message(message)
        secrets = get_twitter_credentials()
        download_image(og_url)
        post_text = create_post_text(post_title, post_url, message_type)
        post_tweet(post_text, secrets)
        logger.setLevel("INFO")
        logger.info(f"post_title: {post_title} is successfully tweeted.")
    except Exception as e:
        logger.setLevel("ERROR")
        logger.error(e)
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
                            "message_type": "new"
                        }
                    )
                }
            }
        ]
    }
    lambda_handler(test_event, None)
