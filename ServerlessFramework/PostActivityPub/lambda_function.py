from misskey import Misskey
import json
import boto3
import httpx
import logging

logger = logging.getLogger()


tmp_file_path = "/tmp/og_image.jpg"

def get_misskey_secret():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="MISSKEY_TOKEN")
    secrets = json.loads(secret_value["SecretString"])
    misskey_token = secrets["MISSKEY_IO_TOKEN"]
    return misskey_token

def download_image(url):
    response = httpx.get(url).content
    with open(tmp_file_path, "wb") as f:
        f.write(response)

def upload_image_to_misskey(mk):
    with open(tmp_file_path, "rb") as f:
        data = mk.drive_files_create(f)
    return data["id"]

def create_post_text(post_title, post_url, message_type):
    type_prefix = {
        "new": "新規記事",
        "legendary": "殿堂入り",
        "random": "ランダム"
    }
    if message_type not in type_prefix:
        raise ValueError("Unknown message type")
    
    return f"[{type_prefix[message_type]}] : {post_title} 健常者エミュレータ事例集\n{post_url}"

def post_note_to_misskey(post_text, uploaded_file_id, mk) -> str:
    note = mk.notes_create(
        text=post_text,
        file_ids=[uploaded_file_id],
    )
    note_id = note["createdNote"]["id"]
    return note_id


def get_infomation_from_message(message):
    post_title = message["post_title"]
    post_url = message["post_url"]
    og_url = message["og_url"]
    message_type = message["message_type"]
    post_id = message["post_id"]
    return post_title, post_url, og_url, message_type, post_id

def send_event_to_sns(post_id, social_post_id):
    sns_client = boto3.client("sns")
    sns_client.publish(
        TopicArn="arn:aws:sns:ap-northeast-1:662924458234:healthy-person-emulator-socialpostIds",
        Message=json.dumps({"post_id": post_id, "social_post_id": social_post_id, "social_type": "misskey"})
    )

def lambda_handler(event, context):
    try:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        print(f"Message is {message}")
        post_title, post_url, og_url, message_type, post_id = get_infomation_from_message(message)
        misskey_secret = get_misskey_secret()
        mk = Misskey('https://misskey.io', i = misskey_secret)
        download_image(og_url)
        uploaded_file_id = upload_image_to_misskey(mk)
        post_text = create_post_text(post_title, post_url, message_type)
        note_id = post_note_to_misskey(post_text, uploaded_file_id, mk)
        send_event_to_sns(post_id, note_id)
        logger.setLevel("INFO")
        logger.info(f"post_title: {post_title} is successfully posted.")
    except Exception as e:
        logger.setLevel("ERROR")
        logger.error(e)
        raise e


if __name__ == "__main__":
    test_event = {
        "Records": [
            {
                "Sns": {
                    "Message":json.dumps(
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