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

def post_note_to_misskey(post_title, post_url, uploaded_file_id, mk):
    mk.notes_create(
        text=f"新規記事 : {post_title} 健常者エミュレータ事例集\n{post_url}",
        file_ids=[uploaded_file_id],
    )

def lambda_handler(event, context):
    try:
        message = json.loads(event["Records"][0]["Sns"]["Message"])
        og_url = message["og_url"]
        post_title = message["post_title"]
        post_url = message["post_url"]
        misskey_secret = get_misskey_secret()
        mk = Misskey('https://misskey.io', i = misskey_secret)
        download_image(og_url)
        uploaded_file_id = upload_image_to_misskey(mk)
        post_note_to_misskey(post_title, post_url, uploaded_file_id, mk)
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
                        }
                    )
                }
            }
        ]
    }
    lambda_handler(test_event, None)