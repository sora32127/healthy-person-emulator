from bs4 import BeautifulSoup
from PIL import Image, ImageDraw, ImageFont
import textwrap
from typing import Final, Dict, List
import boto3
import json
from botocore.exceptions import ClientError
import re
from supabase import create_client, Client
import logging
import datetime
import os


IS_PRODUCTION = os.getenv('AWS_LAMBDA_FUNCTION_NAME') is not None

S3_BUCKET_NAME: Final[str] = "healthy-person-emulator-public-assets"
FONT_FILE_PATH: Final[str] = "./BIZ-UDGOTHICB.TTC" if IS_PRODUCTION else "ServerlessFramework/CreateOGImage/BIZ-UDGOTHICB.TTC"
TEMP_FILE_PATH: Final[str] = "/tmp/{}.jpg" if IS_PRODUCTION else "./tmp/{}.jpg"

logger = logging.getLogger()

IMAGE_WIDTH: Final[int] = 1200
IMAGE_HEIGHT: Final[int] = 630
KEY_COLUMN_WIDTH: Final[int] = 220
CONTENT_COLUMN_WIDTH: Final[int] = IMAGE_WIDTH - KEY_COLUMN_WIDTH
MARGIN: Final[int] = 20
LINE_HEIGHT: Final[int] = 30  # フォントサイズ20px + 行間10px
LINE_MARGIN: Final[int] = LINE_HEIGHT - MARGIN
FONT_SIZE: Final[int] = 20

def get_supabase_secret():

    secret_name = "SUPABASE_CONNECTION_SECRET"
    region_name = "ap-northeast-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

def poll_supabase_for_new_posts(secrets):
    client: Client = create_client(secrets["SUPABASE_URL"], secrets["SUPABASE_SERVICE_ROLE_KEY"])
    if not IS_PRODUCTION:
        test_posts = client.table("dim_posts").select("post_id,post_content,post_title").eq("post_id", 20028).execute()
        return test_posts.data
    one_day_ago = datetime.datetime.now() - datetime.timedelta(hours=24)
    posts = client.table("dim_posts").select("post_id,post_content,post_title").gte('post_date_gmt', one_day_ago).eq("is_sns_shared", False).eq("is_welcomed", True).execute()
    return posts.data

def get_text_data(post_content: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(post_content, "html.parser")
    table_data_raw = soup.find("table").find_all("td")
    table_data = {
        table_data_raw[2 * i].text: table_data_raw[2 * i + 1].text
        for i in range(len(table_data_raw) // 2)
    }
    return table_data


def get_image(
    table_data: Dict[str, str], post_id: int
) -> None:

    try:
        with open(FONT_FILE_PATH):
            pass
    except FileNotFoundError:
        raise FileNotFoundError("The specified font file does not exist.")

    font = ImageFont.truetype(FONT_FILE_PATH, FONT_SIZE)

    # 固定サイズの画像を作成
    im = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (255, 255, 255))
    draw = ImageDraw.Draw(im)

    # 縦線の描画（keyカラムとcontentカラムの区切り）
    draw.line(
        [(KEY_COLUMN_WIDTH, 0), (KEY_COLUMN_WIDTH, IMAGE_HEIGHT)],
        fill=(0, 0, 0),
        width=1,
    )

    # 利用可能な最大行数を計算
    max_lines = (IMAGE_HEIGHT - (2 * MARGIN)) // LINE_HEIGHT

    y_position = MARGIN
    for key, content in table_data.items():
        if y_position + LINE_HEIGHT > IMAGE_HEIGHT - MARGIN:
            break
        y_position += LINE_MARGIN

        # keyの折り返しと描画
        key_lines = textwrap.wrap(key, width=int(KEY_COLUMN_WIDTH/FONT_SIZE))  # 300pxに収まる概算の文字数
        y_position_tmp_key = y_position
        for key_line in key_lines:
            draw.text((MARGIN, y_position_tmp_key), key_line, font=font, fill=(0, 0, 0))
            y_position_tmp_key += LINE_HEIGHT

        # contentの折り返しと描画
        y_position_tmp_content = y_position
        content_lines = textwrap.wrap(content, width=int(CONTENT_COLUMN_WIDTH/FONT_SIZE))  # 900pxに収まる概算の文字数
        for content_line in content_lines:
            draw.text((KEY_COLUMN_WIDTH + MARGIN, y_position_tmp_content), content_line, font=font, fill=(0, 0, 0))
            y_position_tmp_content += LINE_HEIGHT


        y_position = max(y_position, y_position_tmp_content)

        # 横線の描画（最終行以外）
        if y_position < IMAGE_HEIGHT - MARGIN and key != list(table_data.keys())[-1]:
            draw.line(
                [(0, y_position), (IMAGE_WIDTH, y_position)],
                fill=(0, 0, 0),
                width=1,
            )

    im.save(TEMP_FILE_PATH.format(post_id), quality=95)
    
    if not IS_PRODUCTION:
        return

    s3 = boto3.client("s3")
    s3.upload_file(
        TEMP_FILE_PATH.format(post_id),
        S3_BUCKET_NAME,
        "{}.jpg".format(post_id)
    )

def update_postgres_ogp_url(post_id:int, s3_url:str, secrets:Dict[str,str]):
    client: Client = create_client(secrets["SUPABASE_URL"], secrets["SUPABASE_SERVICE_ROLE_KEY"])
    client.table("dim_posts").update({"ogp_image_url": s3_url}).eq("post_id",post_id).execute()
    client.table("dim_posts").update([{"is_sns_shared": True}]).eq("post_id",post_id).execute()
    return

def invoke_sns_post(post_title, post_url, og_url, post_id):
    sns = boto3.client("sns")
    response = sns.publish(
        TopicArn="arn:aws:sns:ap-northeast-1:662924458234:healthy-person-emulator-socialpost",
        Message=json.dumps({
            "post_title": post_title,
            "post_url": post_url,
            "og_url": og_url,
            "message_type": "new",
            "post_id": post_id
        })
    )
    return

def lambda_handler(event, context):
    try:
        secrets = get_supabase_secret()
        posts = poll_supabase_for_new_posts(secrets)
        if len(posts) == 0:
            logger.setLevel("INFO")
            logger.info("There are no posts to create OG Image.")
            return
    
        for post in posts:
            post_id = post["post_id"]
            post_title = post["post_title"]
            post_content = post["post_content"]
            post_url = f"https://healthy-person-emulator.org/archives/{post_id}"
            if re.match(r"^.*プログラムテスト.*$", post_title):
                continue
            table_data = get_text_data(
                post_content=post_content
            )            
            get_image(post_id=post_id, table_data=table_data)
            s3_url = f"https://{S3_BUCKET_NAME}.s3-ap-northeast-1.amazonaws.com/{post_id}.jpg"
            
            if not IS_PRODUCTION:
                return
            update_postgres_ogp_url(post_id=post_id, s3_url=s3_url, secrets=secrets)
            post_url = f"https://healthy-person-emulator.org/archives/{post_id}"
            invoke_sns_post(post_title=post_title, post_url=post_url, og_url=s3_url, post_id=post_id)
            logger.setLevel("INFO")
            logger.info(f"post_id: {post_id} is successfully created OG Image.")
    except Exception as e:
        logger.setLevel("ERROR")
        logger.error(e)
        raise e
    

if __name__ == "__main__":
    lambda_handler(None, None)