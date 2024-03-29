import tweepy
from datetime import datetime
from bs4 import BeautifulSoup
import requests
from PIL import Image, ImageDraw, ImageFont
import textwrap
from typing import Final, Dict
import boto3
import json

FONT_FILE_PATH: Final[str] = "./BIZ-UDGOTHICB.TTC"
TIMESTAMP_STRING: Final[str] = datetime.now().strftime("%Y%m%d%H%M%S")


def get_image(url: str) -> None:
    # 画像の元データを取得する：開始
    res = requests.get(url=url, timeout=10).text
    soup = BeautifulSoup(res, "html.parser")
    table_data_raw = soup.find("table").find_all("td")
    table_data = {
        table_data_raw[2 * i].text: table_data_raw[2 * i + 1].text
        for i in range(len(table_data_raw) // 2)
    }
    # 画像の元データを取得する：終了

    if "Who(誰が)" in table_data.keys():
        # 通常の場合は1:2の比率で画像を作成する
        get_image_by_ratio(1 / 3, 2 / 3, table_data)
    else:
        # 例外の場合は1:1の比率で画像を作成する
        get_image_by_ratio(1 / 2, 1 / 2, table_data)


def get_image_by_ratio(
    key_length_ratio: float, content_length_ratio: float, table_data: Dict[str, str]
) -> None:
    if key_length_ratio + content_length_ratio != 1:
        raise ValueError(
            "The sum of key_length_ratio and content_length_ratio must be 1."
        )
    if key_length_ratio <= 0 or content_length_ratio <= 0:
        raise ValueError("key_length_ratio and content_length_ratio must be positive.")
        # 大枠の画像を作成する：開始

    # 30かけて、整数ではない場合はエラーを出す
    candidate_list = [i / 30 for i in range(1, 31)]
    if key_length_ratio not in candidate_list:
        raise ValueError("key_length_ratio must be a multiple of 30.")

    font = ImageFont.truetype(FONT_FILE_PATH, 20)

    ## 画像の高さを計算する
    image_hight = 0
    for key, content in table_data.items():
        key_line_count = len(textwrap.wrap(key, width=int(30 * key_length_ratio)))
        content_line_count = len(
            textwrap.wrap(content, width=int(30 * content_length_ratio))
        )
        line_count = max(key_line_count, content_line_count)
        image_hight += line_count * 20
        image_hight += 10  # 余白として、上に5px、下に5px追加する

    ## 画像の高さの計算：終了
    image_width = (
        620  # 600 + 左余白5px + keyとcontentの間の線の左右に5pxずつ + 右余白5px
    )
    im = Image.new("RGB", (image_width, image_hight), (255, 255, 255))
    draw = ImageDraw.Draw(im)
    ## 縦線の描画
    draw.line(
        [(int(620 * key_length_ratio), 0), (int(620 * key_length_ratio), image_hight)],
        fill=(0, 0, 0),
        width=1,
    )

    # 大枠の画像を作成する：終了

    # テキストを描画する：開始
    y_position = 0
    for key, content in table_data.items():
        y_position += 5  # 余白として、上に5px追加する
        y_position_tmp = y_position

        ## keyの描画
        key_lines = textwrap.wrap(key, width=int(30 * key_length_ratio))
        y_position_tmp_key = y_position_tmp
        for line in key_lines:
            draw.text((5, y_position_tmp_key), line, font=font, fill=(0, 0, 0))
            y_position_tmp_key += 20

        ## contentの描画
        content_lines = textwrap.wrap(content, width=int(30 * content_length_ratio))
        y_position_tmp_content = y_position_tmp
        for line in content_lines:
            draw.text(
                (620 * key_length_ratio + 5, y_position_tmp_content),
                line,
                font=font,
                fill=(0, 0, 0),
            )
            y_position_tmp_content += 20

        y_position = max(y_position_tmp_key, y_position_tmp_content)
        y_position += 5  # 余白として、下に5px追加する

        ## 横線の描画は最終要素でない場合にのみ行う
        if key != list(table_data.keys())[-1]:
            draw.line([(0, y_position), (600, y_position)], fill=(0, 0, 0), width=1)

    im.save("/tmp/{}.jpg".format(TIMESTAMP_STRING), quality=95)


def get_secrets() -> Dict[str, str]:
    secret_manager_clinet = boto3.client("secretsmanager")
    response = secret_manager_clinet.get_secret_value(SecretId="hpe-twitter-bot-tokens")

    payload = json.loads(response["SecretString"])
    return {
        "CK": payload["CK"],
        "CS": payload["CS"],
        "AT": payload["AT"],
        "ATS": payload["ATS"],
    }


def post_tweet(title, url, secrets):
    text = f"[新規記事] : {title} 健常者エミュレータ事例集 {url}"
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
        filename="/tmp/{}.jpg".format(TIMESTAMP_STRING)
    )  # apiv1とv2を併用している
    tweet = client.create_tweet(text=text, media_ids=[media.media_id])


def lambda_handler(event, context):
    title: str = event["Records"][0]["dynamodb"]["Keys"]["title"]["S"]
    url: str = event["Records"][0]["dynamodb"]["Keys"]["url"]["S"]
    get_image(url=url)
    post_tweet(title=title, url=url, secrets=get_secrets())


if __name__ == "__main__":
    lambda_handler({}, {})
