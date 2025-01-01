import math
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
FONT_FILE_PATH: Final[str] = "./NotoSansJP-VariableFont_wght.ttf" if IS_PRODUCTION else "ServerlessFramework/CreateOGImage/NotoSansJP-VariableFont_wght.ttf"
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
    one_day_ago = datetime.datetime.now() - datetime.timedelta(hours=24)
    posts = client.table("dim_posts").select("post_id,post_content,post_title").gte('post_date_gmt', one_day_ago).eq("is_sns_shared", False).eq("is_welcomed", True).execute()
    data = posts.data
    data.post_content = get_text_data(data.post_content)
    return data

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
    im = Image.new("RGB", (IMAGE_WIDTH, IMAGE_HEIGHT), (245, 245, 245))
    draw = ImageDraw.Draw(im)

    # 縦線の描画（keyカラムとcontentカラムの区切り）
    draw.line(
        [(KEY_COLUMN_WIDTH, 0), (KEY_COLUMN_WIDTH, IMAGE_HEIGHT)],
        fill=(0, 0, 0),
        width=1,
    )

    # エントリごとの高さを計算
    total_entries = len(table_data)
    available_height = IMAGE_HEIGHT - (2 * MARGIN)  # 上下マージンを除いた利用可能な高さ
    entry_height = available_height // total_entries
    inner_margin = 10  # エントリ内の上下マージン

    # 各エントリの描画
    for index, (key, content) in enumerate(table_data.items()):
        # エントリの開始Y座標を計算
        y_position = MARGIN + (index * entry_height)
        
        # キーの描画
        key_lines = textwrap.wrap(key, width=15)  # 300pxに収まる概算の文字数
        if key_lines:
            if len(key_lines) > 2:
                key_text = key_lines[0] + "..."
            else:
                key_text = key_lines[0]
            draw.text(
                (MARGIN, y_position + inner_margin),
                key_text,
                font=font,
                fill=(0, 0, 0)
            )

        # コンテンツの描画
        content_lines = textwrap.wrap(content, width=45)  # 900pxに収まる概算の文字数
        if content_lines:
            if len(content_lines) > 2:
                content_text = content_lines[0] + "\n" + content_lines[1] + "..."
            elif len(content_lines) == 2:
                content_text = content_lines[0] + "\n" + content_lines[1]
            else:
                content_text = content_lines[0]
            draw.text(
                (KEY_COLUMN_WIDTH + MARGIN, y_position + inner_margin),
                content_text,
                font=font,
                fill=(0, 0, 0)
            )

        # 横線の描画（最終エントリ以外）
        if index < total_entries - 1:
            line_y = y_position + entry_height
            draw.line(
                [(0, line_y), (IMAGE_WIDTH, line_y)],
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


test_posts = [
    {
        "post_id": 1,
        "post_title": "（通常）",
        "post_content": {
        "Who(誰が)": "筆者が",
        "When(いつ)": "数年前",
        "Where(どこで)": "実家に帰省した際",
        "Why(なぜ)": "子供を作ることは子供の未来の幸せを願ってではなく、遺伝子を残すためだけの行為だと考えているので",
        "What(何を)": "迎えに来てくれた父親に対して",
        "How(どのように)": "兄貴が結婚して子供残しそうだから、スペアの弟である僕は要らなかったねと発言した",
        "Then(どうなった)": "「そうだな」と悲しそうに肯定された"
    }
    },
    {
        "post_id": 2,
        "post_title": "（長文）",
        "post_content": {
            "Who(誰が)": "これは長いキーに対応する通常の内容です",
            "When(いつ)": "この文章は非常に長い内容を含んでおり、一行に収まらないようになっています。具体的には、先週の木曜日の午後3時15分ごろのことでした。",
            "Where(どこで)": "場所は東京都新宿区の某有名カフェで、窓際の席に座っていた時のことです。外は小雨が降っていて、傘を持っていなかったことを少し後悔していました。",
            "What(何を)": "このテストデータは、システムが長文をどのように処理するかを確認するためのものです。特に、省略記号（...）が適切に表示されるかどうかを検証します。",
            "How(どのように)": "突然、隣のテーブルから大きな物音がしたかと思うと、誰かが「すみません！」と叫ぶ声が聞こえました。振り返ってみると、若い女性がコーヒーをこぼしてしまったようでした。",
            "Then(どうなった)": "結果として、このテストケースによって、システムが長文を適切に処理し、見やすい形で表示できることが確認できました。また、複数行のテキストが省略記号で適切に切り詰められることも確認できました。"
        }
    },
    {
        "post_id": 3,
        "post_title": "（超長文）",
        "post_content": {
            "Who(誰が)": "これは超長いキーに対応するコンテンツです。",
            "When(いつ)": "この出来事は、2024年2月14日のバレンタインデーの午後3時15分ごろのことでした。その日は珍しく雪が降っていて、東京の街中が真っ白に染まっていました。普段は見られない光景に、多くの人々が足を止めて空を見上げていたのを今でも鮮明に覚えています。",
            "Where(どこで)": "場所は東京都新宿区の某有名カフェで、窓際の席に座っていた時のことです。外は小雨が降っていて、傘を持っていなかったことを少し後悔していました。ちょうどその時、向かいのビルでは大規模な工事が行われており、時折大きな物音が聞こえてきていました。",
            "Why(なぜ)": "なぜならば、この文章はシステムの限界をテストするために書かれた特別に長い文章だからです。システムがどのように長文を処理するのか、文字列の切り詰めがどのように行われるのか、そして全体的なレイアウトがどのように調整されるのかを確認する必要があったからです。",
            "What(何を)": "このテストデータは、システムの様々な側面をテストすることを目的としています。具体的には、文字列の折り返し処理、省略記号の表示、レイアウトの調整、文字数制限の処理、そして全体的なデザインの一貫性などを検証します。特に、日本語と英語が混在する場合の処理や、記号類が含まれる場合の表示についても確認が必要です。",
            "How(どのように)": "突然、隣のテーブルから大きな物音がしたかと思うと、誰かが「申し訳ありません！」と叫ぶ声が聞こえました。振り返ってみると、若い女性がコーヒーをこぼしてしまったようでした。周りのお客さんが慌てて立ち上がり、ティッシュやハンカチを差し出していました。店員さんも素早く対応し、新しいドリンクを提供していました。このような予期せぬ出来事に、カフェにいた全員が思い思いの方法で対応を示していたのです。",
            "Then(どうなった)": "結果として、このテストケースによって、システムが超長文を適切に処理し、見やすい形で表示できることが確認できました。また、複数行のテキストが省略記号で適切に切り詰められることも確認できました。さらに、日本語と英語が混在する文章、記号類を含む文章、段落を含む文章など、様々なパターンの文章に対してシステムが適切に対応できることも検証できました。このテストケースは、システムの堅牢性と柔軟性を実証する上で非常に重要な役割を果たしました。"
        }
    },
    {
        "post_id": 4,
        "post_title": "（Thenがない）",
        "post_content": {
            "Who(誰が)": "筆者が",
            "When(いつ)": "数年前",
            "Where(どこで)": "実家に帰省した際",
            "Why(なぜ)": "子供を作ることは子供の未来の幸せを願ってではなく、遺伝子を残すためだけの行為だと考えているので",
            "What(何を)": "迎えに来てくれた父親に対して",
            "How(どのように)": "兄貴が結婚して子供残しそうだから、スペアの弟である僕は要らなかったねと発言した",
        }
    }
]


def lambda_handler(event, context):
    try:
        secrets = get_supabase_secret()
        posts = poll_supabase_for_new_posts(secrets) if IS_PRODUCTION else test_posts
        if len(posts) == 0:
            logger.setLevel("INFO")
            logger.info("There are no posts to create OG Image.")
            return
    
        for post in posts:
            post_id = post["post_id"]
            post_title = post["post_title"]
            post_url = f"https://healthy-person-emulator.org/archives/{post_id}"
            if re.match(r"^.*プログラムテスト.*$", post_title):
                continue
            get_image(post_id=post_id, table_data=post["post_content"])
            s3_url = f"https://{S3_BUCKET_NAME}.s3-ap-northeast-1.amazonaws.com/{post_id}.jpg"
            
            if not IS_PRODUCTION:
                continue
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
    
