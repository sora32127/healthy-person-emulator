import json
import textwrap
from typing import Dict, List
from PIL import Image, ImageDraw, ImageFont
import boto3

SERIF_FONT_FILE_PATH = "./Kokoro.otf"
SANS_SERIF_FONT_FILE_PATH = "./BIZ-UDGOTHICB.TTC"
PRIMARY_COLOR = (220, 91, 102)  # サーモンピンク（プライマリーカラー）
SECONDARY_COLOR = (0, 0, 0)  # 黒（セカンダリーカラー）
ACCENT_COLOR = (255, 255, 255)  # 白（アクセントカラー）
TMP_FILE_PATH = "/tmp/og_image.png"

def get_secrets():
    sm = boto3.client("secretsmanager")
    res = sm.get_secret_value(SecretId="my-toilet-blogs-r2-credentials")
    return json.loads(res["SecretString"])
    

def get_text_data_from_request(event) -> Dict[str, str]:
    print(event)
    body = json.loads(event["body"])
    print(body)
    post_title = body["post_title"]
    post_tags = body["post_tags"]
    post_id = body["post_id"]
    return {
        "post_title": post_title,
        "post_tags": post_tags,
        "post_id": post_id
    }

def create_og_image(post_title: str, post_tags: List[str]):
    # 画像サイズを設定
    width, height = 1200, 630
    im = Image.new("RGB", (width, height), SECONDARY_COLOR)
    draw = ImageDraw.Draw(im)
    
    # フォントの設定
    title_font = ImageFont.truetype(SERIF_FONT_FILE_PATH, 64)
    tag_font = ImageFont.truetype(SANS_SERIF_FONT_FILE_PATH, 32)
    author_font = ImageFont.truetype(SERIF_FONT_FILE_PATH, 40)
    
    # 余白の設定
    margin = 60
    
    # 左側にオレンジ色の縦線を描画
    line_width = 20
    draw.rectangle([0, 0, line_width, height], fill=PRIMARY_COLOR)
    
    # タグの描画
    tag_y = margin
    for tag in post_tags:
        tag_width = draw.textlength(f"#{tag}", font=tag_font)
        tag_bg = Image.new("RGBA", (int(tag_width + 20), 45), PRIMARY_COLOR)
        im.paste(tag_bg, (line_width + 40, tag_y - 5), tag_bg)
        draw.text((line_width + 50, tag_y), f"#{tag}", font=tag_font, fill=SECONDARY_COLOR)
        tag_y += 50
    
    # タイトルの描画
    title_y = max(tag_y + 40, height // 3)
    wrapped_title = textwrap.wrap(post_title, width=25)
    for line in wrapped_title:
        draw.text((line_width + 40, title_y), line, font=title_font, fill=ACCENT_COLOR)
        title_y += 80
    
    # 作者の表示
    author = "contradiction29"
    author_width = draw.textlength(author, font=author_font)
    draw.text((width - margin - author_width, height - margin - 48), author, font=author_font, fill=ACCENT_COLOR)
    
    # コントラストを高めるための半透明の黒いオーバーレイ
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 32))
    im = Image.alpha_composite(im.convert("RGBA"), overlay)
    
    im.save(TMP_FILE_PATH, quality=95)
    return TMP_FILE_PATH

def upload_image_to_r2(image_path: str, post_id: str, secrets: Dict[str, str]):
    R2_ENDPOINT_URL = secrets["R2_ENDPOINT_URL"]
    AWS_ACCESS_KEY_ID = secrets["AWS_ACCESS_KEY_ID"]
    AWS_SECRET_ACCESS_KEY = secrets["AWS_SECRET_ACCESS_KEY"]
    R2_BUCKET_NAME = secrets["R2_BUCKET_NAME"]
    s3 = boto3.client(
        service_name="s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name="apac",
    )
    s3.upload_file(image_path, R2_BUCKET_NAME, f"{post_id}.png")
    res = s3.get_object(Bucket=R2_BUCKET_NAME, Key=f"{post_id}.png")
    if res["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return {
            "status": "success", 
            "message": "画像をR2にアップロードしました", 
            "key": f"{post_id}.png"
        }
    else:
        return {
            "status": "error",
            "message": "画像をR2にアップロードできませんでした",
        }

def lambda_handler(event, context):
    text_data = get_text_data_from_request(event)
    image_path = create_og_image(text_data["post_title"], text_data["post_tags"])
    secrets = get_secrets()
    res = upload_image_to_r2(image_path, text_data["post_id"], secrets)
    return res

if __name__ == "__main__":
    lambda_handler(None, None)