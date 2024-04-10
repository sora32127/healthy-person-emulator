from typing import Dict, List, Set
import json

import boto3
from botocore.exceptions import ClientError
import psycopg2
from openai import OpenAI
import os

def get_secret():

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

def get_target_post_id_list(conn):
    cur = conn.cursor()
    cur.execute("SELECT post_id FROM dim_posts where content_embedding is null order by post_id")
    data = cur.fetchall()
    cur.close()
    black_list = [] # トークン数が多すぎるため
    return [i[0] for i in data if i[0] not in black_list]

def get_post_content(post_id):
    cur = conn.cursor()
    cur.execute("SELECT post_content FROM dim_posts where post_id = %s", (post_id,))
    data = cur.fetchall()
    cur.close()
    return data[0][0]

def get_embedding(post_content):
    openAI_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    try:
        response = openAI_client.embeddings.create(
            input = post_content,
            model = "text-embedding-3-small"
        )
        ans = {
            "embedding": response.data[0].embedding,
            "token_count" : response.usage.total_tokens 
        }
        return ans
    except Exception as e:
        with open("black_list.txt", "a") as f:
            f.write(f"{post_id}\n")
        print(e)
        raise e

def set_embedding_result(embedding_result, post_id):
    cur = conn.cursor()
    cur.execute("UPDATE dim_posts SET content_embedding = %s, token_count = %s where post_id = %s", (embedding_result["embedding"], embedding_result["token_count"], post_id))
    conn.commit()
    cur.close()

secrets = get_secret()
conn = psycopg2.connect(
    dbname=secrets["dbname"],
    user=secrets["username"],
    password=secrets["password"],
    host=secrets["host"],
    port=secrets["port"]
)

target_post_id_list = get_target_post_id_list(conn)
for post_id in target_post_id_list:
    print(f"Start: {post_id}")
    post_content = get_post_content(post_id)
    try:
        embedding_result = get_embedding(post_content)
        set_embedding_result(embedding_result, post_id)
        print(f"End: {post_id}")
    except Exception as e:
        print(f"Error: {post_id}")
        print(e)
        continue

    