from typing import Dict, List, Set
import json
import boto3
from botocore.exceptions import ClientError
import psycopg2
from openai import OpenAI
import os
from supabase import create_client

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

def get_target_post(supabase_client, offset, batch_size):
    data = supabase_client.table("dim_posts").select("post_id, post_content, post_title, rel_post_tags(dim_tags(tag_name))").order('post_id', desc=True).lt("post_id", offset).limit(batch_size).execute()
    normalized_data = [
        {
            "post_id": post["post_id"], 
            "post_content": post["post_content"],
            "post_title": post["post_title"],
            "tags": [tag["dim_tags"]["tag_name"] for tag in post["rel_post_tags"]]
        }
        for post in data.data
    ]
    return normalized_data

def get_embedding_input_text(post):
    template = "タイトル: {title}\nタグ: {tags}\n本文: {content}"
    title = post["post_title"]
    tags = ", ".join(post["tags"])
    content = post["post_content"]
    return template.format(title=title, tags=tags, content=content)

def get_embedding(post):
    openAI_client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])
    try:
        response = openAI_client.embeddings.create(
            input = get_embedding_input_text(post),
            model = "text-embedding-3-small"
        )
        ans = {
            "embedding": response.data[0].embedding,
            "token_count" : response.usage.total_tokens 
        }
        return ans
    except Exception as e:
        with open("black_list.txt", "a") as f:
            f.write(f"{post['post_id']}\n")
        print(e)

def update_embeddings(posts, embeddings, supabase_client):
    try:
        updates = [
            {"post_id": post["post_id"], "content_embedding": embedding["embedding"], "token_count": embedding["token_count"]}
            for post, embedding in zip(posts, embeddings)
        ]

        for update in updates:
            print(f"Updating post {update['post_id']}")
            supabase_client.table("dim_posts").update(update).eq("post_id", update["post_id"]).execute()
    except Exception as e:
        print(e)

def process_posts_in_batches(posts, batch_size, supabase_client):
    for i in range(0, len(posts), batch_size):
        batch = posts[i:i + batch_size]
        embeddings = [get_embedding(post) for post in batch]
        update_embeddings(batch, embeddings, supabase_client)
    min_post_id = min([post["post_id"] for post in posts])
    max_post_id = max([post["post_id"] for post in posts])
    print(f"Finished processing posts from {min_post_id} to {max_post_id}")
    return min_post_id

secrets = get_secret()
supabase_url = secrets["SUPABASE_URL"]
supabase_service_role_key = secrets["SUPABASE_SERVICE_ROLE_KEY"]
supabase_client = create_client(supabase_url, supabase_service_role_key)

post_count = supabase_client.table("dim_posts").select("post_id", count="exact").execute().count
batch_size = 1000
min_post_id = 26864
for i in range(0, post_count, batch_size):
    posts = get_target_post(supabase_client, min_post_id, batch_size)
    min_post_id = process_posts_in_batches(posts, 100, supabase_client)
