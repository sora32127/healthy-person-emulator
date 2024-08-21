import json
from supabase import create_client
import boto3
from logging import getLogger

logger = getLogger()
logger.setLevel("INFO")


def get_secret():
    secretmanager = boto3.client('secretsmanager')
    secret = secretmanager.get_secret_value(SecretId='SUPABASE_CONNECTION_SECRET')
    return json.loads(secret['SecretString'])

def get_supabase_client(secret):
    return create_client(secret['SUPABASE_URL'], secret['SUPABASE_SERVICE_ROLE_KEY'])

def get_random_article(supabase):
    articles = supabase.table('dim_posts') \
        .select('post_id, post_title, ogp_image_url') \
        .eq('is_sns_pickuped', False) \
        .gte('count_likes',10) \
        .limit(1) \
        .execute()
    return articles.data[0]

def update_sns_pickuped(supabase, post_id):
    supabase.table('dim_posts').update({'is_sns_pickuped': True}).eq('post_id', post_id).execute()
    
def publish_to_sns(article):
    message = {
        "post_title": article['post_title'],
        "post_url": f"https://healthy-person-emulator.org/archives/{article['post_id']}",
        "og_url": article['ogp_image_url'],
        "message_type": "random"
    }
    sns = boto3.client('sns')
    sns.publish(TopicArn='arn:aws:sns:ap-northeast-1:662924458234:healthy-person-emulator-socialpost', Message=json.dumps(message))

def lambda_handler(event, context):
    try:    
        secret = get_secret()
        supabase = get_supabase_client(secret)
        article = get_random_article(supabase)
        update_sns_pickuped(supabase, article['post_id'])
        publish_to_sns(article)
        logger.info(f"Article {article['post_id']} picked up")
    except Exception as e:
        logger.error(e)
        raise e

if __name__ == '__main__':
    lambda_handler(None, None)