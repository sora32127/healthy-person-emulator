import dlt
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
from time import time
import boto3
import json
BQ_DATASET = "hpe_raw"

def get_secrets():
    secretmanager_client = boto3.client("secretsmanager")
    secret_value = secretmanager_client.get_secret_value(SecretId="DLT_CONNECTION_PARAMS")
    return json.loads(secret_value["SecretString"])


def process_table(table_name, engine, secrets):
    try:
        with engine.connect() as conn:
            start_time = time()
            query = f"SELECT * FROM {table_name}"
            rows = conn.execute(text(query))
            pipeline = dlt.pipeline(
                pipeline_name=f"extract_{table_name}",
                destination=dlt.destinations.bigquery(
                    credentials=secrets,
                    ),
                dataset_name="HPE_RAW",
            )
            pipeline.run(
                rows,
                table_name=table_name,
                write_disposition="replace",
            )
            end_time = time()

            print(f"Table {table_name} processed in {end_time - start_time:.2f} seconds")
            
    except Exception as e:
        print(f"Failed to process {table_name}: {e}")
        raise e



def lambda_handler(event, context):
    secrets = get_secrets()
    connection_string = secrets["connection_string"]
    engine = create_engine(
        connection_string,
        connect_args={"connect_timeout": 60 * 15}
    )

    with engine.connect() as conn:
        res = conn.execute(text("SELECT tablename FROM pg_catalog.pg_tables where schemaname='public'"))
        table_names = [row[0] for row in res]
    with ThreadPoolExecutor() as executor:
        executor.map(lambda table_name: process_table(table_name, engine, secrets), table_names)

if __name__ == "__main__":
    lambda_handler(None, None)
