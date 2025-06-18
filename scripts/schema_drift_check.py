import json, os, logging, watchtower
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from slack_sdk import WebClient

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
cw = watchtower.CloudWatchLogHandler(log_group="FinancialETL")
logger.addHandler(cw)

def schema_drift_check(**context):
    s3 = S3Hook(aws_conn_id="aws_default")
    key = context['ti'].xcom_pull(task_ids='ingest_intraday')
    obj = s3.get_key(key, "financial-staging").get()
    sample = json.loads(obj['Body'].read())

    with open("config/expected_schema.json") as f:
        expected = set(json.load(f))
    actual = set(sample[0].keys())
    missing = expected - actual
    extra = actual - expected

    if missing or extra:
        client = WebClient(token=os.environ["SLACK_TOKEN"])
        client.chat_postMessage(channel="#data-alerts",
                                text=f"Schema drift: missing {missing}, extra {extra}")
        logger.error(f"Schema drift detected: {missing}, {extra}")
        raise RuntimeError("Schema drift detected")
    logger.info("No schema drift detected")