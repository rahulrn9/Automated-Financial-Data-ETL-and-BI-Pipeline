import json, logging, watchtower, os
from airflow.hooks.http_hook import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# CloudWatch logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
cw = watchtower.CloudWatchLogHandler(log_group="FinancialETL")
logger.addHandler(cw)

def ingest_data(**context):
    http = HttpHook(http_conn_id="financial_api", method="GET")
    response = http.run(endpoint="/intraday", data={"symbol": "XYZ"})
    data = response.json()

    ts = context['ts_nodash']
    key = f"intraday/{ts}.json"
    s3 = S3Hook(aws_conn_id="aws_default")
    s3.load_string(json.dumps(data), key, "financial-staging", replace=True)
    logger.info(f"Ingested data to s3://financial-staging/{key}")
    return key
