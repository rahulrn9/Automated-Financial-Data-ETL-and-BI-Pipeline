import io, os, logging, watchtower
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from prometheus_client import Counter, Histogram, start_http_server
from great_expectations.data_context import DataContext
from slack_sdk import WebClient

# Metrics
start_http_server(8000)
RECORDS = Counter("etl_records_transformed_total", "Records transformed")
DURATION = Histogram("etl_transform_duration_seconds", "Transform duration")

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
cw = watchtower.CloudWatchLogHandler(log_group="FinancialETL")
logger.addHandler(cw)

@DataContext()
@DURATION.time()
def transform_data(**context):
    s3 = S3Hook(aws_conn_id="aws_default")
    raw_key = context['ti'].xcom_pull(task_ids='ingest_intraday')
    obj = s3.get_key(raw_key, "financial-staging").get()
    df = pd.read_json(io.BytesIO(obj['Body'].read()))

    df.drop_duplicates(inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    df['price_change'] = df['price'].pct_change().fillna(0)

    ts = context['ts_nodash']
    out_key = f"intraday_transformed/{ts}.csv"
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    s3.load_string(buf.getvalue(), out_key, "financial-transformed", replace=True)

    # Great Expectations validation
    ge_ctx = DataContext(os.getcwd()+"/great_expectations")
    results = ge_ctx.run_validation_operator(
        "action_list_operator",
        assets_to_validate=[{"path": f"/tmp/{ts}.csv", "datasource": "data__dir", "data_asset_name": "intraday"}],
        run_name="airflow_run_"+ts
    )
    if not results["success"]:
        client = WebClient(token=os.environ["SLACK_TOKEN"])
        client.chat_postMessage(channel="#data-alerts",
                                text=f"Data validation failed for run {ts}")
        raise RuntimeError("GE validation failed")

    RECORDS.inc(len(df))
    logger.info(f"Transformed and validated {len(df)} records")
    return out_key
