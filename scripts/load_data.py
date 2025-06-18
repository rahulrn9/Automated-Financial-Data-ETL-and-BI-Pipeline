import io, logging, watchtower
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from sqlalchemy import create_engine

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
cw = watchtower.CloudWatchLogHandler(log_group="FinancialETL")
logger.addHandler(cw)

def load_data(**context):
    s3 = S3Hook(aws_conn_id="aws_default")
    key = context['ti'].xcom_pull(task_ids='schema_drift_check')
    obj = s3.get_key(key, "financial-transformed").get()
    df = pd.read_csv(io.BytesIO(obj['Body'].read()))

    conn_str = os.environ.get("REDSHIFT_CONN")
    engine = create_engine(conn_str)
    df.to_sql('intraday_trades', engine, index=False, if_exists='append', method='multi')
    logger.info(f"Loaded {len(df)} records into Redshift")
