import pandas as pd

from ddj_cloud.utils.date_and_time import local_now
from ddj_cloud.utils.storage import upload_dataframe


def run():
    now = local_now()
    now_mod = now.replace(minute=int(now.minute / 5) * 5, second=0, microsecond=0).isoformat()
    rows = [
        {"col_1": "hallo welt", "timestamp": now_mod},
        {"col_1": "test", "timestamp": now_mod},
    ]
    df = pd.DataFrame(rows)
    upload_dataframe(df, "test_scraper/test.csv")
