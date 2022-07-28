import importlib
import json
import os

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(
    os.environ.get("SENTRY_DSN"),
    traces_sample_rate=1.0,
    integrations=[AwsLambdaIntegration()],
)

from ddj_cloud.utils.date_and_time import local_now


def scrape(event, context):
    module_name = event["module_name"]
    scraper = importlib.import_module(f"ddj_cloud.scrapers.{module_name}.{module_name}")

    with sentry_sdk.configure_scope() as scope:
        scope.set_tag("scraper", module_name)
        try:
            scraper.run()
            now = local_now()
            print(f"Ran {module_name} at {now}")
        except Exception as e:
            # Catch and send error to Sentry manually so we can continue
            # running other scrapers if one fails
            print(f"Scraper {module_name} failed with:")
            print(e)
            sentry_sdk.capture_exception(e)

    body = {
        "message": f"Ran scraper {module_name} successfully.",
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
