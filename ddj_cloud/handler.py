import importlib
import json
import os

import sentry_sdk
from sentry_sdk.integrations.aws_lambda import AwsLambdaIntegration

sentry_sdk.init(os.environ.get("SENTRY_URI"), integrations=[AwsLambdaIntegration()])

from ddj_cloud.utils.date_and_time import local_now


def scrape(event, context):
    scraper_name = event["scraper"]
    scraper = importlib.import_module(f"ddj_cloud.scrapers.{scraper_name}.{scraper_name}")

    with sentry_sdk.configure_scope() as scope:
        scope.set_tag("scraper", scraper_name)
        try:
            scraper.run()
            now = local_now()
            print(f"Ran {scraper_name} at {now}")
        except Exception as e:
            # Catch and send error to Sentry manually so we can continue
            # running other scrapers if one fails
            print(f"Scraper {scraper_name} failed with {e}")
            print(e)
            sentry_sdk.capture_exception(e)

    body = {
        "message": f"Ran scraper {scraper_name} successfully.",
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
