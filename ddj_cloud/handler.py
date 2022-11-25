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
from ddj_cloud.utils import storage


def scrape(event, context):
    module_name = event["module_name"]

    with sentry_sdk.configure_scope() as scope:
        scope.set_tag("scraper", module_name)
        try:
            scraper = importlib.import_module(f"ddj_cloud.scrapers.{module_name}.{module_name}")

            if getattr(scraper, "run", None):
                scraper.run()
            else:
                print("No run function found")

            now = local_now()
            print(f"Ran {module_name} at {now}")
        except Exception as e:
            # Catch and send error to Sentry manually so we can continue
            # running other scrapers if one fails
            print(f"Scraper {module_name} failed with:")
            print(e)
            sentry_sdk.capture_exception(e)

        # Run CloudFront invalidation
        try:
            storage.run_cloudfront_invalidations()
        except Exception as e:
            print(f"Cloudfront invalidation failed with:")
            print(e)
            sentry_sdk.capture_exception(e)

        print("The scraper performed the following storage operations:")
        for storage_event_description in storage.describe_events():
            print("-", storage_event_description)

    body = {
        "message": f"Ran scraper {module_name} successfully.",
        "storage_events": storage.STORAGE_EVENTS,
    }

    response = {"statusCode": 200, "body": json.dumps(body)}

    return response
