import sentry_sdk

from ddj_cloud.scrapers.divi_intensivregister import (
    bundeslaender_kapazitaeten,
    deutschland_altersgruppen,
    deutschland_kapazitaeten,
    landkreise_kapazitaeten,
)


def run():
    for scraper in [
        deutschland_altersgruppen,
        bundeslaender_kapazitaeten,
        landkreise_kapazitaeten,
        deutschland_kapazitaeten,
    ]:
        try:
            scraper.run()
        except Exception as e:
            sentry_sdk.capture_exception(e)
            print("Error in scraper", scraper.__name__)
            print(e)
