import importlib
from pathlib import Path
import sys

import click
from copier import Worker, AnswersMap


BASE_DIR = Path(__file__).parent
TEMPLATE_NAME = "scraper_template"
TEMPLATE_DIR = BASE_DIR / TEMPLATE_NAME
SCRAPERS_DIR = BASE_DIR / "ddj_cloud" / "scrapers"


@click.group()
def cli():
    ...


def _transform_answers(answers: AnswersMap):
    ...


@cli.command("new")
@click.option("-p", "--pretend", is_flag=True)
def new_scraper(pretend: bool):

    # Set up copier worker
    worker = Worker(
        src_path=str(TEMPLATE_DIR),
        dst_path=SCRAPERS_DIR,
        pretend=pretend,
    )

    # Run prompts
    answers = worker.answers

    # Update answers as needed
    _transform_answers(answers)

    # Run templating and file copy
    worker.run_copy()

    context = {
        **worker.answers.default,
        **worker.answers.user,
        **worker.answers.init,
    }
    print(context)

    if pretend:
        click.echo("This was a pretend-run, no files have been created")
        return

    click.echo(f"New scraper created in {SCRAPERS_DIR / answers.combined['scraper_module']}")



@cli.command("list")
def list_scrapers():
    print("foobar")


@cli.command("test")
@click.option("-s", "--scraper", type=str)
def test_scraper(scraper):
    scraper = importlib.import_module(f"ddj_cloud.scrapers.{scraper}.{scraper}")
    scraper.run()


if __name__ == "__main__":
    sys.exit(cli())
