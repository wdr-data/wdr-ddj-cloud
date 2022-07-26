import importlib
import json
from pathlib import Path
import sys
import tempfile

import click
from cookiecutter.main import _patch_import_path_for_repo
from cookiecutter.generate import generate_context, generate_files
from cookiecutter.prompt import prompt_for_config
from cookiecutter.utils import rmtree
from cookiecutter.exceptions import FailedHookException


BASE_DIR = Path(__file__).parent
TEMPLATE_NAME = "scraper_template"
TEMPLATE_DIR = BASE_DIR / TEMPLATE_NAME
SCRAPERS_DIR = BASE_DIR / "ddj_cloud" / "scrapers"


@click.group()
def cli():
    ...


@cli.command("new")
def new_scraper():

    # Set up and run cookiecutter
    repo_dir = str(TEMPLATE_DIR)
    context = generate_context(context_file=str(TEMPLATE_DIR / "cookiecutter.json"))
    import_patch = _patch_import_path_for_repo(repo_dir)

    with import_patch:
        context["cookiecutter"] = prompt_for_config(context)

    context["cookiecutter"]["_template"] = TEMPLATE_NAME
    context["cookiecutter"]["_output_dir"] = str(SCRAPERS_DIR)

    try:
        with import_patch:
            scraper_dir = generate_files(
                repo_dir=repo_dir,
                context=context,
                overwrite_if_exists=False,
                skip_if_file_exists=False,
                output_dir=str(SCRAPERS_DIR),
                accept_hooks=True,
            )
    except FailedHookException:
        return 1

    scraper_path = Path(scraper_dir)
    click.echo(f"New scraper created in {scraper_path}")

    context = dict(context["cookiecutter"])

    print(context)


@cli.command("list")
def list_scrapers():
    print("foobar")


@cli.command("test")
@click.option("-s", "--scraper", type=str)
def test_scraper(scraper):
    scraper = importlib.import_module(f"ddj_cloud.scrapers.{scraper}")
    scraper.run()


if __name__ == "__main__":
    sys.exit(cli())
