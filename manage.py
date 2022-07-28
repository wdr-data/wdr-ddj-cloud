import importlib
import json
from pathlib import Path
import sys
from typing import Optional
import shutil

import click
from copier import Worker, AnswersMap
import yaml


BASE_DIR = Path(__file__).parent
TEMPLATE_NAME = "scraper_template"
TEMPLATE_DIR = BASE_DIR / TEMPLATE_NAME
SCRAPERS_DIR = BASE_DIR / "ddj_cloud" / "scrapers"
SCRAPERS_CONFIG_NAME = "scrapers_config.json"
SCRAPERS_CONFIG_PATH = BASE_DIR / SCRAPERS_CONFIG_NAME


def _transform_answers(answers: AnswersMap):
    ...


def _load_scrapers_config() -> list[dict]:
    if not SCRAPERS_CONFIG_PATH.exists():
        return []

    with open(SCRAPERS_CONFIG_PATH, encoding="utf-8") as fp:
        scrapers_config = json.load(fp)

    return scrapers_config


def _save_scrapers_config(scrapers_config: list[dict]):
    with open(SCRAPERS_CONFIG_PATH, "w", encoding="utf-8") as fp:
        json.dump(scrapers_config, fp, indent=4, ensure_ascii=False)


def _find_scraper_by_name(name: str, scraper_config: list[dict]) -> Optional[dict]:
    by_display_name = {entry["display_name"]: entry for entry in scraper_config}
    by_module_name = {entry["module_name"]: entry for entry in scraper_config}
    return by_display_name.get(name) or by_module_name.get(name)


def _delete_scraper(scraper: dict, scrapers_config) -> list[dict]:
    click.echo(f"Deleting {SCRAPERS_DIR / scraper['module_name']}... ", nl=False)
    shutil.rmtree(SCRAPERS_DIR / scraper["module_name"])
    click.echo("✅")

    # Update scrapers config
    click.echo("Updating scrapers_config.json... ", nl=False)
    scrapers_config = [s for s in scrapers_config if s["module_name"] != scraper["module_name"]]
    _save_scrapers_config(scrapers_config)
    click.echo("✅")

    # Return updated config
    return scrapers_config


@click.group()
def cli():
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

    # Check if scraper exists already
    scrapers_config = _load_scrapers_config()
    if existing_scraper := (
        _find_scraper_by_name(answers.combined["display_name"], scrapers_config)
        or _find_scraper_by_name(answers.combined["module_name"], scrapers_config)
    ):
        if "DELETE" == click.prompt(
            "A scraper with this name already exists. \nTo delete the existing scraper and continue, type DELETE",
            default="",
            show_default=False,
        ):
            scrapers_config = _delete_scraper(existing_scraper, scrapers_config)
        else:
            click.echo("Aborting!")
            return 1

    # Run templating and file copy
    worker.run_copy()

    context = {
        **worker.answers.default,
        **worker.answers.user,
        **worker.answers.init,
    }

    if pretend:
        click.echo("This was a pretend-run, no files have been created")
        return

    click.echo(f"New scraper created in {SCRAPERS_DIR / answers.combined['module_name']}")

    click.echo(f"Updating {SCRAPERS_CONFIG_NAME}... ", nl=False)
    new_entry = {**context}

    # Convert user-provided interval to a more generalized format for future proofing
    event = {
        "type": "schedule",
        "enabled": True,
        "data": {
            "interval": new_entry.pop("interval"),
            "interval_custom": new_entry.pop("interval_custom"),
        },
    }

    new_entry["events"] = [event]

    scrapers_config.append(new_entry)
    _save_scrapers_config(scrapers_config)
    click.echo("✅")


@cli.command("list")
def list_scrapers():
    scrapers_config = _load_scrapers_config()

    for i, scraper in enumerate(scrapers_config):
        click.echo(f'Display name: {scraper["display_name"]}')
        click.echo(f"Module name: {scraper['module_name']}")
        click.echo(f"Contact: {scraper['contact_name']} <{scraper['contact_email']}>")
        click.echo(f"Intervals: {', '.join(scraper['intervals'])}")
        click.echo("Description:")
        click.echo(scraper["description"])

        if i + 1 != len(scrapers_config):
            click.echo("")
            click.echo("---")
            click.echo("")


@cli.command("delete")
@click.argument("module_name", type=str)
def delete_scraper(module_name):
    scrapers_config = _load_scrapers_config()
    scraper = _find_scraper_by_name(module_name, scrapers_config)
    if scraper is None:
        click.echo(f'Scraper "{module_name}" not found.')
        return 1
    scrapers_config = _delete_scraper(scraper, scrapers_config)


@cli.command("test")
@click.argument("module_name", type=str)
def test_scraper(module_name):
    scraper = importlib.import_module(f"ddj_cloud.scrapers.{module_name}.{module_name}")
    scraper.run()


@cli.command("generate")
def generate_serverless_yml():
    with open(BASE_DIR / "serverless.part.yml", encoding="utf-8") as fp:
        serverless_part_yml = yaml.safe_load(fp)

    functions = serverless_part_yml.get("functions", {})
    scrapers_config = _load_scrapers_config()

    rate_presets = {
        "15min": "rate(15 minutes)",
        "hourly": "rate(1 hour)",
        "daily": "cron(17 1 * * ? *)",
    }

    for scraper in scrapers_config:
        events = []

        for i, event in enumerate(scraper["events"]):
            if event["type"] == "schedule":
                name = "${self:service}-${self:provider.stage}-" + f'{scraper["module_name"]}-{i}'
                rate = event["data"]["interval_custom"] or rate_presets[event["data"]["interval"]]
                events.append(
                    {
                        "schedule": {
                            "name": name,
                            "rate": rate,
                            "enabled": event["enabled"],
                            "input": {
                                "module_name": scraper["module_name"],
                            },
                        }
                    }
                )

        function_definition = {
            "handler": "ddj_cloud.handler.scrape",
            "timeout": 120,
            "memorySize": int(scraper["memory_size"]),
            "ephemeralStorageSize": int(scraper["ephemeral_storage"]),
            "description": scraper["description"],
            "events": events,
        }

        # We use pascal case for the key, otherwise they literally put "Underscore" there
        name_pascal_case = scraper["module_name"].replace("_", " ").title().replace(" ", "")
        functions[name_pascal_case] = function_definition

    serverless_part_yml["functions"] = functions

    with open(BASE_DIR / "serverless.yml", "w", encoding="utf-8") as fp:
        yaml.dump(serverless_part_yml, fp)


if __name__ == "__main__":
    sys.exit(cli())
