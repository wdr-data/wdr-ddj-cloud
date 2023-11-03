import importlib
import json
import os
from pathlib import Path
import sys
from typing import Optional, Tuple
import shutil

import click
from copier import Worker, DEFAULT_DATA
import yaml


BASE_DIR = Path(__file__).parent
TEMPLATE_NAME = "scraper_template"
TEMPLATE_DIR = BASE_DIR / TEMPLATE_NAME
SCRAPERS_DIR = BASE_DIR / "ddj_cloud" / "scrapers"
SCRAPERS_CONFIG_NAME = "scrapers_config.json"
SCRAPERS_CONFIG_PATH = BASE_DIR / SCRAPERS_CONFIG_NAME
LOCAL_STORAGE_NAME = "local_storage"
LOCAL_STORAGE_PATH = BASE_DIR / LOCAL_STORAGE_NAME


def _success(
    text: str,
    *,
    nl: bool = True,
    echo_kwargs: Optional[dict] = None,
    style_kwargs: Optional[dict] = None,
):
    click.echo(
        click.style(text, fg="green", bold=True, **(style_kwargs or {})),
        nl=nl,
        **(echo_kwargs or {}),
    )


def _warn(
    text: str,
    *,
    nl: bool = True,
    echo_kwargs: Optional[dict] = None,
    style_kwargs: Optional[dict] = None,
):
    click.echo(
        click.style(text, fg="yellow", bold=True, **(style_kwargs or {})),
        nl=nl,
        **(echo_kwargs or {}),
    )


def _error(
    text: str,
    *,
    nl: bool = True,
    echo_kwargs: Optional[dict] = None,
    style_kwargs: Optional[dict] = None,
):
    click.echo(
        click.style(text, fg="red", bold=True, **(style_kwargs or {})), nl=nl, **(echo_kwargs or {})
    )


def _info(
    text: str,
    *,
    nl: bool = True,
    echo_kwargs: Optional[dict] = None,
    style_kwargs: Optional[dict] = None,
):
    click.echo(
        click.style(text, fg="bright_blue", **(style_kwargs or {})), nl=nl, **(echo_kwargs or {})
    )


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


def _delete_scraper(scraper: dict, scrapers_config) -> Tuple[bool, list[dict]]:
    _warn(f"Warning: This will delete all files at {SCRAPERS_DIR / scraper['module_name']}")
    _info("To continue, please type ", nl=False)
    click.echo(click.style("DELETE", fg="bright_red", bold=True), nl=False)
    if "DELETE" != click.prompt("", default="", show_default=False):
        _warn("Aborting!")
        return False, scrapers_config

    _info(f"Deleting {SCRAPERS_DIR / scraper['module_name']}... ")
    shutil.rmtree(SCRAPERS_DIR / scraper["module_name"])
    _success("Success!")

    # Update scrapers config
    _info("Updating scrapers_config.json... ")
    scrapers_config = [s for s in scrapers_config if s["module_name"] != scraper["module_name"]]
    _save_scrapers_config(scrapers_config)
    _success("Success!")

    # Return updated config
    return True, scrapers_config


@click.group(help="Management utility for wdr-ddj-cloud. Each command has their own --help option.")
def cli():
    ...


@cli.command(
    "new",
    help="Create a new scraper. This will ask you a bunch of questions to gather information from you and then set up the new scraper automatically.",
)
@click.option(
    "-p",
    "--pretend",
    is_flag=True,
    help="Do a dry-run of the questionnaire but don't write any files.",
)
def new_scraper(pretend: bool):
    # Set up copier worker
    with Worker(
        src_path=str(TEMPLATE_DIR),
        dst_path=SCRAPERS_DIR,
        pretend=pretend,
        skip_answered=True,
        unsafe=True,  # Required by `jinja_extensions`
    ) as worker:
        # Run prompts
        worker._ask()
        answers = worker.answers

        # Check if scraper exists already
        scrapers_config = _load_scrapers_config()
        if existing_scraper := (
            _find_scraper_by_name(answers.combined["display_name"], scrapers_config)
            or _find_scraper_by_name(answers.combined["module_name"], scrapers_config)
        ):
            _info(
                "A scraper with this name already exists. Do you want to delete the existing scraper and continue?"
            )
            deleted_existing, scrapers_config = _delete_scraper(existing_scraper, scrapers_config)
            if not deleted_existing:
                sys.exit(0)

        # HACK: Prevent reprompt
        worker._ask = lambda: None

        # Run templating and file copy
        worker.run_copy()

    if pretend:
        _warn("This was a pretend-run, no files have been created")
        sys.exit(0)

    _success(f"New scraper created in {SCRAPERS_DIR / answers.combined['module_name']}")

    _info(f'Updating "{SCRAPERS_CONFIG_NAME}"... ')

    new_entry = {**answers.combined}

    # Clean up answers
    for key in ["_src_path", *DEFAULT_DATA.keys()]:
        del new_entry[key]

    # Convert user-provided interval to a more generalized format for future proofing
    event = {
        "type": "schedule",
        "enabled": True,
        "data": {
            "interval": new_entry.pop("interval"),
            "interval_custom": new_entry.pop("interval_custom", None),  # Optional
        },
    }

    new_entry["events"] = [event]
    new_entry["extra_env"] = []

    scrapers_config.append(new_entry)
    _save_scrapers_config(scrapers_config)

    _success("Success!")
    _info(
        f'Tip: You can test your scraper with "pipenv run manage test {new_entry["module_name"]}"'
    )


@cli.command("list", help="Print a list of all configured scrapers.")
def list_scrapers():
    scrapers_config = _load_scrapers_config()

    for i, scraper in enumerate(scrapers_config):
        click.echo(click.style("Display name: ", bold=True) + scraper["display_name"])
        click.echo(click.style("Module name: ", bold=True) + scraper["module_name"])
        click.echo(
            click.style("Contact: ", bold=True)
            + f"{scraper['contact_name']} <{scraper['contact_email']}>"
        )
        click.echo(
            click.style("Intervals: ", bold=True)
            + ", ".join(
                f"{'✅' if event['enabled'] else '❌'} {event['data']['interval_custom'] or event['data']['interval']}"
                for event in scraper["events"]
            )
        )
        click.echo(click.style("Description: ", bold=True))
        click.echo(scraper["description"])

        if i + 1 != len(scrapers_config):
            click.echo("")
            click.echo(click.style("---", bold=True))
            click.echo("")


@cli.command("delete", help="Delete a scraper completely.")
@click.argument("module_name", type=str)
def delete_scraper(module_name):
    scrapers_config = _load_scrapers_config()
    scraper = _find_scraper_by_name(module_name, scrapers_config)

    if scraper is None:
        _error(f'Error: Scraper "{module_name}" not found in "{SCRAPERS_CONFIG_NAME}".')
        sys.exit(1)

    scrapers_config = _delete_scraper(scraper, scrapers_config)


@cli.command("test", help="Test a scraper locally.")
@click.argument("module_name", type=str)
def test_scraper(module_name):
    _info(f'Loading scraper module "{module_name}"...')

    # Disable S3/CloudFront for local testing
    os.environ["USE_LOCAL_STORAGE"] = "1"

    if not (SCRAPERS_DIR / module_name).exists():
        _error(f'Error: Scraper "{module_name}" not found in "{SCRAPERS_DIR}".')
        sys.exit(1)

    try:
        scraper = importlib.import_module(f"ddj_cloud.scrapers.{module_name}.{module_name}")
    except:
        _error("Error: Something went wrong during import :(\n")
        raise

    _success("Scraper loaded successfully!")

    try:
        if getattr(scraper, "run", None):
            _info("Running scraper now!\n")
            scraper.run()
        else:
            _warn("Warning: Scraper has no run() method")

    except Exception as e:
        _error("Scraper failed! Logging error...\n")
        raise

    _success("Scraper ran succesfully!")

    # Print storage events
    from ddj_cloud.utils import storage

    _info("\nThe scraper performed the following storage operations:")

    for event_description in storage.describe_events():
        _info(f"- {event_description}")

    _info("\nTip: During local testing, no files are actually uploaded to AWS.")
    _info("Instead, files are saved locally to the following directory:")
    _info(str(LOCAL_STORAGE_PATH))


@cli.command("test-all", help="Test all scrapers locally.")
def test_all_scrapers():
    scrapers_config = _load_scrapers_config()

    for scraper in scrapers_config:
        try:
            test_scraper([scraper["module_name"]])
        except:
            print("")


@cli.command("generate", help='Generate the "serverless.yml" for deployment.')
def generate_serverless_yml():
    _info(
        f'Generating "serverless.yml" from "serverless.part.yml" and "{SCRAPERS_CONFIG_NAME}"... '
    )

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

        extra_env_vars = {var: "${env:" + var + "}" for var in scraper["extra_env"]}

        function_definition = {
            "handler": "ddj_cloud.handler.scrape",
            "timeout": 60 * 15,  # 15 minutes is the max. timeout allowed by AWS
            "memorySize": int(scraper["memory_size"]),
            "ephemeralStorageSize": int(scraper["ephemeral_storage"]),
            "description": scraper["description"],
            "events": events,
            "environment": extra_env_vars,
        }

        # We use pascal case for the key, otherwise they literally put "Underscore" there
        name_pascal_case = scraper["module_name"].replace("_", " ").title().replace(" ", "")
        functions[name_pascal_case] = function_definition

    serverless_part_yml["functions"] = functions

    with open(BASE_DIR / "serverless.yml", "w", encoding="utf-8") as fp:
        yaml.dump(serverless_part_yml, fp)

    _success("Success!")


if __name__ == "__main__":
    sys.exit(cli())
