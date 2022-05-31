"""Console script for papfa."""
import json
import os
import sys
from datetime import datetime, timedelta

import click

from papfa.consumers import consumers_list
from papfa.settings import Papfa


def get_list_of_consumers():
    list_of_consumers = []
    for directory in Papfa.get_instance()["consumers_dirs"]:
        try:
            consumers = __import__(f"{directory}.consumers").consumers
        except ModuleNotFoundError:
            continue
        for f_name in set(consumers.__dir__()) & set(consumers_list):
            f = getattr(consumers, f_name)
            if getattr(f, "__is_consumer__", False):
                list_of_consumers.append(f"{directory}.consumers.{f_name}")
    return list_of_consumers


def configure_papfa(app_dir):
    sys.path.insert(1, os.getcwd())
    try:
        __import__(f"{app_dir}.papfa")
    except ModuleNotFoundError:
        raise RuntimeError("Papfa is not found.")
    if not Papfa.papfa_configured():
        raise RuntimeError("Papfa is not configured.")
    return Papfa.get_instance()


@click.group()
def main():
    pass


@main.command()
@click.option("--app", "-a", help="Application", required=True)
def list(app):
    try:
        configure_papfa(app)
        consumers = get_list_of_consumers()
    except RuntimeError as e:
        click.secho(e, fg="red")
        sys.exit(1)
    if len(consumers) == 0:
        click.secho("No consumers found", fg="red")
        return
    click.secho("\n  List of Consumers: \n", fg="yellow")
    for consumer in consumers:
        click.secho(f"\t~ {consumer}", fg="green")


@main.command()
@click.argument("name")
@click.option("--app", "-a", help="Application", required=True)
def consume(name, app):
    try:
        configure_papfa(app)
    except RuntimeError as e:
        click.secho(e, fg="red")
        sys.exit(1)
    if name not in get_list_of_consumers():
        click.secho("Consumer not found", fg="red")
        sys.exit(1)

    click.secho("Start Consuming {}...".format(name), fg="green")
    module, *address = name.split(".")
    module = __import__(module)
    for a in address:
        module = getattr(module, a)
    module.consume()


@main.command()
@click.argument("topic")
@click.argument("group_id")
def stats(topic, group_id):
    _dir = f"{topic}-{group_id}"
    with open(f"./consume-data/{_dir}.json") as f:
        _data = json.load(f)
    max_length = max([len(k) for k in _data.keys()])
    for key, value in _data.items():
        key = key.replace("_", " ").title()
        value = datetime.fromtimestamp(value / 10**3)
        color = "green"
        if value < datetime.now() - timedelta(minutes=5):
            color = "yellow"
        if value < datetime.now() - timedelta(hours=1):
            color = "red"

        click.echo(
            key + (" " * (max_length - len(key))) + "\t" + click.style(value, fg=color)
        )


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
