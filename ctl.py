#!/usr/bin/env python3

import click
import yaml
import logging

import dbs.postgres as postgres
from cli_helpers import tabular_output

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")


def get_config(file) -> dict:
    file_path = file
    if not file_path:
        file_path = "conf/application.yaml"
    logging.info(f"Load config path: {file_path}")
    with open(file_path) as stream:
        return yaml.full_load(stream)


def render(left, right) -> str:
    if left == right:
        return "-"
    return f"left: {left} | right: {right}"


def print_result(db, result) -> None:
    headers = ["table", "count", "max id", "sequence"]
    left_tables = result.get("left")
    right_tables = result.get("right")
    data = []
    keys = ['count', "max", "seq"]
    for table in result.get("tables"):
        left = left_tables.get(table)
        right = right_tables.get(table)
        row = [table]
        for key in keys:
            row.append(render(left[key], right[key]))
        data.append(row)
    print(f"Database: {db}")
    rows = tabular_output.format_output(data, headers, format_name="fancy_grid")
    for row in rows:
        print(row)


@click.group()
def cli():
    pass


@cli.command()
@click.option("-f", "file", type=str, help="Config file path")
def pg(file):
    """Compare PostgreSQL tables and table's count, max id and sequence last value"""
    global_config = get_config(file)
    config = global_config.get("postgres")
    databases = config.get("databases")
    for db in databases:
        result = postgres.load(config, db)
        print_result(db, result)


if __name__ == "__main__":
    cli()
