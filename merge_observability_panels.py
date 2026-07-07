"""
merge_observability_panels.py

Adds 3 new panels (Kafka Consumer Lag, Landing Zone Freshness, Backup
Health) to grafana_dashboards/project_monitoring.json, matching the exact
schema/datasource of the existing "dbt Run Results" panel already in that
file (marcusolsson-json-datasource, same datasource reference, same
table viz group/version).

Does NOT overwrite the original file -- writes
project_monitoring_updated.json alongside it, so you can diff/review
before importing into Grafana.

Usage:
    python merge_observability_panels.py
"""
import json
import copy
import sys
from pathlib import Path

SOURCE = Path("grafana_dashboards/project_monitoring.json")
OUTPUT = Path("grafana_dashboards/project_monitoring_updated.json")


def make_panel(template, panel_id, title, url_path, fields):
    p = copy.deepcopy(template)
    p["spec"]["id"] = panel_id
    p["spec"]["title"] = title
    q = p["spec"]["data"]["spec"]["queries"][0]["spec"]["query"]["spec"]
    q["urlPath"] = url_path
    q["fields"] = fields
    # Clear any value mappings copied from the template that don't apply
    # (pass/fail/warn was specific to /pipeline-health's status values) --
    # add generic true/false -> green/red mappings instead, which fit all
    # three new endpoints' within_slo/status fields.
    field_config = p["spec"]["vizConfig"]["spec"]["fieldConfig"]["defaults"]
    field_config["mappings"] = [
        {
            "type": "value",
            "options": {
                "true":  {"color": "green", "index": 0},
                "OK":    {"color": "green", "index": 0},
                "false": {"color": "red", "index": 1},
                "STALE": {"color": "red", "index": 1},
                "NO_BACKUP_FOUND": {"color": "red", "index": 2},
            },
        }
    ]
    return p


def main():
    if not SOURCE.exists():
        print(f"ERROR: {SOURCE} not found. Run this from the project root.")
        sys.exit(1)

    with open(SOURCE, encoding="utf-8") as f:
        data = json.load(f)

    elements = data["spec"]["elements"]
    if "panel-1" not in elements:
        print("ERROR: panel-1 (dbt Run Results) not found -- schema may have "
              "changed since this script was written. Aborting rather than "
              "guessing.")
        sys.exit(1)

    template = elements["panel-1"]

    # Figure out the next free panel id and next y position from the
    # existing layout, rather than hardcoding -- works regardless of how
    # many panels already exist.
    layout_items = data["spec"]["layout"]["spec"]["items"]
    existing_ids = [
        elements[item["spec"]["element"]["name"]]["spec"]["id"]
        for item in layout_items
    ]
    next_id = max(existing_ids, default=0) + 1

    max_y = 0
    for item in layout_items:
        y = item["spec"]["y"]
        h = item["spec"]["height"]
        max_y = max(max_y, y + h)

    new_panels = [
        (
            "Kafka Consumer Lag", "/kafka-lag",
            [
                {"jsonPath": "$.topics[*].topic", "name": "topic"},
                {"jsonPath": "$.topics[*].lag", "name": "lag", "type": "number"},
                {"jsonPath": "$.topics[*].within_slo", "name": "within_slo"},
            ],
        ),
        (
            "Landing Zone Freshness", "/freshness",
            [
                {"jsonPath": "$.domains[*].domain", "name": "domain"},
                {"jsonPath": "$.domains[*].age_minutes", "name": "age_minutes", "type": "number"},
                {"jsonPath": "$.domains[*].within_slo", "name": "within_slo"},
            ],
        ),
        (
            "Backup Health", "/backup-health",
            [
                {"jsonPath": "$.duckdb.status", "name": "duckdb_status"},
                {"jsonPath": "$.duckdb.age_hours", "name": "duckdb_age_hours", "type": "number"},
                {"jsonPath": "$.postgres.status", "name": "postgres_status"},
                {"jsonPath": "$.postgres.age_hours", "name": "postgres_age_hours", "type": "number"},
            ],
        ),
    ]

    y_cursor = max_y
    for i, (title, url_path, fields) in enumerate(new_panels):
        panel_id = next_id + i
        panel_key = f"panel-{panel_id}"
        elements[panel_key] = make_panel(template, panel_id, title, url_path, fields)

        height = 9
        layout_items.append({
            "kind": "GridLayoutItem",
            "spec": {
                "x": 0,
                "y": y_cursor,
                "width": 24,
                "height": height,
                "element": {"kind": "ElementReference", "name": panel_key},
            },
        })
        y_cursor += height

    with open(OUTPUT, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Wrote {OUTPUT}")
    print(f"Added {len(new_panels)} panels: {[p[0] for p in new_panels]}")
    print(f"Original file untouched: {SOURCE}")
    print()
    print("Next: review the diff, then import project_monitoring_updated.json")
    print("into Grafana (see instructions printed by the calling script).")


if __name__ == "__main__":
    main()
