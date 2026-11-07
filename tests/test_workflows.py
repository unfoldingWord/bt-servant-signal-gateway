"""Pin the develop/main branching model's CI and deploy gates.

These guards live in GitHub Actions YAML, which nothing else exercises before
a real release. The tests fail loudly if a trigger or guard is dropped.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

WORKFLOWS = Path(__file__).resolve().parent.parent / ".github" / "workflows"


def _load(name: str) -> dict[str, Any]:
    data = yaml.safe_load((WORKFLOWS / name).read_text())
    assert isinstance(data, dict), f"{name} did not parse to a mapping"
    return data


def _triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    # YAML 1.1 parses a bare `on:` key as the boolean True.
    triggers = workflow.get("on", workflow.get(True))
    assert isinstance(triggers, dict), "workflow has no `on:` mapping"
    return triggers


def test_ci_runs_on_develop_and_main() -> None:
    triggers = _triggers(_load("ci.yml"))
    assert triggers["push"]["branches"] == ["main", "develop"]
    assert triggers["pull_request"]["branches"] == ["main", "develop"]


def test_ci_release_source_gates_prs_into_main() -> None:
    job = _load("ci.yml")["jobs"]["release-source"]
    assert job["name"] == "Release Source"
    assert "github.event_name == 'pull_request'" in job["if"]
    assert "github.base_ref == 'main'" in job["if"]
    script = job["steps"][0]["run"]
    assert "develop|hotfix/*)" in script
    assert "exit 1" in script


def test_staging_deploy_stays_manual() -> None:
    # Auto-deploy is deliberately disabled: staging has no registered Signal number.
    triggers = _triggers(_load("deploy-staging.yml"))
    assert set(triggers) == {"workflow_dispatch"}


def test_production_deploy_requires_release_tag_on_main() -> None:
    workflow = _load("deploy.yml")
    assert set(_triggers(workflow)) == {"workflow_dispatch"}
    steps = workflow["jobs"]["deploy"]["steps"]
    checkout, guard = steps[0], steps[1]
    assert checkout["uses"].startswith("actions/checkout@")
    assert checkout["with"]["fetch-depth"] == 0
    assert guard["name"] == "Require a release tag on main"
    assert "refs/tags/v*)" in guard["run"]
    assert "git fetch --no-tags origin main" in guard["run"]
    assert 'git merge-base --is-ancestor "$GITHUB_SHA" origin/main' in guard["run"]
    deploy_index = next(i for i, s in enumerate(steps) if "flyctl deploy" in s.get("run", ""))
    assert deploy_index > 1
