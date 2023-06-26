from copy import deepcopy
import json
from typing import Dict


def update_source_config(base_config: Dict, test_case: Dict):
    section: str = "sources"
    base = deepcopy(base_config)
    base[section][0].update(
        {k: v for k, v in test_case.items() if k not in ["parameters", "expected"]}
    )
    base[section][0]["parameters"].update(test_case.get("parameters", {}))
    updated = base
    return updated


def update_transform_config(base_config: Dict, test_case: Dict):
    section: str = "transforms"
    base = deepcopy(base_config)
    base[section][0].update(
        {k: v for k, v in test_case.items() if k not in ["parameters", "expected"]}
    )
    base[section][0]["parameters"].update(test_case.get("parameters", {}))
    updated = base
    return updated


def update_sink_config(base_config: Dict, test_case: Dict):
    section: str = "sinks"
    base = deepcopy(base_config)
    base[section][0].update(
        {k: v for k, v in test_case.items() if k not in ["parameters", "expected"]}
    )
    base[section][0]["parameters"].update(test_case.get("parameters", {}))
    updated = base
    return updated


def update_profile_config(base_profile: Dict, test_case: Dict):
    section: str = "test"
    base = deepcopy(base_profile)
    base[section].update({k: v for k, v in test_case.items()})
    updated = base
    return updated


def update_config_json(tcase, config_path):
    with open(config_path, "r+") as f:
        base_conf = json.loads(f.read())
        if tcase.get("sources", None):
            base_conf = update_source_config(base_conf, tcase["sources"][0])
        if tcase.get("transforms", None):
            base_conf = update_transform_config(base_conf, tcase["transforms"][0])
        if tcase.get("sinks", None):
            base_conf = update_sink_config(base_conf, tcase["sinks"][0])

        updated = base_conf

        # overwrite json
        f.seek(0)
        f.write(json.dumps(updated))
        f.truncate()

    return config_path


def update_profile_json(tcase, profile_path):
    with open(profile_path, "r+") as f:
        base_profile = json.loads(f.read())
        if tcase.get("test", None):
            updated = update_profile_config(base_profile, tcase["test"])

        # overwrite json
        f.seek(0)
        f.write(json.dumps(updated))
        f.truncate()

    return profile_path
