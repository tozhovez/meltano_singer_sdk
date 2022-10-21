"""Helper functions for Meltano and MeltanoHub interop."""

from __future__ import annotations

from ._typing import (
    is_array_type,
    is_boolean_type,
    is_date_or_datetime_type,
    is_integer_type,
    is_object_type,
    is_secret_type,
    is_string_type,
)


def _to_meltano_kind(jsonschema_def: dict) -> str | None:
    """Returns a Meltano `kind` indicator for the provided JSON Schema property node.

    For reference:
    https://docs.meltano.com/reference/plugin-definition-syntax#settingskind

    Args:
        jsonschema_type: JSON Schema type to check.

    Returns:
        A string representing the meltano 'kind'.
    """
    if is_secret_type(jsonschema_def):
        return "password"

    if is_date_or_datetime_type(jsonschema_def):
        return "date_iso8601"

    if is_string_type(jsonschema_def):
        return "string"

    if is_object_type(jsonschema_def):
        return "object"

    if is_array_type(jsonschema_def):
        return "array"

    if is_boolean_type(jsonschema_def):
        return "boolean"

    if is_integer_type(jsonschema_def):
        return "integer"

    return None


def meltano_yaml_str(
    plugin_name: str,
    capabilities: list[str],
    config_jsonschema: dict,
) -> str:
    """Returns a Meltano plugin definition as a yaml string.

    Args:
        plugin_name: Name of the plugin.
        capabilities: List of capabilities.
        config_jsonschema: JSON Schema of the expected config.

    Returns:
        A string representing the Meltano plugin Yaml definition.
    """
    capabilities_str: str = "\n".join(
        [f" - {capability}" for capability in capabilities]
    )
    settings_str: str = "\n".join(
        [
            f"""- name: {setting_name}
  label: {setting_name.replace("_", " ").title()}
  kind: {_to_meltano_kind(property_node)}
  description: {property_node.get("description", 'null')}"""
            for setting_name, property_node in config_jsonschema["properties"].items()
        ]
    )
    required_settings = [
        setting_name
        for setting_name, type_dict in config_jsonschema["properties"].items()
        if setting_name in config_jsonschema.get("required", [])
        or type_dict.get("required", False)
    ]
    settings_group_validation_str = " - - " + "\n   - ".join(required_settings)

    return f"""name: {plugin_name}
namespace: {plugin_name.replace('-', '_')}

## The following could not be auto-detected:
# maintenance_status:   #
# repo:                 #
# variant:              #
# label:                #
# description:          #
# pip_url:              #
# domain_url:           #
# logo_url:             #
# keywords: []          #

capabilities:
{capabilities_str}
settings_group_validation:
{settings_group_validation_str}
settings:
{settings_str}
"""