"""
Utility transformations for drilling_machine JSON objects.

Design goals:
- Robust to malformed input
- No prints; use logging
- Return a new transformed dict (do not mutate the original)
- Clear typing and docstrings for easier review & unit testing
"""

from __future__ import annotations
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Set
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Allowed top-level keys (kept as a frozenset for fast membership checks)
RELEVANT_KEYS: Set[str] = frozenset(
    {
        "machine_id",
        "name",
        "location",
        "status",
        "specifications",
        "last_maintenance_date",
        "next_maintenance_due",
        "contact_information",
    }
)

METERS_PER_MILE: float = 1609.0


# --- Helpers -----------------------------------------------------------------
def _to_float(value: Any) -> Optional[float]:
    """Safely convert value to float. Return None on failure."""
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return float(value)
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None


def _format_iso_date_to_ddmmyyyy(value: str) -> Optional[str]:
    """
    Convert ISO-like date 'YYYY-MM-DD' to 'DD/MM/YYYY'.
    If parsing fails, return None.
    """
    if not isinstance(value, str) or "-" not in value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        # Not strictly ISO, ignore conversion
        return None


def _deep_normalize_keys(obj: Any) -> Any:
    """
    Recursively lower-case dict keys. For lists, apply to each element.
    Returns a new object (does not mutate input).
    """
    if isinstance(obj, Mapping):
        result: Dict[str, Any] = {}
        for k, v in obj.items():
            new_key = k.lower() if isinstance(k, str) else k
            result[new_key] = _deep_normalize_keys(v)
        return result
    if isinstance(obj, list):
        return [_deep_normalize_keys(item) for item in obj]
    return obj


# --- Public pipeline steps --------------------------------------------------
def normalisation_casse_clefs(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Return a deep-copied dict where all string keys are lower-cased.
    This function will not mutate the input.
    """
    if not isinstance(x_dict_DM, Mapping):
        logger.debug("normalisation_casse_clefs: input is not a mapping; returning empty dict")
        return {}
    return dict(_deep_normalize_keys(x_dict_DM))


def remove_irrelevant_data_points(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Remove top-level keys that are not in RELEVANT_KEYS.
    Keeps nested structures inside relevant keys untouched.
    Returns a new dict.
    """
    if not isinstance(x_dict_DM, Mapping):
        logger.debug("remove_irrelevant_data_points: input is not a mapping; returning empty dict")
        return {}

    result: Dict[str, Any] = {}
    for key, value in x_dict_DM.items():
        if key in RELEVANT_KEYS:
            result[key] = value
        else:
            logger.debug("Dropping irrelevant top-level key: %s", key)
    return result


def format_dates(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Convert ISO 'YYYY-MM-DD' date strings in 'last_maintenance_date' and
    'next_maintenance_due' to 'DD/MM/YYYY'. If conversion fails, keep original value.
    Returns a new dict (shallow copy with potential modified date strings).
    """
    if not isinstance(x_dict_DM, Mapping):
        return {}

    result = dict(x_dict_DM)  # shallow copy
    for date_key in ("last_maintenance_date", "next_maintenance_due"):
        val = result.get(date_key)
        if isinstance(val, str):
            converted = _format_iso_date_to_ddmmyyyy(val)
            if converted:
                logger.debug("Converted %s: %s -> %s", date_key, val, converted)
                result[date_key] = converted
            else:
                logger.debug("No conversion for %s (value=%r)", date_key, val)
    return result


def convert_miles_to_meters(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Convert known mileage fields under 'specifications' to meters.
    - 'depth_capacity_miles' -> 'depth_capacity_meters'
    - 'drilling_speed_miles_per_day' -> 'drilling_speed_meters_per_day'
    Conversion is best-effort; if value cannot be parsed, the original field is left untouched.
    Returns a new dict (shallow copy); 'specifications' is copied if present.
    """
    if not isinstance(x_dict_DM, Mapping):
        return {}

    result = dict(x_dict_DM)
    specs = dict(result.get("specifications", {})) if isinstance(result.get("specifications"), Mapping) else {}
    changed = False

    # depth_capacity
    depth_miles = specs.get("depth_capacity_miles")
    depth_val = _to_float(depth_miles)
    if depth_val is not None:
        specs.pop("depth_capacity_miles", None)
        specs["depth_capacity_meters"] = depth_val * METERS_PER_MILE
        logger.debug("Converted depth_capacity: %r miles -> %r meters", depth_val, specs["depth_capacity_meters"])
        changed = True

    # drilling_speed
    speed_miles = specs.get("drilling_speed_miles_per_day")
    speed_val = _to_float(speed_miles)
    if speed_val is not None:
        specs.pop("drilling_speed_miles_per_day", None)
        specs["drilling_speed_meters_per_day"] = speed_val * METERS_PER_MILE
        logger.debug("Converted drilling_speed: %r miles/day -> %r meters/day", speed_val, specs["drilling_speed_meters_per_day"])
        changed = True

    if changed:
        result["specifications"] = specs
    else:
        # If nothing changed but original specs was present, keep original mapping type
        if "specifications" in result:
            result["specifications"] = specs

    return result


def missing_contact_information(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Ensure 'contact_information' exists with a known shape.
    Returns a new dict (shallow copy).
    """
    if not isinstance(x_dict_DM, Mapping):
        return {}

    result = dict(x_dict_DM)
    contact = result.get("contact_information")
    if not isinstance(contact, Mapping):
        result["contact_information"] = {
            "operator_company": None,
            "contact_person": None,
            "phone": None,
            "email": None,
        }
        logger.debug("Added missing contact_information template")
    return result
