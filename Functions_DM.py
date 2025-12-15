from __future__ import annotations
from typing import Any, Dict, Iterable, Optional, Set, Mapping
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# Liste des clés obligatoires dans les fichiers JSON.
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



#--- Fonctions utilitaires ---------------------------------------------

# Conversion des données int en float.
def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (float, int)):
        return float(value)
    try:
        return float(str(value).strip())
    except (ValueError, TypeError):
        return None

# Conversion des dates 'AAAA-MM-JJ' en 'JJ-MM-AAAA'.
def format_iso_date_to_ddmmyyyy(value: str) -> Optional[str]:
    if not isinstance(value, str) or "-" not in value:
        return None
    try:
        dt = datetime.strptime(value, "%Y-%m-%d")
        return dt.strftime("%d/%m/%Y")
    except ValueError:
        return None

# Normalisation de la casse des clefs des dictionaires.
def deep_normalize_keys(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        result: Dict[str, Any] = {}
        for k, v in obj.items():
            new_key = k.lower() if isinstance(k, str) else k
            result[new_key] = _deep_normalize_keys(v)
        return result
    if isinstance(obj, list):
        return [_deep_normalize_keys(item) for item in obj]
    return obj



# --- Fonctions du pipeline --------------------------------------------------

# Normalisation des clefs des dictionnaires.
def normalisation_casse_clefs(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(x_dict_DM, Mapping):
        logger.debug("normalisation_casse_clefs: l'objet passé en argument de la fonction n'est pas de type 'Mapping'.")
        return {}
    return dict(_deep_normalize_keys(x_dict_DM))

# Suppression des clefs qui ne sont pas pertinentes pour le traitement.
def remove_irrelevant_data_points(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(x_dict_DM, Mapping):
        logger.debug("remove_irrelevant_data_points: l'objet passé en argument de la fonction n'est pas de type 'Mapping'.")
        return {}

    result: Dict[str, Any] = {}
    for key, value in x_dict_DM.items():
        if key in RELEVANT_KEYS:
            result[key] = value
        else:
            logger.debug("Suppression de la clef : '%s'", key)
    return result

# Conversion des dates pour les clefs 'last_maintenance_date' and 'next_maintenance_due'.
# Si la conversion échoue, on garde le format de la date d'origine (politique best effort).
def format_dates(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
    if not isinstance(x_dict_DM, Mapping):
        return {}

    result = dict(x_dict_DM)  # shallow copy
    for date_key in ("last_maintenance_date", "next_maintenance_due"):
        val = result.get(date_key)
        if isinstance(val, str):
            converted = format_iso_date_to_ddmmyyyy(val)
            if converted:
                logger.debug("Converted %s: %s -> %s", date_key, val, converted)
                result[date_key] = converted
            else:
                logger.debug("Pas de conversion de date effectuée pour : %s (value=%r)", date_key, val)
    return result

# Conversion des valeurs des clefs 'depth_capacity_miles' (-> 'depth_capacity_meters') et 'drilling_speed_miles_per_day'
# (-> 'drilling_speed_meters_per_day') en kms. Si la conversion échoue, on garde la valeur d'origine (politique best effort).
def convert_miles_to_meters(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
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
        logger.debug("Conversion de 'depth_capacity' : %r miles -> %r meters", depth_val, specs["depth_capacity_meters"])
        changed = True

    # drilling_speed
    speed_miles = specs.get("drilling_speed_miles_per_day")
    speed_val = _to_float(speed_miles)
    if speed_val is not None:
        specs.pop("drilling_speed_miles_per_day", None)
        specs["drilling_speed_meters_per_day"] = speed_val * METERS_PER_MILE
        logger.debug("Conversion de 'drilling_speed' : %r miles/day -> %r meters/day", speed_val, specs["drilling_speed_meters_per_day"])
        changed = True

    if changed:
        result["specifications"] = specs
    else:
        # S'il n'y a pas eu de conversion et que la valeur d'origine était présente, on garde le type "Mapping" d'origine.
        if "specifications" in result:
            result["specifications"] = specs

    return result

# Création de la clef 'contact_information' avec ses champs par défaut, si elle est manquante.
def missing_contact_information(x_dict_DM: Mapping[str, Any]) -> Dict[str, Any]:
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
        logger.debug("Ajout de la clef 'contact_information' et de ses valeurs par défaut.")
    return result
