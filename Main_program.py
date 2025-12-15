"""Traitement de fichiers JSON à travers un pipeline de validation et de transformation"""

from __future__ import annotations
from typing import Callable, Any, Dict, List
import json
from pathlib import Path
from collections import Counter
import tempfile
import os

from subfolder1.Logging import logger
from subfolder1.Functions_DM import (
    normalisation_casse_clefs,
    remove_irrelevant_data_points,
    format_dates,
    convert_miles_to_meters,
    missing_contact_information,
)

# Type alias pour le pipeline
PipelineStep = Callable[[Dict[str, Any]], Dict[str, Any]]

# Domain exceptions
class FileProcessingError(RuntimeError):
    """Erreur métier lors de la lecture ou de l'écriture des fichiers JSON."""

class PipelineStepError(RuntimeError):
    """Erreur métier lors de la transformation des JSON dans le pipeline."""

# indicateurs de performance du traitement
metrics = Counter({
    "files_total": 0,
    "files_processed": 0,
    "files_failed": 0,
    "pipeline_step_failures": 0,
})

def _step_name(step: PipelineStep) -> str:
    """Retourne un nom lisible et stable pour chaque step du pipeline (protection contre les fonctions lambdas et partielles)"""
    return getattr(step, "__name__", repr(step))

def process_file(in_path: Path, out_dir: Path, pipeline: List[PipelineStep]) -> None:
    """Chargement des JSON depuis in_path, exécution des transformations via le pipeline, et enregistrement des nouveaux JSON dans out_dir"""
    logger.info("Traitement du fichier %s", in_path.name)

    # Lecture des JSON. Interception des erreurs spécifiques et levée d'une erreur métier ("FileProcessingError").
    try:
        with in_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        # do not log here (top-level will log); raise domain-specific error
        raise FileProcessingError(f"Input file not found: {in_path}") from e
    except PermissionError as e:
        raise FileProcessingError(f"Permission denied reading {in_path}") from e
    except json.JSONDecodeError as e:
        raise FileProcessingError(f"Invalid JSON in {in_path}") from e
    except OSError as e:
        raise FileProcessingError(f"OS error reading {in_path}") from e

    # Exécution du pipeline. Interception des erreurs spécifiques et levée d'une erreur métier ("PipelineStepError").
    for step in pipeline:
        try:
            data = step(data)
        except Exception as e:
            name = _step_name(step)
            # metrics
            metrics["pipeline_step_failures"] += 1
            raise PipelineStepError(f"Erreur dans le step '{name}' pour le fichier '{in_path.name}'") from e

        if not isinstance(data, dict):
            name = _step_name(step)
            raise PipelineStepError(
                f"Le step '{name}' du pipeline n'a pas retourné un dictionnaire (fichier '{in_path.name}')"
            )

    # Écriture atomique des fichiers JSON.
    out_path = out_dir / in_path.name
    tmp_file = None
    try:
        fd, tmp_path_str = tempfile.mkstemp(prefix=f".{in_path.name}.tmp.", dir=str(out_dir))
        tmp_file = Path(tmp_path_str)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
            
        os.replace(str(tmp_file), str(out_path))

    # Suppression du fichier tmp_file s'il existe toujours.
    # Interception des erreurs spécifiques et levée d'une erreur métier ("FileProcessingError").
    except PermissionError as e:
        if tmp_file and tmp_file.exists():
            try:
                tmp_file.unlink()
            except Exception:
                pass
        raise FileProcessingError(f"Permission pour écrire '{out_path}' refusée.") from e
    except OSError as e:
        if tmp_file and tmp_file.exists():
            try:
                tmp_file.unlink()
            except Exception:
                pass
        raise FileProcessingError(f"'OSError' lors de l'écriture de '{out_path}'") from e
    except Exception:
        # Interception des erreurs inattendues : suppression du fichier tmp_file s'il existe, et levée de la même erreur.
        if tmp_file and tmp_file.exists():
            try:
                tmp_file.unlink()
            except Exception:
                pass
        raise

def main(
    fld_raw: Path,
    fld_processed: Path,
    pipeline: List[PipelineStep]
) -> None:
    fld_raw = fld_raw.expanduser().resolve()
    fld_processed = fld_processed.expanduser().resolve()
    if not fld_raw.is_dir():
        logger.error("Le dossier '%s' n'existe pas.", fld_raw)
        raise NotADirectoryError(f"Le dossier '{fld_raw}' n'existe pas.")
    fld_processed.mkdir(parents=True, exist_ok=True)

    files = sorted(fld_raw.glob("drilling_machine*.json"))
    if not files:
        logger.warning("Aucun fichier JSON n'a été trouvé dans '%s'", fld_raw)
        return

    # Traitement des fichiers JSON (politique du 'best effort' : si une erreur survient lors du traitement d'un fichier, on log l'erreur et on passe au fichier suivant.)
    success = 0
    failures = 0
    metrics["files_total"] += len(files)

    for file_path in files:
        try:
            process_file(file_path, fld_processed, pipeline)
            success += 1
            metrics["files_processed"] += 1
            logger.info("Fichier '%s' traité avec succès (success=%d, failures=%d).", file_path.name, success, failures)
        # Journalisation des erreurs et du stack traceback (erreurs remontées du pipeline - fonction 'process_file').
        except PipelineStepError as exc:
            failures += 1
            metrics["files_failed"] += 1
            logger.exception("PipelineStepError pour le fichier '%s': %s", file_path.name, exc
        except FileProcessingError as exc:
            failures += 1
            metrics["files_failed"] += 1
            logger.exception("FileProcessingError processing %s: %s", file_path.name, exc)
        except Exception as exc:
            logger.exception("Unexpected error processing %s: %s", file_path.name, exc)
            raise

    logger.info("Traitement des fichiers terminé : %s succès, %s échec(s) - (nbr total de fichiers : %s)", success, failures, len(files))

# main guard.
if __name__ == "__main__":
    RAW = Path(r"C:\users\jmcha\desktop\raw")
    PROCESSED = Path(r"C:\users\jmcha\desktop\processed")

    pipeline: List[PipelineStep] = [
        normalisation_casse_clefs,
        remove_irrelevant_data_points,
        format_dates,
        convert_miles_to_meters,
        missing_contact_information,
    ]

    main(RAW, PROCESSED, pipeline)

    






