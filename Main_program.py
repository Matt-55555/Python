# # Nous avons 5 fichiers JSON dans le dossier "raw", qui représentent des
# # données collectées sur 5 machines de forage différentes. Nous voulons opérer
# # les actions suivantes sur ces fichiers :

# # - normaliser la casse des clefs des dictionnaires (et dictionnaires imbriqués)
# # des fichiers JSON,
# # - enlever les données qui ne nous intéressent pas,
# # - convertir les dates en format jj/mm/aaaa lorsqu'elles sont en format aaaa-mm-dd,
# # - convertir les données exprimées en miles en données exprimées en mètres,
# # - lorsqu'il est manquant, ajouter un champs "contact information" qui contient les
# # informations "operator_company", "contact_person", "phone", et "email",
# # - uniformiser le formatage de la valeur du champs "machine_id", de manière à ce que le
# # numéro de l'ID soit toujours composé de 3 nombre (mettre des leading 0 si le numéro de
# # l'ID ne comporte qu'un ou deux chiffres).

# # Une fois ces actions effectuées, nous voulons enregistrer ces fichiers JSON
# # traités dans un répertoire nommé "processed".

# # ------------------------------------------------------------------------------------------
# # ------------------------------------------------------------------------------------------



"""Process drilling_machine JSON files through a validation/transformation pipeline."""

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

# Type alias for the pipeline
PipelineStep = Callable[[Dict[str, Any]], Dict[str, Any]]

# Domain exceptions
class FileProcessingError(RuntimeError):
    """High-level error when reading/writing JSON files."""

class PipelineStepError(RuntimeError):
    """High-level error when a pipeline step fails."""

# in-process metrics
metrics = Counter({
    "files_total": 0,
    "files_processed": 0,
    "files_failed": 0,
    "pipeline_step_failures": 0,
})

def _step_name(step: PipelineStep) -> str:
    """Return a stable readable name for a step (works for lambdas/partials)."""
    return getattr(step, "__name__", repr(step))

def process_file(in_path: Path, out_dir: Path, pipeline: List[PipelineStep]) -> None:
    """Load JSON from in_path, run it through pipeline steps, and save result to out_dir."""
    # keep the informational log (not an error log)
    logger.info("Processing %s", in_path.name)

    # --- Read file: catch specific exceptions, raise domain error ---
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

    # --- Pipeline steps: wrap any step failure in PipelineStepError ---
    for step in pipeline:
        try:
            data = step(data)
        except Exception as e:
            name = _step_name(step)
            # metrics
            metrics["pipeline_step_failures"] += 1
            # raise wrapped error; top-level will log
            raise PipelineStepError(f"Error in step '{name}' for file '{in_path.name}'") from e

        if not isinstance(data, dict):
            name = _step_name(step)
            raise PipelineStepError(
                f"Pipeline step '{name}' did not return a dict (file {in_path.name})"
            )

    # Write file atomically and durably: write to a temp file in the same directory,
    # fsync, then atomically replace the target file.
    out_path = out_dir / in_path.name
    tmp_file = None
    try:
        # Create a named temp file in the output directory so os.replace stays on same filesystem
        # delete=False so we can fsync and then move it
        fd, tmp_path_str = tempfile.mkstemp(prefix=f".{in_path.name}.tmp.", dir=str(out_dir))
        tmp_file = Path(tmp_path_str)
        # Wrap fd as a file object opened in text mode with UTF-8 encoding
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            # Dump JSON to the temp file
            json.dump(data, f, ensure_ascii=False, indent=2)
            # Ensure Python buffers are flushed
            f.flush()
            # Make sure OS buffers are flushed to disk
            os.fsync(f.fileno())

        # Atomic replace: move temp into final location
        # os.replace is atomic when src and dst are on same filesystem
        os.replace(str(tmp_file), str(out_path))

    except PermissionError as e:
        # Clean up temp file if exists
        if tmp_file and tmp_file.exists():
            try:
                tmp_file.unlink()
            except Exception:
                pass
        raise FileProcessingError(f"Permission denied writing {out_path}") from e
    except OSError as e:
        if tmp_file and tmp_file.exists():
            try:
                tmp_file.unlink()
            except Exception:
                pass
        raise FileProcessingError(f"OS error writing {out_path}") from e
    except Exception:
        # Remove temp file on any unexpected error, then re-raise (so top-level can decide)
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
        logger.error("Input folder does not exist: %s", fld_raw)
        raise NotADirectoryError(f"Input folder does not exist: {fld_raw}")
    fld_processed.mkdir(parents=True, exist_ok=True)

    files = sorted(fld_raw.glob("drilling_machine*.json"))
    if not files:
        logger.warning("No files matched in %s", fld_raw)
        return

    # Policy: attempt to process all files; collect failures and continue
    success = 0
    failures = 0
    metrics["files_total"] += len(files)

    for file_path in files:
        try:
            process_file(file_path, fld_processed, pipeline)
            success += 1
            metrics["files_processed"] += 1
            logger.info("Processed %s (success=%d, failures=%d)", file_path.name, success, failures)
        except PipelineStepError as exc:
            failures += 1
            metrics["files_failed"] += 1
            # Top-level logging only: single place logs the exception and stack trace
            logger.exception("PipelineStepError processing %s: %s", file_path.name, exc)
            # continue to next file (best-effort)
        except FileProcessingError as exc:
            failures += 1
            metrics["files_failed"] += 1
            logger.exception("FileProcessingError processing %s: %s", file_path.name, exc)
            # continue to next file
        except Exception as exc:
            # Unexpected error: log and re-raise so the process fails loudly (indicates bug)
            logger.exception("Unexpected error processing %s: %s", file_path.name, exc)
            raise

    logger.info("Processing complete: %d success, %d failures (total %d files)", success, failures, len(files))
    # Hook: push metrics to your monitoring system here if desired
    # Example (pseudo): push_metrics(metrics)

if __name__ == "__main__":
    RAW = Path(r"C:\users\jmcha\desktop\data - benjamin dubreu\raw")
    PROCESSED = Path(r"C:\users\jmcha\desktop\data - benjamin dubreu\processed")

    pipeline: List[PipelineStep] = [
        normalisation_casse_clefs,
        remove_irrelevant_data_points,
        format_dates,
        convert_miles_to_meters,
        missing_contact_information,
    ]

    main(RAW, PROCESSED, pipeline)

    
