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

# indicateur de performance du process
metrics = Counter({
    "files_total": 0,
    "files_processed": 0,
    "files_failed": 0,
    "pipeline_step_failures": 0,
})

def _step_name(step: PipelineStep) -> str:
    """Retourne un nom lisible et stable pour chaque step du pipeline (protection contre les fonctions lambdas et fonctions partielles)"""
    return getattr(step, "__name__", repr(step))

def process_file(in_path: Path, out_dir: Path, pipeline: List[PipelineStep]) -> None:
    """Chargement des JSON depuis in_path, exécution des transformations via le pipeline, et enregistrement des nouveaux JSON dans out_dir"""
    logger.info("Processing %s", in_path.name)

    # Lecture des JSON - interception d'erreurs spécifiques et levée d'une erreur sur mesure ("FileProcessingError").
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

    # Exécution du pipeline - interception des erreurs spécifiques et levée d'une erreur sur mesure ("PipelineStepError").
    for step in pipeline:
        try:
            data = step(data)
        except Exception as e:
            name = _step_name(step)
            # metrics
            metrics["pipeline_step_failures"] += 1
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

    



