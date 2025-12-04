import logging
from pathlib import Path
from datetime import datetime

# Set default log directory (creates if not exist)
LOG_DIR = Path.home() / "desktop" / "PYTHON-LOGS"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Create a new log file for each run with timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"Dev-Benjamin_{timestamp}.log"

# Define the log format
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

# Use FileHandler
handler = logging.FileHandler(filename=log_file, encoding="utf-8")
handler.setFormatter(formatter)

# Get root logger and attach handler
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Optional: also log to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
