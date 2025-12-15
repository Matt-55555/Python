import logging
from pathlib import Path
from datetime import datetime

# Définition du répertoire contenant le log.
LOG_DIR = Path.home() / "desktop" / "PYTHON-LOGS"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# Création d'un nouveau fichier log horodaté pour chaque lancement du programme.
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"Dev-Benjamin_{timestamp}.log"

# Définition du format du log.
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

# Définition du Handler.
handler = logging.FileHandler(filename=log_file, encoding="utf-8")
handler.setFormatter(formatter)

# Connection du Handler au root logger.
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

# Affichage du logging dans le Terminal.
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

