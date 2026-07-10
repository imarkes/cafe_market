from __future__ import annotations

import logging
from pathlib import Path
from urllib.request import urlretrieve

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
STORAGE_DIR = PROJECT_ROOT / "storage"
RAW_DIR = STORAGE_DIR / "raw"

class TaxDataDownloader:
    """Consome a API do Banco Central para baixar séries temporais de dados econômicos.
    Séries a ingerir:
    • SELIC — código 11 (taxa diária) ou 432 (meta Selic). Escolha e justifique.
    • IPCA — código 433 (variação mensal).
    """

    def __init__(self, output_dir: str | Path | None = None) -> None:
        self.output_dir = Path(RAW_DIR or output_dir)
        # self.output_dir.mkdir(parents=True, exist_ok=True)

    def download(self, code: int, start_date: str, end_date: str) -> Path:
        url = (
            "https://api.bcb.gov.br/dados/serie/bcdata.sgs."
            f"{code}/dados?formato=json&dataInicial={start_date}&dataFinal={end_date}"
        )
        output_path = self.output_dir / f"serie_{code}.json"
        logger.info("Downloading series %s to %s", code, output_path)
        urlretrieve(url, output_path)
        return output_path
