import logging
from pathlib import Path


def configure_logging(log_level: str = "INFO") -> None:
    """Configure console and file logging for the pipeline."""
    log_dir = Path(__file__).resolve().parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8"),
        ],
    )

    logging.getLogger("pyspark").setLevel(logging.WARNING)
