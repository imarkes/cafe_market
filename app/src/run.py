from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Any

from .job_payload import payload
from .jobs.job_bronze import JobBronzeData
from .jobs.job_gold import JobGoldData
from .jobs.job_raw import JobRawData
from .jobs.job_silver import JobSilverData
from .pipeline.manager import DataManager
from .utils.logging_config import configure_logging

configure_logging(os.getenv("LOG_LEVEL", "INFO"))

logger = logging.getLogger(__name__)


SUPPORTED_LAYERS = (
    "all",
    "download",
    "raw",
    "bronze",
    "silver",
    "gold",
)

SUPPORTED_BCB_SERIES = (
    "all",
    "selic",
    "ipca",
)


def _env_bool(name: str, default: bool = False) -> bool:
    """
    Converte uma variável de ambiente textual para booleano.

    Valores reconhecidos como verdadeiros:
    ``1``, ``true``, ``yes``, ``y`` e ``on``.
    """

    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


@dataclass(frozen=True)
class PipelineConfig:
    """
    Configuração de execução da pipeline.

    Attributes
    ----------
    layer
        Camada que será executada.

    source
        Fonte específica do payload. Quando ausente, a camada processa
        todas as fontes aplicáveis.

    download_bcb
        Define se as séries do Banco Central devem ser baixadas antes
        do fluxo completo.

    bcb_series
        Série econômica que será baixada: ``selic``, ``ipca`` ou ``all``.
    """

    layer: str = "all"
    source: str | None = None
    download_bcb: bool = False
    bcb_series: str = "all"

    def validate(
        self,
        pipeline_payload: dict[str, dict[str, Any]],
    ) -> None:
        """Valida a configuração antes de iniciar os jobs."""

        if self.layer not in SUPPORTED_LAYERS:
            raise ValueError(
                f"Camada inválida: {self.layer!r}. "
                f"Valores permitidos: {', '.join(SUPPORTED_LAYERS)}."
            )

        if self.bcb_series not in SUPPORTED_BCB_SERIES:
            raise ValueError(
                f"Série BCB inválida: {self.bcb_series!r}. "
                f"Valores permitidos: {', '.join(SUPPORTED_BCB_SERIES)}."
            )

        if self.source and self.source not in pipeline_payload:
            raise ValueError(
                f"A fonte {self.source!r} não existe no payload. "
                f"Fontes disponíveis: {', '.join(pipeline_payload)}."
            )

        if self.layer == "all" and self.source:
            raise ValueError(
                "PIPELINE_SOURCE não pode ser usado com PIPELINE_LAYER=all. "
                "Para executar uma fonte específica, escolha raw, bronze ou silver."
            )

        if self.layer == "gold" and self.source:
            raise ValueError(
                "A camada Gold depende do conjunto das fontes e não aceita "
                "a execução por uma única PIPELINE_SOURCE."
            )

        if self.layer == "download" and self.source:
            if self.source not in {"selic", "ipca"}:
                raise ValueError(
                    "A camada download aceita somente as fontes "
                    "'selic' ou 'ipca'."
                )


class PipelineRunner:
    """
    Orquestra a execução das camadas da aplicação.

    O DataManager é criado de forma tardia para evitar a inicialização
    desnecessária do Spark em execuções exclusivas da camada Raw ou Gold.
    """

    def __init__(
        self,
        pipeline_payload: dict[str, dict[str, Any]],
        config: PipelineConfig,
    ) -> None:
        self.payload = pipeline_payload
        self.config = config
        self._manager: DataManager | None = None

    @property
    def manager(self) -> DataManager:
        """Cria o DataManager somente quando uma operação Spark é solicitada."""

        if self._manager is None:
            logger.info("Initializing DataManager and Spark session...")
            self._manager = DataManager()

        return self._manager

    def run(self) -> None:
        """Executa a camada selecionada na configuração."""

        self.config.validate(self.payload)

        logger.info(
            "Starting pipeline | layer=%s | source=%s",
            self.config.layer,
            self.config.source or "all",
        )

        try:
            runners = {
                "all": self._run_all,
                "download": self._run_download,
                "raw": self._run_raw,
                "bronze": self._run_bronze,
                "silver": self._run_silver,
                "gold": self._run_gold,
            }

            runners[self.config.layer]()

            logger.info(
                "Pipeline finished successfully | layer=%s",
                self.config.layer,
            )

        except Exception:
            logger.exception(
                "Pipeline failed | layer=%s | source=%s",
                self.config.layer,
                self.config.source or "all",
            )
            raise

        finally:
            self._stop_spark()

    def _run_all(self) -> None:
        """
        Executa o pipeline completo.

        Ordem:
        1. Download opcional das séries BCB.
        2. Preparação Raw dos arquivos INMET.
        3. Bronze.
        4. Silver.
        5. Gold.
        """

        if self.config.download_bcb:
            self._download_bcb_series(
                series_name=self.config.bcb_series,
            )

        self._run_raw()
        self._run_bronze()
        self._run_silver()
        self._run_gold()

    def _run_download(self) -> None:
        """Baixa uma ou todas as séries econômicas do Banco Central."""

        series_name = (
            self.config.source
            if self.config.source
            else self.config.bcb_series
        )

        self._download_bcb_series(series_name)

    def _download_bcb_series(
        self,
        series_name: str,
    ) -> None:
        """Executa o download das séries BCB configuradas."""

        tax_name = None if series_name == "all" else series_name

        downloaded_files = self.manager.download_tax_series(
            payload=self.payload,
            tax_name=tax_name,
        )

        for name, path in downloaded_files.items():
            logger.info(
                "BCB series downloaded | source=%s | path=%s",
                name,
                path,
            )

    def _run_raw(self) -> None:
        """
        Prepara os arquivos CSV do INMET.

        Quando ``source`` é informado, somente essa fonte é processada.
        Caso contrário, todas as fontes cujo nome começa com
        ``inmet_`` são preparadas.
        """

        job = JobRawData(payload=self.payload)

        if self.config.source:
            if not self.config.source.startswith("inmet_"):
                raise ValueError(
                    "A camada Raw atual processa apenas fontes INMET. "
                    f"Fonte recebida: {self.config.source!r}."
                )

            job.prepare_file_csv_inmet(self.config.source)
            return

        inmet_sources = [
            source_name
            for source_name in self.payload
            if source_name.startswith("inmet_")
        ]

        if not inmet_sources:
            logger.warning("No INMET sources found in payload.")
            return

        for source_name in inmet_sources:
            job.prepare_file_csv_inmet(source_name)

    def _run_bronze(self) -> None:
        """Executa a ingestão da camada Bronze."""

        job = JobBronzeData(
            payload=self.payload,
            run=self.manager,
        )

        if self.config.source:
            job.process_source(self.config.source)
            return

        job.run_all()

    def _run_silver(self) -> None:
        """Executa a padronização da camada Silver."""

        job = JobSilverData(
            payload=self.payload,
            run=self.manager,
        )

        if self.config.source:
            job.process_data(self.config.source)
            return

        job.run_all()

    def _run_gold(self) -> None:
        """Cria o Data Warehouse analítico no DuckDB."""

        job = JobGoldData(payload=self.payload)
        job.create_datawarehouse()

    def _stop_spark(self) -> None:
        """Encerra a sessão Spark caso ela tenha sido inicializada."""

        if self._manager is None:
            return

        spark = getattr(self._manager, "spark", None)

        if spark is not None:
            logger.info("Stopping Spark session...")
            spark.stop()


def parse_arguments() -> PipelineConfig:
    """
    Lê argumentos da linha de comando.

    Os valores das variáveis de ambiente são utilizados como padrão.
    Argumentos informados no terminal possuem precedência.
    """

    parser = argparse.ArgumentParser(
        description="Culttivo data pipeline runner.",
    )

    parser.add_argument(
        "--layer",
        choices=SUPPORTED_LAYERS,
        default=os.getenv("PIPELINE_LAYER", "all").strip().lower(),
        help=(
            "Camada que será executada. "
            "Valores: all, download, raw, bronze, silver ou gold."
        ),
    )

    parser.add_argument(
        "--source",
        default=os.getenv("PIPELINE_SOURCE") or None,
        help=(
            "Fonte específica do payload, por exemplo: "
            "inmet_franca, robusta, selic ou ipca."
        ),
    )

    parser.add_argument(
        "--download-bcb",
        action=argparse.BooleanOptionalAction,
        default=_env_bool("DOWNLOAD_BCB", False),
        help=(
            "Baixa as séries do Banco Central antes da execução completa."
        ),
    )

    parser.add_argument(
        "--bcb-series",
        choices=SUPPORTED_BCB_SERIES,
        default=os.getenv("BCB_SERIES", "all").strip().lower(),
        help="Série BCB a baixar: all, selic ou ipca.",
    )

    arguments = parser.parse_args()

    return PipelineConfig(
        layer=arguments.layer,
        source=arguments.source,
        download_bcb=arguments.download_bcb,
        bcb_series=arguments.bcb_series,
    )


def main() -> None:
    """Ponto de entrada principal da aplicação."""

    config = parse_arguments()

    runner = PipelineRunner(
        pipeline_payload=payload,
        config=config,
    )

    runner.run()


if __name__ == "__main__":
    main()
