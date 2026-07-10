from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


PROJECT_ROOT = Path(__file__).resolve().parents[3]

STORAGE_DIR = PROJECT_ROOT /"storage"

RAW_DIR = STORAGE_DIR / "raw"
META_DIR = STORAGE_DIR / "meta"


class JobRawData:
    """
    Prepara arquivos brutos que precisam de tratamento antes da camada Bronze.

    Atualmente, a classe trata arquivos CSV do INMET contendo:

    - metadados da estação no início do arquivo;
    - cabeçalho tabular após os metadados;
    - valores separados por ponto e vírgula;
    - nomes de colunas específicos do INMET.
    """

    INMET_COLUMN_MAPPING = {
        "Data Medicao": "data_medicao",
        "PRECIPITACAO TOTAL, DIARIO (AUT)(mm)": "precipitacao_total_mm",
        "TEMPERATURA MAXIMA, DIARIA (AUT)(°C)": "temp_max_c",
        "TEMPERATURA MEDIA, DIARIA (AUT)(°C)": "temp_media_c",
        "TEMPERATURA MINIMA, DIARIA (AUT)(°C)": "temp_min_c",
        "UMIDADE RELATIVA DO AR, MEDIA DIARIA (AUT)(%)": "umidade_rel_media",
        "UMIDADE RELATIVA DO AR, MINIMA DIARIA (AUT)(%)": "umidade_rel_minima",
    }

    def __init__(self, payload: dict[str, dict[str, Any]]) -> None:
        self.payload = payload

    def prepare_file_csv_inmet(self, source_name: str) -> dict[str, str]:
        """
        Prepara um arquivo CSV do INMET para ingestão na camada Bronze.

        O processamento executa as seguintes etapas:

        1. Valida a fonte no payload.
        2. Lê o arquivo bruto.
        3. Extrai os metadados da estação.
        4. Persiste os metadados em JSON.
        5. Localiza o cabeçalho tabular.
        6. Padroniza os nomes das colunas.
        7. Grava uma cópia limpa do CSV.
        8. Atualiza ``path_raw_cleaned`` no payload.

        Parameters
        ----------
        source_name
            Nome da fonte configurada no payload, como
            ``inmet_franca`` ou ``inmet_patrocinio``.

        Returns
        -------
        dict[str, str]
            Caminhos dos arquivos de metadados e CSV limpo.

        Raises
        ------
        KeyError
            Quando a fonte não existe no payload.

        ValueError
            Quando a fonte não possui ``path_raw`` ou o cabeçalho
            do INMET não é encontrado.

        FileNotFoundError
            Quando o arquivo bruto não existe.
        """

        logger.info("Starting raw preparation for source: %s", source_name)

        source_payload = self._get_source_payload(source_name)
        source_path = self._resolve_source_path(source_payload)

        lines = self._read_source_lines(source_path)

        metadata = self._extract_metadata(lines)

        metadata_path = self._write_metadata(
            source_name=source_name,
            metadata=metadata,
        )

        header_index = self._find_header_index(lines)

        data_lines = self._prepare_data_lines(
            lines=lines,
            header_index=header_index,
        )

        cleaned_path = self._resolve_cleaned_path(
            source_name=source_name,
            source_path=source_path,
            source_payload=source_payload,
        )

        self._write_cleaned_file(
            data_lines=data_lines,
            cleaned_path=cleaned_path,
        )

        source_payload["metadata_path"] = str(metadata_path)
        source_payload["path_raw_cleaned"] = str(cleaned_path)

        logger.info(
            "Raw preparation completed for '%s'. Cleaned file: %s",
            source_name,
            cleaned_path,
        )

        return {
            "metadata_path": str(metadata_path),
            "cleaned_path": str(cleaned_path),
        }

    def _get_source_payload(self, source_name: str) -> dict[str, Any]:
        """Obtém e valida a configuração da fonte."""

        source_payload = self.payload.get(source_name)

        if not isinstance(source_payload, dict):
            raise KeyError(
                f"Source '{source_name}' was not found or has an invalid payload."
            )

        return source_payload

    def _resolve_source_path(
        self,
        source_payload: dict[str, Any],
    ) -> Path:
        """Resolve e valida o caminho do arquivo bruto."""

        raw_path = source_payload.get("path_raw")

        if not raw_path:
            raise ValueError("The source payload does not contain 'path_raw'.")

        source_path = Path(raw_path).expanduser()

        if not source_path.is_absolute():
            source_path = (PROJECT_ROOT / source_path).resolve()
        else:
            source_path = source_path.resolve()

        if not source_path.exists():
            raise FileNotFoundError(
                f"INMET raw file was not found: {source_path}"
            )

        if not source_path.is_file():
            raise ValueError(
                f"INMET raw path is not a file: {source_path}"
            )

        return source_path

    @staticmethod
    def _read_source_lines(source_path: Path) -> list[str]:
        """Lê o arquivo utilizando UTF-8 e remove o BOM, quando existente."""

        try:
            content = source_path.read_text(encoding="utf-8-sig")
        except UnicodeDecodeError:
            logger.warning(
                "UTF-8 decoding failed for %s. Trying Latin-1.",
                source_path,
            )
            content = source_path.read_text(encoding="latin-1")

        lines = content.splitlines()

        if not lines:
            raise ValueError(f"Source file is empty: {source_path}")

        return lines

    @staticmethod
    def _extract_metadata(lines: list[str]) -> dict[str, str]:
        """
        Extrai os metadados presentes antes do cabeçalho tabular.

        A extração é interrompida ao encontrar ``Data Medicao`` para
        impedir que linhas de dados sejam avaliadas como metadados.
        """

        metadata: dict[str, str] = {}

        for line in lines:
            stripped = line.strip().lstrip("\ufeff")

            if stripped.startswith("Data Medicao"):
                break

            if not stripped or ":" not in stripped:
                continue

            key, value = stripped.split(":", 1)

            normalized_key = key.strip()
            normalized_value = value.strip()

            if normalized_key and normalized_value:
                metadata[normalized_key] = normalized_value

        return metadata

    @staticmethod
    def _write_metadata(
        source_name: str,
        metadata: dict[str, str],
    ) -> Path:
        """Persiste os metadados da estação em JSON."""

        metadata_dir = META_DIR / source_name
        metadata_dir.mkdir(parents=True, exist_ok=True)

        metadata_path = metadata_dir / "metadata.json"

        metadata_path.write_text(
            json.dumps(
                metadata,
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        logger.info(
            "Metadata file written with %s fields: %s",
            len(metadata),
            metadata_path,
        )

        return metadata_path

    @staticmethod
    def _find_header_index(lines: list[str]) -> int:
        """Localiza a linha que contém o cabeçalho tabular do INMET."""

        for index, line in enumerate(lines):
            normalized_line = line.strip().lstrip("\ufeff")

            if normalized_line.startswith("Data Medicao"):
                return index

        raise ValueError(
            "INMET header was not found. Expected a line starting "
            "with 'Data Medicao'."
        )

    def _prepare_data_lines(
        self,
        lines: list[str],
        header_index: int,
    ) -> list[str]:
        """Normaliza o cabeçalho e preserva as linhas de medições."""

        data_lines = [
            line.rstrip()
            for line in lines[header_index:]
            if line.strip()
        ]

        if not data_lines:
            raise ValueError(
                "No tabular data was found after the INMET header."
            )

        original_columns = [
            column.strip()
            for column in data_lines[0].split(";")
        ]

        normalized_columns = [
            self.INMET_COLUMN_MAPPING.get(column, column)
            for column in original_columns
        ]

        duplicated_columns = self._find_duplicates(normalized_columns)

        if duplicated_columns:
            raise ValueError(
                "Duplicated columns found after normalization: "
                f"{duplicated_columns}"
            )

        normalized_header = ";".join(normalized_columns)

        return [normalized_header, *data_lines[1:]]

    @staticmethod
    def _resolve_cleaned_path(
        source_name: str,
        source_path: Path,
        source_payload: dict[str, Any],
    ) -> Path:
        """
        Resolve o caminho do CSV limpo.

        Dá preferência ao ``path_raw_cleaned`` definido no payload.
        Caso ele não exista, gera um nome automaticamente.
        """

        configured_path = source_payload.get("path_raw_cleaned")

        if configured_path:
            cleaned_path = Path(configured_path).expanduser()

            if not cleaned_path.is_absolute():
                cleaned_path = (PROJECT_ROOT / cleaned_path).resolve()
            else:
                cleaned_path = cleaned_path.resolve()
        else:
            cleaned_path = (
                RAW_DIR
                / f"{source_path.stem}_{source_name}_cleaned.csv"
            )

        cleaned_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        return cleaned_path

    @staticmethod
    def _write_cleaned_file(
        data_lines: list[str],
        cleaned_path: Path,
    ) -> None:
        """Grava o conteúdo tabular normalizado em CSV."""

        cleaned_content = "\n".join(data_lines).rstrip() + "\n"

        cleaned_path.write_text(
            cleaned_content,
            encoding="utf-8",
        )

        logger.info("Cleaned INMET file written to: %s", cleaned_path)

    @staticmethod
    def _find_duplicates(columns: list[str]) -> list[str]:
        """Retorna os nomes de colunas duplicados."""

        seen: set[str] = set()
        duplicated: set[str] = set()

        for column in columns:
            if column in seen:
                duplicated.add(column)

            seen.add(column)

        return sorted(duplicated)
