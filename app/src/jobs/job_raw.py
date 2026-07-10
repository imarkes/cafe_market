
import json
from pathlib import Path

from pipeline.manager import DataManager

class JobRawData:

    def __init__(self, payload: dict, run: DataManager) -> None:
        self.payload = payload
        self.run = run

    def prepare_file_csv_inmet(self, source_name: str) -> dict[str, str]:
        """Prepara um CSV INMET extraindo metadados e escrevendo uma cópia limpa com colunas padronizadas.

        A função lê o arquivo bruto, salva as informações de cabeçalho como metadados em JSON,
        identifica a linha do cabeçalho, renomeia as colunas para o esquema padronizado e grava
        um novo CSV limpo na pasta de arquivos brutos.
        """
        source_payload = self.payload.get(source_name)
        if not isinstance(source_payload, dict):
            return {}

        path = source_payload.get("path_raw")
        if not path:
            return {}

        source_path = Path(path)
        if not source_path.exists():
            return {}

        lines = source_path.read_text(encoding="utf-8").splitlines()

        metadata: dict[str, str] = {}
        for line in lines:
            stripped = line.strip()
            if ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            if key and value.strip():
                metadata[key.strip()] = value.strip()

        meta_dir = Path("../storage/meta") / source_name
        meta_dir.mkdir(parents=True, exist_ok=True)

        metadata_path = meta_dir / "metadata.json"
        metadata_path.write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        header_index = next(
            (
                index
                for index, line in enumerate(lines)
                if line.startswith("Data Medicao")
            ),
            None,
        )
        if header_index is None:
            header_index = 0

        data_lines = [line.rstrip() for line in lines[header_index:]]
        if not data_lines:
            return {
                "metadata_path": str(metadata_path),
                "cleaned_path": "",
            }

        columns = [column.strip() for column in data_lines[0].split(";")]
        renamed_columns = [
            {
                "Data Medicao": "data_medicao",
                "PRECIPITACAO TOTAL, DIARIO (AUT)(mm)": "precipitacao_total_mm",
                "TEMPERATURA MAXIMA, DIARIA (AUT)(°C)": "temp_max_c",
                "TEMPERATURA MEDIA, DIARIA (AUT)(°C)": "temp_media_c",
                "TEMPERATURA MINIMA, DIARIA (AUT)(°C)": "temp_min_c",
                "UMIDADE RELATIVA DO AR, MEDIA DIARIA (AUT)(%)": "umidade_rel_media",
                "UMIDADE RELATIVA DO AR, MINIMA DIARIA (AUT)(%)": "umidade_rel_minima",
            }.get(column, column)
            for column in columns
        ]

        cleaned_lines = [";".join(renamed_columns)] + data_lines[1:]
        cleaned_content = "\n".join(cleaned_lines).strip() + "\n"

        raw_dir = Path("../storage/raw")
        raw_dir.mkdir(parents=True, exist_ok=True)
        cleaned_path = raw_dir / f"{source_path.stem}_{source_name}_cleaned.csv"
        cleaned_path.write_text(cleaned_content, encoding="utf-8")

        source_payload["path_raw_cleaned"] = str(cleaned_path)

        return {
            "metadata_path": str(metadata_path),
            "cleaned_path": str(cleaned_path),
        }

if __name__ == "__main__":
    ...
