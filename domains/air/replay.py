from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class AirQualityMetadata:
    latitude: float
    longitude: float
    elevation: float
    utc_offset_seconds: int
    timezone: str
    timezone_abbreviation: str


@dataclass(frozen=True)
class AirQualitySample:
    time: str
    pm10: float
    pm2_5: float

    def epoch_timestamp(self, utc_offset_seconds: int) -> float:
        local_tz = timezone(timedelta(seconds=utc_offset_seconds))
        dt = datetime.fromisoformat(self.time).replace(tzinfo=local_tz)
        return dt.timestamp()


class AirQualityDataset:
    def __init__(
        self,
        metadata: AirQualityMetadata,
        samples: list[AirQualitySample],
        source_path: str | Path,
    ) -> None:
        if not samples:
            raise ValueError("El dataset de aire no contiene muestras")

        self.metadata = metadata
        self.samples = samples
        self.source_path = Path(source_path)

    @classmethod
    def from_csv(cls, path: str | Path) -> "AirQualityDataset":
        source_path = Path(path)

        with source_path.open(
            "r",
            encoding="utf-8-sig",
            newline=""
        ) as fh:
            lines = fh.readlines()

        if len(lines) < 5:
            raise ValueError(
                f"CSV Open-Meteo incompleto: {source_path}"
            )

        # Las primeras dos líneas contienen los metadatos de Open-Meteo.
        metadata_reader = csv.DictReader(lines[:2])
        metadata_row = next(metadata_reader, None)

        if metadata_row is None:
            raise ValueError(
                f"No se pudo leer metadata de {source_path}"
            )

        metadata = AirQualityMetadata(
            latitude=float(metadata_row["latitude"]),
            longitude=float(metadata_row["longitude"]),
            elevation=float(metadata_row["elevation"]),
            utc_offset_seconds=int(
                metadata_row["utc_offset_seconds"]
            ),
            timezone=str(metadata_row["timezone"]),
            timezone_abbreviation=str(
                metadata_row["timezone_abbreviation"]
            ),
        )

        # Buscar dónde comienza realmente la tabla horaria.
        data_header_index = next(
            (
                i
                for i, line in enumerate(lines)
                if line.strip().startswith("time,")
            ),
            None,
        )

        if data_header_index is None:
            raise ValueError(
                f"No se encontró la tabla horaria en {source_path}"
            )

        reader = csv.DictReader(lines[data_header_index:])

        if reader.fieldnames is None:
            raise ValueError(
                f"No se encontró cabecera horaria en {source_path}"
            )

        # Los CSV descargados incluyen la unidad en el nombre.
        pm10_column = next(
            (
                name
                for name in reader.fieldnames
                if name.strip().startswith("pm10")
            ),
            None,
        )

        pm25_column = next(
            (
                name
                for name in reader.fieldnames
                if name.strip().startswith("pm2_5")
            ),
            None,
        )

        if pm10_column is None or pm25_column is None:
            raise ValueError(
                "El CSV debe contener columnas pm10 y pm2_5"
            )

        samples: list[AirQualitySample] = []

        for row_number, row in enumerate(reader, start=1):
            time_value = (row.get("time") or "").strip()
            pm10_value = (row.get(pm10_column) or "").strip()
            pm25_value = (row.get(pm25_column) or "").strip()

            if not time_value or not pm10_value or not pm25_value:
                raise ValueError(
                    f"Muestra incompleta en {source_path}, "
                    f"fila de datos {row_number}"
                )

            samples.append(
                AirQualitySample(
                    time=time_value,
                    pm10=float(pm10_value),
                    pm2_5=float(pm25_value),
                )
            )

        return cls(
            metadata=metadata,
            samples=samples,
            source_path=source_path,
        )

    def __len__(self) -> int:
        return len(self.samples)

    def get(self, index: int) -> AirQualitySample:
        return self.samples[index]


class AirQualityReplay:
    """Replay determinista, en orden cronológico, de una serie cacheada."""

    def __init__(
        self,
        dataset: AirQualityDataset,
        loop: bool = False,
    ) -> None:
        self.dataset = dataset
        self.loop = loop
        self.index = 0

    def has_next(self) -> bool:
        return self.loop or self.index < len(self.dataset)

    def next_sample(self) -> AirQualitySample:
        if self.index >= len(self.dataset):
            if not self.loop:
                raise StopIteration(
                    "Fin del dataset de calidad del aire"
                )

            self.index = 0

        sample = self.dataset.get(self.index)
        self.index += 1

        return sample

    def reset(self) -> None:
        self.index = 0