from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import requests


ENDPOINT = (
    "https://air-quality-api.open-meteo.com/"
    "v1/air-quality"
)

COMMUNES = {
    "santiago": (-33.5, -70.6),
    "las_condes": (-33.399998, -70.6),
    "quilicura": (-33.399998, -70.7),
}

START_DATE = "2026-07-01"
END_DATE = "2026-07-31"
TIMEZONE = "America/Santiago"


def download_air_quality(
    latitude: float,
    longitude: float,
    output_path: str | Path,
) -> Path:

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "pm2_5,pm10",
        "start_date": START_DATE,
        "end_date": END_DATE,
        "timezone": TIMEZONE,
    }

    response = requests.get(
        ENDPOINT,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    data: dict[str, Any] = response.json()

    hourly = data.get("hourly", {})

    times = hourly.get("time", [])
    pm25 = hourly.get("pm2_5", [])
    pm10 = hourly.get("pm10", [])

    if not times or not (
        len(times) == len(pm25) == len(pm10)
    ):
        raise ValueError(
            "Open-Meteo devolvió una serie incompleta"
        )

    path = Path(output_path)
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as fh:

        writer = csv.writer(fh)

        # Metadata de la descarga.
        writer.writerow(
            [
                "latitude",
                "longitude",
                "elevation",
                "utc_offset_seconds",
                "timezone",
                "timezone_abbreviation",
            ]
        )

        writer.writerow(
            [
                data.get("latitude", latitude),
                data.get("longitude", longitude),
                data.get("elevation", 0.0),
                data.get("utc_offset_seconds", 0),
                data.get("timezone", TIMEZONE),
                data.get(
                    "timezone_abbreviation",
                    "",
                ),
            ]
        )

        writer.writerow([])

        # Serie horaria.
        writer.writerow(
            [
                "time",
                "pm10 (μg/m³)",
                "pm2_5 (μg/m³)",
            ]
        )

        for (
            sample_time,
            pm10_value,
            pm25_value,
        ) in zip(times, pm10, pm25):

            if (
                pm10_value is None
                or pm25_value is None
            ):
                raise ValueError(
                    "Open-Meteo devolvió "
                    f"un dato faltante en {sample_time}"
                )

            writer.writerow(
                [
                    sample_time,
                    pm10_value,
                    pm25_value,
                ]
            )

    return path


def main() -> int:

    for (
        name,
        (latitude, longitude),
    ) in COMMUNES.items():

        output = (
            f"data/air_quality/{name}.csv"
        )

        download_air_quality(
            latitude,
            longitude,
            output,
        )

        print(
            f"[air-download] "
            f"{name} -> {output}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())