"""In-memory time series storage"""

from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from loguru import logger
from infrasys.arrow_storage import ArrowTimeSeriesStorage

from infrasys.exceptions import ISNotStored
from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadataBase,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase


class InMemoryTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in memory."""

    def __init__(self) -> None:
        self._arrays: dict[int, TimeSeriesData] = {}  # Time series ID, not metadata ID

    def get_time_series_directory(self) -> None:
        return None

    def add_time_series(self, metadata: TimeSeriesMetadata, time_series: TimeSeriesData) -> None:
        if metadata.time_series_id not in self._arrays:
            self._arrays[metadata.time_series_id] = time_series
            logger.debug("Added {} to store", time_series.summary)
        else:
            logger.debug("{} was already stored", time_series.summary)

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> TimeSeriesData:
        time_series = self._arrays.get(metadata.time_series_id)
        if time_series is None:
            msg = f"No time series with {metadata.time_series_id} is stored"
            raise ISNotStored(msg)

        if isinstance(metadata, SingleTimeSeriesMetadataBase):
            return self._get_single_time_series(metadata, start_time=start_time, length=length)
        raise NotImplementedError(str(metadata.get_time_series_data_type()))

    def remove_time_series(self, time_series_id: int) -> None:
        time_series = self._arrays.pop(time_series_id, None)
        if time_series is None:
            msg = f"No time series with {time_series_id} is stored"
            raise ISNotStored(msg)

    def serialize(
        self, data: dict[str, Any], dst: Path | str, _: Optional[Path | str] = None
    ) -> None:
        base_directory = dst if isinstance(dst, Path) else Path(dst)
        storage = ArrowTimeSeriesStorage.create_with_permanent_directory(base_directory)
        for ts in self._arrays.values():
            metadata_type = ts.get_time_series_metadata_type()
            metadata = metadata_type.from_data(ts)
            storage.add_time_series(metadata, ts)
        data["time_series_storage_type"] = "ArrowTimeSeriesStorage"

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadataBase,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> SingleTimeSeries:
        base_ts = self._arrays[metadata.time_series_id]
        assert isinstance(base_ts, SingleTimeSeries)
        if start_time is None and length is None:
            return base_ts

        index, length = metadata.get_range(start_time=start_time, length=length)
        return SingleTimeSeries(
            id=metadata.time_series_id,
            variable_name=base_ts.variable_name,
            resolution=base_ts.resolution,
            initial_time=start_time or base_ts.initial_time,
            data=base_ts.data[index : index + length],
            normalization=metadata.normalization,
        )
