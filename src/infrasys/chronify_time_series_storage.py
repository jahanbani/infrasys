"""Implementation of arrow storage for time series."""

import atexit
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Optional, Self

import pandas as pd
import pint
from chronify import DatetimeRange, Store, TableSchema
from loguru import logger

from infrasys.time_series_models import (
    SingleTimeSeries,
    SingleTimeSeriesMetadata,
    TimeSeriesData,
    TimeSeriesMetadata,
)
from infrasys.time_series_storage_base import TimeSeriesStorageBase


class ChronifyTimeSeriesStorage(TimeSeriesStorageBase):
    """Stores time series in disk"""

    TABLE_NAME = "time_series"

    def __init__(self, store: Store) -> None:
        self._store = store

    @classmethod
    def create_with_temp_directory(
        cls, base_directory: Optional[Path] = None, engine_name: str = "duckdb"
    ) -> Self:
        """Construct ChronifyTimeSeriesStorage with a temporary directory."""
        with NamedTemporaryFile(dir=base_directory, suffix=".db") as f:
            dst_file = Path(f.name)
            f.close()
        logger.debug("Creating database at {}", dst_file)
        atexit.register(_delete_tmp_file, dst_file)
        store = Store(engine_name=engine_name, file_path=dst_file)
        return cls(store)

    @classmethod
    def from_file_to_tmp_file(
        cls, src_file: Path, dst_dir: Optional[Path] = None, engine_name: str = "duckdb"
    ) -> Self:
        """Construct ChronifyTimeSeriesStorage after copying from an existing database file."""
        with NamedTemporaryFile(dir=dst_dir, suffix=".db") as f:
            dst_file = Path(f.name)
            f.close()
        orig_store = Store(engine_name=engine_name, file_path=src_file)
        orig_store.backup(dst_file)
        new_store = Store(engine_name=engine_name, file_path=dst_file)
        atexit.register(_delete_tmp_file, dst_file)
        return cls(new_store)

    @classmethod
    def from_file(cls, file_path: Path, engine_name: str = "duckdb") -> Self:
        """Construct ChronifyTimeSeriesStorage with an existing database file."""
        store = Store(engine_name=engine_name, file_path=file_path)
        return cls(store)

    def get_database_url(self) -> str:
        """Return the path to the underlying database."""
        assert self._store.engine.url.database is not None
        # We don't expect to use an in-memory db.
        return self._store.engine.url.database

    def get_time_series_directory(self) -> Path:
        assert self._store.engine.url.database is not None
        return Path(self._store.engine.url.database).parent

    def add_time_series(
        self,
        metadata: TimeSeriesMetadata,
        time_series: TimeSeriesData,
    ) -> None:
        if not isinstance(time_series, SingleTimeSeries):
            msg = f"Bug: need to implement add_time_series for {type(time_series)}"
            raise NotImplementedError(msg)

        df = self._to_dataframe(time_series)
        schema = TableSchema(
            name=self.TABLE_NAME,
            value_column="value",
            time_array_id_columns=["id"],
            time_config=DatetimeRange(
                start=time_series.initial_time,
                resolution=time_series.resolution,
                length=len(time_series.data),
                time_column="timestamp",
            ),
        )
        self._store.ingest_table(df, schema)
        logger.debug("Added {} to time series storage", time_series.summary)

    def get_engine_name(self) -> str:
        """Return the name of the underlying database engine."""
        return self._store.engine.name

    def get_time_series(
        self,
        metadata: TimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> Any:
        if isinstance(metadata, SingleTimeSeriesMetadata):
            return self._get_single_time_series(
                metadata=metadata, start_time=start_time, length=length
            )

        msg = f"Bug: need to implement get_time_series for {type(metadata)}"
        raise NotImplementedError(msg)

    def remove_time_series(self, time_series_id: int) -> None:
        self._store.delete_rows(self.TABLE_NAME, {"id": time_series_id})

    def serialize(
        self, data: dict[str, Any], dst: Path | str, src: Optional[Path | str] = None
    ) -> None:
        with NamedTemporaryFile(dir=dst, suffix=".db") as f:
            path = Path(f.name)
            f.close()
        self._store.backup(path)
        data["filename"] = str(path)
        data["time_series_storage_type"] = "ChronifyTimeSeriesStorage"
        data["engine_name"] = self._store.engine.name

    def _get_single_time_series(
        self,
        metadata: SingleTimeSeriesMetadata,
        start_time: datetime | None = None,
        length: int | None = None,
    ) -> SingleTimeSeries:
        _, required_len = metadata.get_range(start_time=start_time, length=length)
        where_clauses = ["id = ?"]
        params: list[Any] = [metadata.time_series_id]
        if start_time is not None:
            where_clauses.append("timestamp >= ?")
            params.append(start_time)
        where_clause = " AND ".join(where_clauses)
        limit = "" if length is None else f" LIMIT {required_len}"
        query = f"""
            SELECT value
            FROM {self.TABLE_NAME}
            WHERE {where_clause}
            ORDER BY timestamp
            {limit}
        """
        df = self._store.read_raw_query(query, params=params)
        if len(df) != required_len:
            msg = f"Bug: {len(df)=} {length=} {required_len=}"
            raise Exception(msg)
        values = df["value"].values
        if metadata.quantity_metadata is not None:
            np_array = metadata.quantity_metadata.quantity_type(
                values, metadata.quantity_metadata.units
            )
        else:
            np_array = values
        return SingleTimeSeries(
            id=metadata.time_series_id,
            variable_name=metadata.variable_name,
            resolution=metadata.resolution,
            initial_time=start_time or metadata.initial_time,
            data=np_array,
            normalization=metadata.normalization,
        )

    def _to_dataframe(self, time_series: SingleTimeSeries) -> pd.DataFrame:
        """Create record batch to save array to disk."""
        if isinstance(time_series.data, pint.Quantity):
            array = time_series.data.magnitude
        else:
            array = time_series.data
        df = pd.DataFrame({"timestamp": time_series.make_timestamps(), "value": array})
        df["id"] = time_series.id
        return df


def _delete_tmp_file(path: Path) -> None:
    path.unlink()
    logger.info("Deleted time series file: {}", path)
