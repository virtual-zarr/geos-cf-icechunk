import os
import warnings
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from itertools import islice
from typing import Any

import icechunk
import numpy as np
import pandas as pd
import pandera as pa
import xarray as xr
from icechunk import Repository
from obstore.store import from_url
from pandera.xarray import Coordinate, DatasetSchema, DataVar
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
from virtualizarr import open_virtual_mfdataset
from virtualizarr.manifests import ChunkManifest, ManifestArray
from virtualizarr.parsers import HDFParser
from virtualizarr.registry import ObjectStoreRegistry
from zarr.core.metadata import ArrayV3Metadata

BASE_URL = "https://ds.nccs.nasa.gov"
THREDDS_URL = f"{BASE_URL}/thredds/fileServer"
URL_PREFIX = f"{THREDDS_URL}/"
ICECHUNK_COMMIT_RETRIES = int(os.getenv("ICECHUNK_COMMIT_RETRIES", "10"))
DATASET_ATTRS_TO_DROP = [
    "History",
    "Source",
    "ProductionDateTime",
    "Comment",
    "Filename",
    "GranuleID",
    "TemporalRange",
    "RangeBeginningDate",
    "RangeBeginningTime",
    "RangeEndingDate",
    "RangeEndingTime",
]

# Production initialization key
PROD_INIT_KEY = "GMAO/GEOS-CF/analysis-v2/Y2025/M08/D04/GEOS.cf.ana.aqc_tavg_1hr_glo_L1440x721_slv.20250804_0930z.nc4"  # noqa: E501
PROD_START_DT = datetime(2025, 8, 4, 9, 30, tzinfo=UTC)
PROD_END_DT = datetime(2026, 4, 9, 20, 30, tzinfo=UTC)
ONE_YEAR_HOURLY = 24 * 365
ICECHUNK_BRANCH = "main"


warnings.filterwarnings(
    "ignore",
    message="Numcodecs codecs are not in the Zarr version 3 specification*",
    category=UserWarning,
)


# Expected structure of a GEOS-CF virtual dataset, validated before writing to
# icechunk. Checks are kept at the schema level (no data-value assertions) so
# that validation never attempts to load virtual chunk data
EXPECTED_DATASET_DIMS = ("time", "lev", "lat", "lon")
EXPECTED_DATA_DTYPE = np.float32
EXPECTED_LEV_SIZE = 1
EXPECTED_LAT_SIZE = 721
EXPECTED_LON_SIZE = 1440
# The full set of data variables a valid GEOS-CF dataset should contain
EXPECTED_DATA_VARS = ("CO", "NO2", "O3", "PM10_RH35", "PM25_RH35", "SO2")


def _build_geos_cf_dataset_schema(data_var_names: list[str]) -> DatasetSchema:
    """Build the declarative pandera schema for a GEOS-CF virtual dataset."""
    data_vars = {
        name: DataVar(
            dtype=EXPECTED_DATA_DTYPE,
            dims=EXPECTED_DATASET_DIMS,
            sizes={
                "time": None,
                "lev": EXPECTED_LEV_SIZE,
                "lat": EXPECTED_LAT_SIZE,
                "lon": EXPECTED_LON_SIZE,
            },
            array_type=ManifestArray,
        )
        for name in data_var_names
    }
    coords = {
        "time": Coordinate(dtype="datetime64[ns]", dims=("time",)),
        "lev": Coordinate(dtype=np.float64, dims=("lev",)),
        "lat": Coordinate(dtype=np.float64, dims=("lat",)),
        "lon": Coordinate(dtype=np.float64, dims=("lon",)),
    }
    return DatasetSchema(
        data_vars=data_vars,
        coords=coords,
        strict=True,
        strict_coords=True,
    )


def _build_vds_for_keys(keys: list[str]) -> xr.Dataset:
    normalized_keys = [key.strip() for key in keys if key.strip()]
    source_urls: list[str] = []
    for normalized_key in normalized_keys:
        if normalized_key.startswith(("http://", "https://")):
            source_urls.append(normalized_key)
        else:
            source_urls.append(f"{THREDDS_URL}/{normalized_key.lstrip('/')}")

    # store = AiohttpStore(BASE_URL)
    store = from_url(BASE_URL)
    registry = ObjectStoreRegistry({THREDDS_URL: store})
    parser = HDFParser()
    vds = open_virtual_mfdataset(
        urls=source_urls,
        parser=parser,
        registry=registry,
        parallel=ThreadPoolExecutor,
    )
    vds.attrs.clear()
    return vds


def build_vds(keys: list[str]) -> xr.Dataset:
    normalized_keys = [key.strip() for key in keys if key.strip()]
    if not normalized_keys:
        raise ValueError("No valid keys provided to build_vds")

    return _build_vds_for_keys(normalized_keys)


def build_empty_vds(
    start_date: datetime, end_date: datetime, ref_key: str
) -> xr.Dataset:

    ref_ds = build_vds([ref_key])
    naive_start = (
        start_date.replace(tzinfo=None) if start_date.tzinfo is not None else start_date
    )
    naive_end = (
        end_date.replace(tzinfo=None) if end_date.tzinfo is not None else end_date
    )
    full_time = pd.date_range(start=naive_start, end=naive_end, freq="1h")
    target_shape = (len(full_time), 1, 721, 1440)

    # scaffold blank dataset
    blank_ds = xr.Dataset(
        coords={
            "time": full_time,
            "lev": ref_ds["lev"],
            "lat": ref_ds["lat"],
            "lon": ref_ds["lon"],
        },
        attrs=ref_ds.attrs,
    )

    for var_name in ref_ds.data_vars:
        donor_arr = ref_ds[var_name].data

        meta_dict = donor_arr.metadata.to_dict()
        meta_dict["shape"] = target_shape
        new_metadata = ArrayV3Metadata.from_dict(meta_dict)

        time_axis = ref_ds[var_name].dims.index("time")
        chunk_grid_shape = tuple(
            len(full_time) if i == time_axis else s
            for i, s in enumerate(donor_arr.manifest.shape_chunk_grid)
        )
        empty_manifest = ChunkManifest({}, shape=chunk_grid_shape)

        blank_ds[var_name] = xr.Variable(
            dims=["time", "lev", "lat", "lon"],
            data=ManifestArray(chunkmanifest=empty_manifest, metadata=new_metadata),
            attrs=ref_ds[var_name].attrs,
        )

    return blank_ds


class Processor:
    bucket: str | None
    prefix: str
    init_key: str | None
    init_dt_range: tuple[datetime, datetime] | None
    region: str | None

    def __init__(
        self,
        bucket: str | None = None,
        prefix: str | None = None,
        init_key: str | None = None,
        init_dt_range: tuple[datetime, datetime] | None = None,
        region: str | None = None,
    ) -> None:
        self.bucket = bucket or os.getenv("ICECHUNK_BUCKET")
        resolved_prefix = (
            prefix if prefix is not None else os.getenv("ICECHUNK_PREFIX", "")
        )
        self.prefix = resolved_prefix.strip("/")
        self.init_key = init_key or os.getenv("ICECHUNK_INIT_KEY") or PROD_INIT_KEY
        self.init_dt_range = init_dt_range
        self.region = region or os.getenv("AWS_REGION")

    def _get_storage(self) -> icechunk.Storage:
        if not self.bucket:
            raise ValueError("ICECHUNK_BUCKET environment variable is not set")

        return icechunk.s3_storage(
            bucket=self.bucket,
            prefix=self.prefix or None,
            region=self.region,
            from_env=True,
        )

    def _get_config(
        self, split_config: icechunk.ManifestSplittingConfig | None = None
    ) -> icechunk.RepositoryConfig:
        cache_config = icechunk.CachingConfig(
            num_snapshot_nodes=200,
            num_chunk_refs=2_000_000,
            num_transaction_changes=0,
            num_bytes_attributes=0,
            num_bytes_chunks=0,
        )
        manifest = (
            icechunk.ManifestConfig(splitting=split_config) if split_config else None
        )
        kwargs: dict[str, Any] = {"caching": cache_config}
        if manifest:
            kwargs["manifest"] = manifest
        config = icechunk.RepositoryConfig(**kwargs)
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(
                url_prefix=URL_PREFIX, store=icechunk.http_store()
            )
        )
        return config

    def initialize_store(self) -> Repository:
        if not self.init_key:
            raise ValueError("Processor init_key must be provided to initialize_store")
        if not self.init_dt_range:
            raise ValueError(
                "Processor init_dt_range must be provided to initialize_store"
            )
        storage = self._get_storage()

        split_config = icechunk.ManifestSplittingConfig.from_dict(
            {
                # Apply this to all arrays in the repository
                icechunk.ManifestSplitCondition.AnyArray(): {
                    # Split along the 'time' dimension every 8,760 chunks
                    icechunk.ManifestSplitDimCondition.DimensionName(
                        "time"
                    ): ONE_YEAR_HOURLY
                }
            }
        )
        config = self._get_config(split_config=split_config)
        repo = icechunk.Repository.open_or_create(
            storage=storage,
            config=config,
            authorize_virtual_chunk_access={URL_PREFIX: None},
        )
        history = list(islice(repo.ancestry(branch=ICECHUNK_BRANCH), 2))
        snapshots = list(history)
        if len(snapshots) == 1:
            session = repo.writable_session(ICECHUNK_BRANCH)
            start, end = self.init_dt_range
            vds = build_empty_vds(start, end, self.init_key)
            vds.vz.to_icechunk(session.store, validate_containers=False)
            try:
                _ = session.commit(message="Initialization")
                repo.save_config()
            except icechunk.ConflictError:
                pass
        return repo

    @retry(
        reraise=True,
        stop=stop_after_attempt(ICECHUNK_COMMIT_RETRIES),
        wait=wait_random_exponential(1, 10),
    )
    def commit_with_retry(
        self,
        repo: icechunk.Repository,
        vds: xr.Dataset,
        message: str,
        write_kwargs: dict[str, Any] | None = None,
    ) -> str:
        session = repo.writable_session(ICECHUNK_BRANCH)
        vds.vz.to_icechunk(
            session.store,
            **(write_kwargs or {}),
            validate_containers=False,
        )
        return str(
            session.commit(
                message=message,
                rebase_with=icechunk.ConflictDetector(),
            )
        )

    @classmethod
    def validate_dataset(
        cls,
        dataset: xr.Dataset,
        data_vars: Iterable[str],
    ) -> bool:
        """Validate a virtual dataset against the GEOS-CF schema.

        Validation is schema-level only, so it never loads virtual chunk data.

        Args:
            dataset (xr.Dataset): dataset to validate
            data_vars (Iterable[str]): data variables to validate

        Returns:
            bool: True if the dataset conforms to the schema, False otherwise
        """


        schema = _build_geos_cf_dataset_schema(data_vars)
        try:
            schema.validate(dataset, lazy=True)
        except (pa.errors.SchemaError, pa.errors.SchemaErrors):
            return False
        return True

    def process_file(self, file_keys: list[str], overwrite: bool = False) -> str:
        repo = self.initialize_store()
        vds = build_vds(file_keys)
        vds.attrs.clear()

        if not self.validate_dataset(vds, EXPECTED_DATA_VARS):
            raise ValueError("Dataset validation failed for processed dataset")

        keys_str = ", ".join([key.split("/")[-1] for key in file_keys])

        if overwrite:
            write_kwargs = {"region": "auto"}
            message = f"Overwrite {keys_str}"
        else:
            write_kwargs = {"append_dim": "time"}
            message = f"Append {keys_str}"

        return str(
            self.commit_with_retry(
                repo,
                vds,
                message=message,
                write_kwargs=write_kwargs,
            )
        )

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        repo = self.initialize_store()
        _ = repo.expire_snapshots(older_than=expiry_time)
        return repo.garbage_collect(delete_object_older_than=expiry_time)

    def search_state(
        self,
        start_dt: datetime | None = None,
        end_dt: datetime | None = None,
    ) -> tuple[datetime | None, set[datetime]]:
        """Return latest time and stored times in a single repository read."""
        try:
            repo = icechunk.Repository.open(
                storage=self._get_storage(),
                config=self._get_config(),
            )
            session = repo.readonly_session(branch=ICECHUNK_BRANCH)
            ds = xr.open_zarr(session.store)
            try:
                if "time" not in ds.coords:
                    return None, set()

                time_index = pd.DatetimeIndex(ds["time"].values)
                if len(time_index) == 0:
                    return None, set()

                if time_index.tz is None:
                    time_index = time_index.tz_localize(UTC)
                else:
                    time_index = time_index.tz_convert(UTC)

                # Latest time
                latest_ts = time_index.max()
                latest_dt: datetime | None = None
                if not pd.isna(latest_ts):
                    latest_dt = latest_ts.to_pydatetime()

                # Stored times within range
                filtered = time_index
                if start_dt is not None:
                    filtered = filtered[filtered >= pd.Timestamp(start_dt)]
                if end_dt is not None:
                    filtered = filtered[filtered <= pd.Timestamp(end_dt)]

                stored = {ts.to_pydatetime() for ts in filtered}
                return latest_dt, stored
            finally:
                ds.close()
        except Exception:
            return None, set()
