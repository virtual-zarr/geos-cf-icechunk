import numpy as np
import pandas as pd
import pytest
import xarray as xr
from virtualizarr.manifests import ChunkManifest, ManifestArray
from virtualizarr.manifests.utils import create_v3_array_metadata
from virtualizarr_processor.processor import Processor


def _virtual_variable(
    dims: tuple[str, ...],
    shape: tuple[int, ...],
    dtype: str,
    attrs: dict | None = None,
) -> xr.Variable:
    """Build a single-chunk ManifestArray-backed (virtual) variable."""
    metadata = create_v3_array_metadata(
        shape=shape, chunk_shape=shape, data_type=np.dtype(dtype)
    )
    chunk_key = ".".join("0" for _ in shape)
    manifest = ChunkManifest(
        entries={
            chunk_key: {"path": "s3://bucket/file.nc", "offset": 0, "length": 1000}
        }
    )
    marr = ManifestArray(metadata=metadata, chunkmanifest=manifest)
    return xr.Variable(dims, marr, attrs=attrs or {})


@pytest.fixture
def processor() -> Processor:
    return Processor()


@pytest.fixture
def virtual_dataset() -> xr.Dataset:
    """A valid GEOS-CF-shaped virtual dataset."""
    return xr.Dataset(
        data_vars={
            "foo": _virtual_variable(
                dims=("time", "lev", "lat", "lon"),
                shape=(1, 1, 721, 1440),
                dtype="float32",
            )
        },
        coords={
            "time": pd.date_range("2024-01-01", periods=1, freq="1h"),
            "lev": [1000.0],
            "lat": np.linspace(-90.0, 90.0, 721),
            "lon": np.linspace(0.0, 359.75, 1440),
        },
    )


def test_validate_dataset_accepts_expected_shape(
    processor: Processor, virtual_dataset: xr.Dataset
) -> None:
    assert processor.validate_dataset(virtual_dataset, ["foo"])


def test_validate_dataset_accepts_empty_dataset(
    processor: Processor, virtual_dataset: xr.Dataset
) -> None:
    empty = virtual_dataset.drop_vars("foo")

    assert processor.validate_dataset(empty, [])


def test_validate_dataset_rejects_missing_coordinate(
    processor: Processor, virtual_dataset: xr.Dataset
) -> None:
    invalid = virtual_dataset.drop_vars("lev")

    assert not processor.validate_dataset(invalid, ["foo"])


def test_validate_dataset_rejects_wrong_variable_dims(
    processor: Processor,
    virtual_dataset: xr.Dataset,
) -> None:
    invalid = virtual_dataset.assign(
        foo=_virtual_variable(
            dims=("time", "lat", "lon"),
            shape=(1, 721, 1440),
            dtype="float32",
            attrs=virtual_dataset["foo"].attrs,
        )
    )

    assert not processor.validate_dataset(invalid, ["foo"])


def test_validate_dataset_rejects_wrong_coordinate_sizes(
    processor: Processor,
) -> None:
    invalid = xr.Dataset(
        data_vars={
            "foo": _virtual_variable(
                dims=("time", "lev", "lat", "lon"),
                shape=(1, 1, 721, 1439),
                dtype="float32",
            )
        },
        coords={
            "time": pd.date_range("2024-01-01", periods=1, freq="1h"),
            "lev": [1000.0],
            "lat": np.linspace(-90.0, 90.0, 721),
            "lon": np.linspace(0.0, 359.75, 1439),
        },
    )

    assert not processor.validate_dataset(invalid, ["foo"])


def test_validate_dataset_rejects_wrong_data_dtype(
    processor: Processor,
    virtual_dataset: xr.Dataset,
) -> None:
    invalid = virtual_dataset.assign(
        foo=_virtual_variable(
            dims=("time", "lev", "lat", "lon"),
            shape=(1, 1, 721, 1440),
            dtype="float64",
        )
    )

    assert not processor.validate_dataset(invalid, ["foo"])


def test_process_file_rejects_invalid_dataset(
    processor: Processor, monkeypatch: pytest.MonkeyPatch
) -> None:
    invalid_ds = xr.Dataset(coords={"time": pd.date_range("2024-01-01", periods=1)})

    monkeypatch.setattr(
        "virtualizarr_processor.processor.build_vds",
        lambda file_keys: invalid_ds,
    )
    monkeypatch.setattr(processor, "initialize_store", lambda: object())

    try:
        processor.process_file(["example.nc4"])
    except ValueError as exc:
        assert str(exc) == "Dataset validation failed for processed dataset"
    else:
        raise AssertionError("Expected process_file to reject invalid dataset")