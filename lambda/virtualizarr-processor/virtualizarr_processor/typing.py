from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

import icechunk
import xarray as xr
from icechunk import Repository


@runtime_checkable
class VirtualizarrProcessor(Protocol):
    def initialize_store(self) -> Repository:
        """
        Initialize an IcechunkStore with the necessary structure and return
        a Repository handle.

        This store should have a dimension that can be used append function.

        Parameters
        ----------

        Returns
        -------
        Repository
            The initialized repository.
        """
        ...

    def process_file(self, file_keys: list[str]) -> str:
        """
        Uses a Virtualizarr parser to parse the file, manipulate the resulting
        ManifestStore and add it to the Icechunk store

        Parameters
        ----------
            file_keys: Full key path(s) to source files.
        Returns
        -------
        str
            A snapshot id of the append commit.
        """
        ...

    @classmethod
    def validate_dataset(cls, dataset: xr.Dataset) -> bool:
        """
        Validate a parsed xarray Dataset before writing it to Icechunk.

        Parameters
        ----------
            dataset: The parsed xarray Dataset.
        Returns
        -------
        bool
            True if the dataset passes validation.
        """
        ...

    def garbage_collect(self, expiry_time: datetime) -> icechunk.GCSummary:
        """
        Run Icechunk garbage collection and snapshot removal.

        Parameters
        ----------
            repo: And Icechunk Repository.
            expiry_time: Remove snapshots older than this time.
        Returns
        -------
        GCSummary
        """
        ...

    # def cron_processing(self, store: IcechunkStore) -> str:
    # """
    # Variable level operations that need to be run periodically and then
    # released as a tag.

    # Parameters
    # ----------
    # store: And Icechunk store.
    # Returns
    # -------
    # str
    # """
    # ...
