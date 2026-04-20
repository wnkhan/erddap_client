"""Utilities for exploring ERDDAP servers and datasets."""

from .mapping import make_map_axes
from .erddap_wrapper import ErddapIngestor, GLIDER_URL
from .glider_cache import (
    build_cache,
    build_grid_dataset_map,
    dataset_ids_by_grid,
    list_grid_labels,
    list_grid_dataset_ids,
    load_dataset_data,
    load_grid_datasets,
    load_grid_data,
    write_grid_cache,
)

__all__ = [
    "make_map_axes",
    "ErddapIngestor",
    "GLIDER_URL",
    "build_cache",
    "build_grid_dataset_map",
    "dataset_ids_by_grid",
    "list_grid_labels",
    "list_grid_dataset_ids",
    "load_dataset_data",
    "load_grid_datasets",
    "load_grid_data",
    "write_grid_cache",
]
