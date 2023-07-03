import json
import logging
import os
from functools import singledispatch
from typing import Any, Callable, Dict, Optional, Set, Type

import typing_inspect

from hamilton.base import SimplePythonGraphAdapter
from hamilton.node import Node

logger = logging.getLogger(__name__)


@singledispatch
def write_feather(data: object, filepath: str, name: str) -> None:
    """Writes data to a feather file."""
    raise NotImplementedError(f"No feather writer for type {type(data)} registered.")


@singledispatch
def read_feather(data: object, filepath: str) -> Any:
    """Reads from a feather file"""
    raise NotImplementedError(f"No feather reader for type {type(data)} registered.")


@singledispatch
def write_parquet(data: object, filepath: str, name: str) -> None:
    """Writes data to a parquet file."""
    raise NotImplementedError(f"No parquet writer for type {type(data)} registered.")


@singledispatch
def read_parquet(data: object, filepath: str) -> Any:
    """Reads from a parquet file"""
    raise NotImplementedError(f"No parquet reader for type {type(data)} registered.")


@singledispatch
def write_json(data: object, filepath: str, name: str) -> None:
    """Writes data to a json file."""
    raise NotImplementedError(f"No json writer for type {type(data)} registered.")


@singledispatch
def read_json(data: object, filepath: str) -> Any:
    """Reads from a json file"""
    raise NotImplementedError(f"No json reader for type {type(data)} registered.")


try:
    import pandas as pd

    @write_json.register(pd.DataFrame)
    def write_json_pd1(data: pd.DataFrame, filepath: str, name: str) -> None:
        """Writes a data frame to a feather file."""
        return data.to_json(filepath)

    @write_json.register(pd.Series)
    def write_json_pd2(data: pd.Series, filepath: str, name: str) -> None:
        """Writes a data frame to a feather file."""
        _df = data.to_frame(name=name)
        return _df.to_json(filepath)

    @read_json.register(pd.Series)
    def read_json_pd1(data: pd.Series, filepath: str) -> pd.Series:
        """Reads a data frame from a feather file."""
        _df = pd.read_json(filepath)
        return _df[_df.columns[0]]

    @read_json.register(pd.DataFrame)
    def read_json_pd2(data: pd.DataFrame, filepath: str) -> pd.DataFrame:
        """Reads a data frame from a feather file."""
        return pd.read_json(filepath)

    try:
        import pyarrow  # noqa: F401

        @write_feather.register(pd.DataFrame)
        def write_feather_pd1(data: pd.DataFrame, filepath: str, name: str) -> None:
            """Writes a data frame to a feather file."""
            data.to_feather(filepath)

        @write_feather.register(pd.Series)
        def write_feather_pd2(data: pd.Series, filepath: str, name: str) -> None:
            """Writes a data frame to a feather file."""
            data.to_frame(name=name).to_feather(filepath)

        @read_feather.register(pd.Series)
        @read_feather.register(pd.DataFrame)
        def read_feather_pd1(data: pd.DataFrame, filepath: str) -> pd.DataFrame:
            """Reads a data frame from a feather file."""
            return pd.read_feather(filepath)

        @read_feather.register(pd.Series)
        def read_feather_pd2(data: pd.Series, filepath: str) -> pd.Series:
            """Reads a data frame from a feather file."""
            _df = pd.read_feather(filepath)
            return _df[_df.columns[0]]

        @write_parquet.register(pd.DataFrame)
        def write_parquet_pd1(data: pd.DataFrame, filepath: str, name: str) -> None:
            """Writes a data frame to a parquet file."""
            data.to_parquet(filepath)

        @write_parquet.register(pd.Series)
        def write_parquet_pd2(data: pd.Series, filepath: str, name: str) -> None:
            """Writes a data frame to a parquet file."""
            data.to_frame(name=name).to_parquet(filepath)

        @read_parquet.register(pd.DataFrame)
        def read_parquet_pd1(data: pd.DataFrame, filepath: str) -> pd.DataFrame:
            """Reads a data frame from a parquet file."""
            return pd.read_parquet(filepath)

        @read_parquet.register(pd.Series)
        def read_parquet_pd2(data: pd.Series, filepath: str) -> pd.Series:
            """Reads a data frame from a parquet file."""
            _df = pd.read_parquet(filepath)
            return _df[_df.columns[0]]

    except ImportError:
        pass


except ImportError:
    pass


@write_json.register(dict)
def write_json_dict(data: dict, filepath: str, name: str) -> None:
    """Writes a dictionary to a JSON file."""
    if isinstance(data, dict):
        with open(filepath, "w", encoding="utf8") as file:
            json.dump(data, file)
    else:
        raise ValueError(f"Expected a dict, got {type(data)}")


@read_json.register(dict)
def read_json_dict(data: dict, filepath: str) -> dict:
    """Reads a dictionary from a JSON file."""
    with open(filepath, "r", encoding="utf8") as file:
        return json.load(file)


class CachingAdapter(SimplePythonGraphAdapter):
    """Caching adapter.

    Any node with tag "cache" will be cached (or loaded from cache) in the format defined by the
    tag's value. There are a handful of formats supported, and other formats' readers and writers
    can be provided to the constructor.

    Values are loaded from cache if the node's file exists, unless one of these is true:
     * node is explicitly forced to be computed with a constructor argument,
     * any of its (potentially transitive) dependencies that are configured to be cached
       was nevertheless computed (either forced or missing cached file).
    """

    def __init__(
        self,
        cache_path: str,
        *args,
        force_compute: Optional[Set[str]] = None,
        writers: Optional[Dict[str, Callable[[Any, str], None]]] = None,
        readers: Optional[Dict[str, Callable[[str], Any]]] = None,
        **kwargs,
    ):
        """Constructs the adapter.

        :param cache_path: Path to the directory where cached files are stored.
        :param force_compute: Set of nodes that should be forced to compute even if cache exists.
        :param writers: A dictionary of writers for custom formats.
        :param readers: A dictionary of readers for custom formats.
        """

        super().__init__(*args, **kwargs)
        self.cache_path = cache_path
        self.force_compute = force_compute if force_compute is not None else {}
        self.computed_nodes = set()

        self.writers = writers or {}
        self.readers = readers or {}

        self._init_default_readers_writers()

    def _init_default_readers_writers(self):
        if "json" not in self.writers:
            self.writers["json"] = write_json
        if "json" not in self.readers:
            self.readers["json"] = read_json

        if "feather" not in self.writers:
            self.writers["feather"] = write_feather
        if "feather" not in self.readers:
            self.readers["feather"] = read_feather

        if "parquet" not in self.writers:
            self.writers["parquet"] = write_parquet
        if "parquet" not in self.readers:
            self.readers["parquet"] = read_parquet

    def _check_format(self, fmt):
        if fmt not in self.writers:
            raise ValueError(f"invalid cache format: {fmt}")

    def _write_cache(self, fmt: str, data: Any, filepath: str, node_name: str) -> None:
        self._check_format(fmt)
        self.writers[fmt](data, filepath, node_name)

    def _read_cache(self, fmt: str, expected_type: Any, filepath: str) -> None:
        self._check_format(fmt)
        return self.readers[fmt](expected_type, filepath)

    def _get_empty_expected_type(self, expected_type: Type) -> Any:
        if typing_inspect.is_generic_type(expected_type):
            return typing_inspect.get_origin(expected_type)()
        return expected_type()  # This ASSUMES that we can just do `str()`, `pd.DataFrame()`, etc.

    def execute_node(self, node: Node, kwargs: Dict[str, Any]) -> Any:
        """Executes nodes conditionally according to caching rules.

        This node is executed if at least one of these is true:
        * no cache is present,
        * it is explicitly forced by passing it to the adapter in ``force_compute``,
        * at least one of its upstream nodes that had a @cache annotation was computed,
          either due to lack of cache or being explicitly forced.
        """
        cache_format = node.tags.get("cache")
        implicitly_forced = any(dep.name in self.computed_nodes for dep in node.dependencies)
        if cache_format is not None:
            filepath = f"{self.cache_path}/{node.name}.{cache_format}"
            explicitly_forced = node.name in self.force_compute
            if explicitly_forced or implicitly_forced or not os.path.exists(filepath):
                result = node.callable(**kwargs)
                print(
                    f"Writing cache for {node.name} to {filepath} with type {type(result)} to {cache_format}"
                )
                self._write_cache(cache_format, result, filepath, node.name)
                self.computed_nodes.add(node.name)
                return result
            empty_expected_type = self._get_empty_expected_type(node.type)
            return self._read_cache(cache_format, empty_expected_type, filepath)

        if implicitly_forced:
            # For purposes of caching, we only mark it as computed if any cached input was computed.
            # Otherwise, dependants would always be recomputed if they have a non-cached dependency.
            self.computed_nodes.add(node.name)
        return node.callable(**kwargs)

    def build_result(self, **outputs: Dict[str, Any]) -> Any:
        """Clears the computed nodes information and delegates to the super class."""
        self.computed_nodes = set()
        return super().build_result(**outputs)
