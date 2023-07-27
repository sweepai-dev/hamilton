import dataclasses
from typing import Any, Collection, Dict, List, Optional, Type

from hamilton import base, graph, node
from hamilton.io.data_adapters import DataSaver
from hamilton.io.materialization import MaterializerFactory, _FactoryProtocol, materialize

global_mock_data_saver_cache = {}


@dataclasses.dataclass
class MockDataSaver(DataSaver):
    storage_key: str
    other_storage_key: Optional[str] = None

    def save_data(self, data: Any) -> Dict[str, Any]:
        global_mock_data_saver_cache[self.storage_key] = data
        if self.other_storage_key is not None:
            global_mock_data_saver_cache[self.other_storage_key] = data
        return {"saved": True}

    @classmethod
    def applicable_types(cls) -> Collection[Type]:
        return [dict]

    @classmethod
    def name(cls) -> str:
        return "mock_for_testing"


class JoinBuilder(base.ResultMixin):
    @staticmethod
    def build_result(**outputs: Dict[str, Any]) -> Any:
        out = {}
        for output in outputs.values():
            out.update(output)
        return out

    def output_type(self) -> Type:
        return dict

    def input_types(self) -> List[Type]:
        return [dict]


def test_materialization_dynamic_property_access():
    json_materializer = materialize.json
    assert isinstance(json_materializer, _FactoryProtocol)  # It should produce a factory function


def test_materializer_factory_generates_nodes_no_builder():
    factory = MaterializerFactory(
        "test_materializer",
        [MockDataSaver],
        dependencies=["only_node"],
        result_builder=None,
        storage_key="test_materializer_factory_generates_nodes_no_builder",
    )

    def test() -> dict:
        return {"test_materializer_factory_generates_nodes_no_builder": "ran_correctly"}

    base_node = node.Node.from_fn(test)
    fn_graph = graph.FunctionGraph({"only_node": base_node}, {})
    nodes = factory.resolve(fn_graph)
    assert len(nodes) == 1  # No builder node
    (node_,) = nodes
    # Call, test the side effect as well as the ret val
    res = node_(test=test())
    assert res == {"saved": True}
    assert (
        global_mock_data_saver_cache["test_materializer_factory_generates_nodes_no_builder"]
        == test()
    )


def test_materializer_factory_generates_nodes_with_builder():
    factory = MaterializerFactory(
        "test_materializer",
        [MockDataSaver],
        dependencies=["first_node", "second_node"],
        result_builder=JoinBuilder(),
        storage_key="test_materializer_factory_generates_nodes_with_builder",
        other_storage_key="test_materializer_factory_generates_nodes_with_builder_second_store",
    )

    def first_node() -> dict:
        return {"test_materializer_factory_generates_nodes_no_builder": "ran_correctly"}

    def second_node() -> dict:
        return {"test_materializer_factory_generates_nodes_no_builder_still": "ran_correctly"}

    base_node_0 = node.Node.from_fn(first_node)
    base_node_1 = node.Node.from_fn(second_node)

    fn_graph = graph.FunctionGraph(
        {"first_node": base_node_0, "second_node": base_node_1}, {}, None
    )
    nodes = factory.resolve(fn_graph)
    assert len(nodes) == 2  # One builder node
    nodes_by_name = {node_.name: node_ for node_ in nodes}
    # Call, test the side effect as well as the ret val
    materializer = nodes_by_name.pop("test_materializer")  # This one has a defined name
    res = materializer(test_materializer_build_result={**first_node(), **second_node()})
    assert res == {"saved": True}
    assert global_mock_data_saver_cache[
        "test_materializer_factory_generates_nodes_with_builder"
    ] == {**first_node(), **second_node()}
    (joiner,) = nodes_by_name.values()
    assert joiner(first_node=first_node(), second_node=second_node()) == {
        **first_node(),
        **second_node(),
    }
    assert global_mock_data_saver_cache[
        "test_materializer_factory_generates_nodes_with_builder_second_store"
    ] == {**first_node(), **second_node()}
