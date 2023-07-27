import typing
from typing import Any, Dict, List, Optional, Protocol, Type, Union

from hamilton import base, graph, node
from hamilton.function_modifiers.adapters import SaveToDecorator
from hamilton.function_modifiers.dependencies import SingleDependency, value
from hamilton.graph import FunctionGraph
from hamilton.io.data_adapters import DataSaver
from hamilton.registry import LOADER_REGISTRY


class materialization_meta__(type):
    """Metaclass for the load_from decorator. This is specifically to allow class access method.
    Note that there *is* another way to do this -- we couold add attributes dynamically on the
    class in registry, or make it a function that just proxies to the decorator. We can always
    change this up, but this felt like a pretty clean way of doing it, where we can decouple the
    registry from the decorator class.
    """

    def __getattr__(cls, item: str):
        if item in LOADER_REGISTRY:
            potential_loaders = LOADER_REGISTRY[item]
            savers = [loader for loader in potential_loaders if issubclass(loader, DataSaver)]
            if len(savers) > 0:
                return materialize.partial(LOADER_REGISTRY[item])
        try:
            return super().__getattribute__(item)
        except AttributeError as e:
            raise AttributeError(
                f"No loader named: {item} available for {cls.__name__}. "
                f"Available loaders are: {LOADER_REGISTRY.keys()}. "
                f"If you've gotten to this point, you either (1) spelled the "
                f"loader name wrong, (2) are trying to use a loader that does"
                f"not exist (yet)"
            ) from e


class MaterializerFactory:
    def __init__(
        self,
        name: str,
        savers: List[Type[DataSaver]],
        result_builder: Optional[base.ResultMixin],
        dependencies: List[str],
        **data_saver_kwargs: Any,
    ):
        self.name = name
        self.savers = savers
        self.result_builder = result_builder
        self.dependencies = dependencies
        self.data_saver_kwargs = self._process_kwargs(data_saver_kwargs)

    @staticmethod
    def _process_kwargs(
        data_saver_kwargs: Dict[str, Union[Any, SingleDependency]]
    ) -> Dict[str, SingleDependency]:
        """Processes raw strings from the user, converting them into dependency specs.

        :param data_saver_kwargs: Kwargs passed in from the user
        :return:
        """
        processed_kwargs = {}
        for kwarg, kwarg_val in data_saver_kwargs.items():
            if not isinstance(kwarg_val, SingleDependency):
                processed_kwargs[kwarg] = value(kwarg_val)
            else:
                processed_kwargs[kwarg] = kwarg_val
        return processed_kwargs

    def _resolve_dependencies(self, fn_graph: graph.FunctionGraph) -> List[node.Node]:
        return [fn_graph.nodes[name] for name in self.dependencies]

    def resolve(self, fn_graph: graph.FunctionGraph) -> List[node.Node]:
        """Resolves a materializer, returning the set of nodes that should get
        appended to the function graph. This does two things:

        1. Adds a node that handles result-building
        2. Adds a node that handles data-saving, reusing the data saver functionality.

        :param graph:
        :return:
        """
        node_dependencies = self._resolve_dependencies(fn_graph)

        def join_function(**kwargs):
            return self.result_builder.build_result(**kwargs)

        out = []
        if self.result_builder is None:
            if len(node_dependencies) != 1:
                raise ValueError(
                    "Must specify result builder if the materializer has more than one dependency "
                    "it is materializing. Otherwise we have no way to join them before storage! "
                    f"See materializer {self.name}."
                )
            save_dep = node_dependencies[0]
        else:
            join_node = node.Node(
                name=f"{self.name}_build_result",
                typ=self.result_builder.output_type(),
                doc_string=f"Builds the result for {self.name} materializer",
                callabl=join_function,
                input_types={dep.name: dep.type for dep in node_dependencies},
            )
            out.append(join_node)
            save_dep = join_node

        out.append(
            SaveToDecorator(self.savers, self.name, **self.data_saver_kwargs).create_saver_node(
                save_dep, {}, save_dep.callable
            )
        )
        return out


@typing.runtime_checkable
class _FactoryProtocol(Protocol):
    """Typing for the create_materializer_factory function"""

    def __call__(
        self,
        name: str,
        dependencies: List[str],
        join: base.ResultMixin = None,
        **kwargs: Union[str, SingleDependency],
    ) -> MaterializerFactory:
        ...


class materialize(metaclass=materialization_meta__):
    @classmethod
    def partial(cls, data_savers: List[Type[DataSaver]]) -> _FactoryProtocol:
        def create_materializer_factory(
            name: str, dependencies: List[str], join: base.ResultMixin = None, **kwargs: typing.Any
        ) -> MaterializerFactory:
            return MaterializerFactory(
                name=name,
                savers=data_savers,
                result_builder=join,
                dependencies=dependencies,
                **kwargs,
            )

        return create_materializer_factory


def modify_graph(
    fn_graph: FunctionGraph, materializer_factory: List[MaterializerFactory]
) -> FunctionGraph:
    """Modifies the function graph, adding in the specified materialization nodes.

    :param graph:
    :param materializers:
    :return:
    """
    additional_nodes = []
    for materializer in materializer_factory:
        additional_nodes.extend(materializer.resolve(fn_graph))
    return fn_graph.with_nodes({node_.name: node_ for node_ in additional_nodes})
