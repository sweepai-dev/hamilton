import abc
import functools
import logging
import sys
import time

# required if we want to run this code stand alone.
import typing
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from types import ModuleType
from typing import Any, Callable, Collection, Dict, List, Optional, Set, Tuple, Union

import pandas as pd

from hamilton.execution import executors, graph_functions, grouping, state

SLACK_ERROR_MESSAGE = (
    "-------------------------------------------------------------------\n"
    "Oh no an error! Need help with Hamilton?\n"
    "Join our slack and ask for help! https://join.slack.com/t/hamilton-opensource/shared_invite/zt-1bjs72asx-wcUTgH7q7QX1igiQ5bbdcg\n"
    "-------------------------------------------------------------------\n"
)

if __name__ == "__main__":
    import base
    import graph
    import node
    import telemetry
else:
    from . import base, graph, node, telemetry

logger = logging.getLogger(__name__)


def capture_function_usage(call_fn: Callable) -> Callable:
    """Decorator to wrap some driver functions for telemetry capture.

    We want to use this for non-constructor and non-execute functions.
    We don't capture information about the arguments at this stage,
    just the function name.

    :param call_fn: the Driver function to capture.
    :return: wrapped function.
    """

    @functools.wraps(call_fn)
    def wrapped_fn(*args, **kwargs):
        try:
            return call_fn(*args, **kwargs)
        finally:
            if telemetry.is_telemetry_enabled():
                try:
                    function_name = call_fn.__name__
                    event_json = telemetry.create_driver_function_invocation_event(function_name)
                    telemetry.send_event_json(event_json)
                except Exception as e:
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.error(
                            f"Failed to send telemetry for function usage. Encountered:{e}\n"
                        )

    return wrapped_fn


@dataclass
class Variable:
    """External facing API for hamilton. Having this as a dataclass allows us
    to hide the internals of the system but expose what the user might need.
    Furthermore, we can always add attributes and maintain backwards compatibility."""

    name: str
    type: typing.Type
    tags: Dict[str, str] = field(default_factory=dict)
    is_external_input: bool = field(default=False)
    originating_functions: Optional[Tuple[Callable, ...]] = None

    @staticmethod
    def from_node(n: node.Node) -> "Variable":
        """Creates a Variable from a Node.

        :param n: Node to create the Variable from.
        :return: Variable created from the Node.
        """
        return Variable(
            name=n.name,
            type=n.type,
            tags=n.tags,
            is_external_input=n.user_defined,
            originating_functions=n.originating_functions,
        )


class DriverCommon:
    """Driver functionality common between the V1 and V2 driver"""

    def __init__(
        self,
        config: Dict[str, Any],
        *modules: ModuleType,
        adapter: base.HamiltonGraphAdapter = None,
    ):
        """Constructor: creates a DAG given the configuration & modules to crawl.

        :param config: This is a dictionary of initial data & configuration.
                       The contents are used to help create the DAG.
        :param modules: Python module objects you want to inspect for Hamilton Functions.
        :param adapter: Optional. A way to wire in another way of "executing" a hamilton graph.
                        Defaults to using original Hamilton adapter which is single threaded in memory python.
        """
        self.driver_run_id = uuid.uuid4()
        if adapter is None:
            adapter = base.SimplePythonDataFrameGraphAdapter()
        error = None
        self.graph_modules = modules
        try:
            self.graph = graph.FunctionGraph.from_modules(*modules, config=config, adapter=adapter)
            self.adapter = adapter
        except Exception as e:
            error = telemetry.sanitize_error(*sys.exc_info())
            logger.error(SLACK_ERROR_MESSAGE)
            raise e
        finally:
            self.capture_constructor_telemetry(error, modules, config, adapter)

    def capture_constructor_telemetry(
        self,
        error: Optional[str],
        modules: Tuple[ModuleType],
        config: Dict[str, Any],
        adapter: base.HamiltonGraphAdapter,
    ):
        """Captures constructor telemetry.

        Notes:
        (1) we want to do this in a way that does not break.
        (2) we need to account for all possible states, e.g. someone passing in None, or assuming that
        the entire constructor code ran without issue, e.g. `adpater` was assigned to `self`.

        :param error: the sanitized error string to send.
        :param modules: the list of modules, could be None.
        :param config: the config dict passed, could be None.
        :param adapter: the adapter passed in, might not be attached to `self` yet.
        """
        if telemetry.is_telemetry_enabled():
            try:
                adapter_name = telemetry.get_adapter_name(adapter)
                result_builder = telemetry.get_result_builder_name(adapter)
                # being defensive here with ensuring values exist
                payload = telemetry.create_start_event_json(
                    len(self.graph.nodes) if hasattr(self, "graph") else 0,
                    len(modules) if modules else 0,
                    len(config) if config else 0,
                    dict(self.graph.decorator_counter) if hasattr(self, "graph") else {},
                    adapter_name,
                    result_builder,
                    self.driver_run_id,
                    error,
                    self.__class__.__qualname__,
                )
                telemetry.send_event_json(payload)
            except Exception as e:
                # we don't want this to fail at all!
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Error caught in processing telemetry: {e}")

    def _node_is_required_by_anything(self, node_: node.Node, node_set: Set[node.Node]) -> bool:
        """Checks dependencies on this node and determines if at least one requires it.

        Nodes can be optionally depended upon, i.e. the function parameter has a default value. We want to check that
        of the nodes the depend on this one, at least one of them requires it, i.e. the parameter is not optional.

        :param node_: node in question
        :param node_set: checks that we traverse only nodes in the provided set.
        :return: True if it is required by any downstream node, false otherwise
        """
        required = False
        for downstream_node in node_.depended_on_by:
            if downstream_node not in node_set:
                continue
            _, dep_type = downstream_node.input_types[node_.name]
            if dep_type == node.DependencyType.REQUIRED:
                return True
        return required

    def validate_inputs(
        self,
        user_nodes: Collection[node.Node],
        inputs: typing.Optional[Dict[str, Any]] = None,
        nodes_set: Collection[node.Node] = None,
    ):
        """Validates that inputs meet our expectations. This means that:
        1. The runtime inputs don't clash with the graph's config
        2. All expected graph inputs are provided, either in config or at runtime

        :param user_nodes: The required nodes we need for computation.
        :param inputs: the user inputs provided.
        :param nodes_set: the set of nodes to use for validation; Optional.
        """
        if inputs is None:
            inputs = {}
        if nodes_set is None:
            nodes_set = set(self.graph.nodes.values())
        (all_inputs,) = (graph_functions.combine_config_and_inputs(self.graph.config, inputs),)
        errors = []
        for user_node in user_nodes:
            if user_node.name not in all_inputs:
                if self._node_is_required_by_anything(user_node, nodes_set):
                    errors.append(
                        f"Error: Required input {user_node.name} not provided "
                        f"for nodes: {[node.name for node in user_node.depended_on_by]}."
                    )
            elif all_inputs[user_node.name] is not None and not self.adapter.check_input_type(
                user_node.type, all_inputs[user_node.name]
            ):
                errors.append(
                    f"Error: Type requirement mismatch. Expected {user_node.name}:{user_node.type} "
                    f"got {all_inputs[user_node.name]}:{type(all_inputs[user_node.name])} instead."
                )
        if errors:
            errors.sort()
            error_str = f"{len(errors)} errors encountered:\n  " + "\n  ".join(errors)
            raise ValueError(error_str)

    def capture_execute_telemetry(
        self,
        error: Optional[str],
        final_vars: List[str],
        inputs: Dict[str, Any],
        overrides: Dict[str, Any],
        run_successful: bool,
        duration: float,
    ):
        """Captures telemetry after execute has run.

        Notes:
        (1) we want to be quite defensive in not breaking anyone's code with things we do here.
        (2) thus we want to double-check that values exist before doing something with them.

        :param error: the sanitized error string to capture, if any.
        :param final_vars: the list of final variables to get.
        :param inputs: the inputs to the execute function.
        :param overrides: any overrides to the execute function.
        :param run_successful: whether this run was successful.
        :param duration: time it took to run execute.
        """
        if telemetry.is_telemetry_enabled():
            try:
                payload = telemetry.create_end_event_json(
                    run_successful,
                    duration,
                    len(final_vars) if final_vars else 0,
                    len(overrides) if isinstance(overrides, Dict) else 0,
                    len(inputs) if isinstance(overrides, Dict) else 0,
                    self.driver_run_id,
                    error,
                )
                telemetry.send_event_json(payload)
            except Exception as e:
                # we don't want this to fail at all!
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Error caught in processing telemetry:\n{e}")

    def _create_final_vars(self, final_vars: List[Union[str, Callable, Variable]]) -> List[str]:
        """Creates the final variables list - converting functions names as required.

        :param final_vars:
        :return: list of strings in the order that final_vars was provided.
        """
        _final_vars = []
        errors = []
        module_set = {_module.__name__ for _module in self.graph_modules}
        for final_var in final_vars:
            if isinstance(final_var, str):
                _final_vars.append(final_var)
            elif isinstance(final_var, Variable):
                _final_vars.append(final_var.name)
            elif isinstance(final_var, Callable):
                if final_var.__module__ in module_set:
                    _final_vars.append(final_var.__name__)
                else:
                    errors.append(
                        f"Function {final_var.__module__}.{final_var.__name__} is a function not in a "
                        f"module given to the driver. Valid choices are {module_set}."
                    )
            else:
                errors.append(
                    f"Final var {final_var} is not a string, a function, or a driver.Variable."
                )
        if errors:
            errors.sort()
            error_str = f"{len(errors)} errors encountered:\n  " + "\n  ".join(errors)
            raise ValueError(error_str)
        return _final_vars

    @capture_function_usage
    def list_available_variables(self) -> List[Variable]:
        """Returns available variables, i.e. outputs.

        These variables corresond 1:1 with nodes in the DAG, and contain the following information:
        1. name: the name of the node
        2. tags: the tags associated with this node
        3. type: The type of data this node returns
        4. is_external_input: Whether this node represents an external input (required from outside),
        or not (has a function specifying its behavior).


        :return: list of available variables (i.e. outputs).
        """
        return [Variable.from_node(n) for n in self.graph.get_nodes()]

    @capture_function_usage
    def display_all_functions(
        self, output_file_path: str, render_kwargs: dict = None, graphviz_kwargs: dict = None
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Displays the graph of all functions loaded!

        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph-all.dot'
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
            See https://graphviz.readthedocs.io/en/stable/api.html#graphviz.Graph.render for other options.
        :param graphviz_kwargs: Optional. Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
            See https://graphviz.org/doc/info/attrs.html for options.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        try:
            return self.graph.display_all(output_file_path, render_kwargs, graphviz_kwargs)
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def visualize_execution(
        self,
        final_vars: List[Union[str, Callable, Variable]],
        output_file_path: str,
        render_kwargs: dict,
        inputs: Dict[str, Any] = None,
        graphviz_kwargs: dict = None,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Visualizes Execution.

        Note: overrides are not handled at this time.

        Shapes:

         - ovals are nodes/functions
         - rectangles are nodes/functions that are requested as output
         - shapes with dotted lines are inputs required to run the DAG.

        :param final_vars: the outputs we want to compute. They will become rectangles in the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
            See https://graphviz.readthedocs.io/en/stable/api.html#graphviz.Graph.render for other options.
        :param inputs: Optional. Runtime inputs to the DAG.
        :param graphviz_kwargs: Optional. Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
            See https://graphviz.org/doc/info/attrs.html for options.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        _final_vars = self._create_final_vars(final_vars)
        nodes, user_nodes = self.graph.get_upstream_nodes(_final_vars, inputs)
        self.validate_inputs(user_nodes, inputs, nodes)
        node_modifiers = {fv: {graph.VisualizationNodeModifiers.IS_OUTPUT} for fv in _final_vars}
        for user_node in user_nodes:
            if user_node.name not in node_modifiers:
                node_modifiers[user_node.name] = set()
            node_modifiers[user_node.name].add(graph.VisualizationNodeModifiers.IS_USER_INPUT)
        try:
            return self.graph.display(
                nodes.union(user_nodes),
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                node_modifiers=node_modifiers,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def has_cycles(self, final_vars: List[Union[str, Callable, Variable]]) -> bool:
        """Checks that the created graph does not have cycles.

        :param final_vars: the outputs we want to compute.
        :return: boolean True for cycles, False for no cycles.
        """
        _final_vars = self._create_final_vars(final_vars)
        # get graph we'd be executing over
        nodes, user_nodes = self.graph.get_upstream_nodes(_final_vars)
        return self.graph.has_cycles(nodes, user_nodes)

    @capture_function_usage
    def what_is_downstream_of(self, *node_names: str) -> List[Variable]:
        """Tells you what is downstream of this function(s), i.e. node(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :return: list of "variables" (i.e. nodes), inclusive of the function names, that are downstream of the passed
                in function names.
        """
        downstream_nodes = self.graph.get_downstream_nodes(list(node_names))
        return [Variable.from_node(n) for n in downstream_nodes]

    @capture_function_usage
    def display_downstream_of(
        self, *node_names: str, output_file_path: str, render_kwargs: dict, graphviz_kwargs: dict
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Creates a visualization of the DAG starting from the passed in function name(s).

        Note: for any "node" visualized, we will also add its parents to the visualization as well, so
        there could be more nodes visualized than strictly what is downstream of the passed in function name(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Pass in None to skip saving any file.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        downstream_nodes = self.graph.get_downstream_nodes(list(node_names))
        try:
            return self.graph.display(
                downstream_nodes,
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                strictly_display_only_passed_in_nodes=False,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def display_upstream_of(
        self, *node_names: str, output_file_path: str, render_kwargs: dict, graphviz_kwargs: dict
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Creates a visualization of the DAG going backwards from the passed in function name(s).

        Note: for any "node" visualized, we will also add its parents to the visualization as well, so
        there could be more nodes visualized than strictly what is downstream of the passed in function name(s).

        :param node_names: names of function(s) that are starting points for traversing the graph.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Pass in None to skip saving any file.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        :return: the graphviz object if you want to do more with it.
            If returned as the result in a Jupyter Notebook cell, it will render.
        """
        upstream_nodes, user_nodes = self.graph.get_upstream_nodes(list(node_names))
        node_modifiers = {}
        for n in user_nodes:
            node_modifiers[n.name] = {graph.VisualizationNodeModifiers.IS_USER_INPUT}
        try:
            return self.graph.display(
                upstream_nodes,
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                strictly_display_only_passed_in_nodes=False,
                node_modifiers=node_modifiers,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @capture_function_usage
    def what_is_upstream_of(self, *node_names: str) -> List[Variable]:
        """Tells you what is upstream of this function(s), i.e. node(s).

        :param node_names: names of function(s) that are starting points for traversing the graph backwards.
        :return: list of "variables" (i.e. nodes), inclusive of the function names, that are upstream of the passed
                in function names.
        """
        upstream_nodes, _ = self.graph.get_upstream_nodes(list(node_names))
        return [Variable.from_node(n) for n in upstream_nodes]

    @capture_function_usage
    def what_is_the_path_between(
        self, upstream_node_name: str, downstream_node_name: str
    ) -> List[Variable]:
        """Tells you what nodes are on the path between two nodes.

        Note: this is inclusive of the two nodes, and returns an unsorted list of nodes.

        :param upstream_node_name: the name of the node that we want to start from.
        :param downstream_node_name: the name of the node that we want to end at.
        :return: Nodes representing the path between the two nodes, inclusive of the two nodes, unsorted.
            Returns empty list if no path exists.
        :raise ValueError: if the upstream or downstream node name is not in the graph.
        """
        all_variables = {n.name: n for n in self.graph.get_nodes()}
        # ensure that the nodes exist
        if upstream_node_name not in all_variables:
            raise ValueError(f"Upstream node {upstream_node_name} not found in graph.")
        if downstream_node_name not in all_variables:
            raise ValueError(f"Downstream node {downstream_node_name} not found in graph.")
        nodes_for_path = self.graph.nodes_between(upstream_node_name, downstream_node_name)
        return [Variable.from_node(n) for n in nodes_for_path]

    @capture_function_usage
    def visualize_path_between(
        self,
        upstream_node_name: str,
        downstream_node_name: str,
        output_file_path: Optional[str] = None,
        render_kwargs: dict = None,
        graphviz_kwargs: dict = None,
        strict_path_visualization: bool = False,
    ) -> Optional["graphviz.Digraph"]:  # noqa F821
        """Visualizes the path between two nodes.

        This is useful for debugging and understanding the path between two nodes.

        :param upstream_node_name: the name of the node that we want to start from.
        :param downstream_node_name: the name of the node that we want to end at.
        :param output_file_path: the full URI of path + file name to save the dot file to.
            E.g. 'some/path/graph.dot'. Pass in None to skip saving any file.
        :param render_kwargs: a dictionary of values we'll pass to graphviz render function. Defaults to viewing.
            If you do not want to view the file, pass in `{'view':False}`.
        :param graphviz_kwargs: Kwargs to be passed to the graphviz graph object to configure it.
            E.g. dict(graph_attr={'ratio': '1'}) will set the aspect ratio to be equal of the produced image.
        :param strict_path_visualization: If True, only the nodes in the path will be visualized. If False, the
            nodes in the path and their dependencies, i.e. parents, will be visualized.
        :return: graphviz object.
        :raise ValueError: if the upstream or downstream node names are not found in the graph,
            or there is no path between them.
        """
        if render_kwargs is None:
            render_kwargs = {}
        if graphviz_kwargs is None:
            graphviz_kwargs = {}
        all_variables = {n.name: n for n in self.graph.get_nodes()}
        # ensure that the nodes exist
        if upstream_node_name not in all_variables:
            raise ValueError(f"Upstream node {upstream_node_name} not found in graph.")
        if downstream_node_name not in all_variables:
            raise ValueError(f"Downstream node {downstream_node_name} not found in graph.")

        # set whether the node is user input
        node_modifiers = {}
        for n in self.graph.get_nodes():
            if n.user_defined:
                node_modifiers[n.name] = {graph.VisualizationNodeModifiers.IS_USER_INPUT}

        # create nodes that constitute the path
        nodes_for_path = self.graph.nodes_between(upstream_node_name, downstream_node_name)
        if len(nodes_for_path) == 0:
            raise ValueError(
                f"No path found between {upstream_node_name} and {downstream_node_name}."
            )
        # add is path for node_modifier's dict
        for n in nodes_for_path:
            if n.name not in node_modifiers:
                node_modifiers[n.name] = set()
            node_modifiers[n.name].add(graph.VisualizationNodeModifiers.IS_PATH)
        try:
            return self.graph.display(
                nodes_for_path,
                output_file_path,
                render_kwargs=render_kwargs,
                graphviz_kwargs=graphviz_kwargs,
                node_modifiers=node_modifiers,
                strictly_display_only_passed_in_nodes=strict_path_visualization,
            )
        except ImportError as e:
            logger.warning(f"Unable to import {e}", exc_info=True)

    @abc.abstractmethod
    def execute(
        self,
        final_vars: List[Union[str, Callable, Variable]],
        overrides: Dict[str, Any] = None,
        display_graph: bool = False,
        inputs: Dict[str, Any] = None,
    ) -> Any:
        pass


class Driver(DriverCommon):
    """This class orchestrates creating and executing the DAG to create a dataframe.

    .. code-block:: python

        from hamilton import driver
        from hamilton import base

        # 1. Setup config or invariant input.
        config = {}

        # 2. we need to tell hamilton where to load function definitions from
        import my_functions
        # or programmatically (e.g. you can script module loading)
        module_name = 'my_functions'
        my_functions = importlib.import_module(module_name)

        # 3. Determine the return type -- default is a pandas.DataFrame.
        adapter = base.SimplePythonDataFrameGraphAdapter() # See GraphAdapter docs for more details.

        # These all feed into creating the driver & thus DAG.
        dr = driver.Driver(config, module, adapter=adapter)

    """

    def execute(
        self,
        final_vars: List[Union[str, Callable, Variable]],
        overrides: Dict[str, Any] = None,
        display_graph: bool = False,
        inputs: Dict[str, Any] = None,
    ) -> Any:
        """Executes computation.

        :param final_vars: the final list of outputs we want to compute.
        :param overrides: values that will override "nodes" in the DAG.
        :param display_graph: DEPRECATED. Whether we want to display the graph being computed.
        :param inputs: Runtime inputs to the DAG.
        :return: an object consisting of the variables requested, matching the type returned by the GraphAdapter.
            See constructor for how the GraphAdapter is initialized. The default one right now returns a pandas
            dataframe.
        """
        if display_graph:
            logger.warning(
                "display_graph=True is deprecated. It will be removed in the 2.0.0 release. "
                "Please use visualize_execution()."
            )
        start_time = time.time()
        run_successful = True
        error = None
        _final_vars = self._create_final_vars(final_vars)
        try:
            outputs = self.raw_execute(_final_vars, overrides, display_graph, inputs=inputs)
            result = self.adapter.build_result(**outputs)
            return result
        except Exception as e:
            run_successful = False
            logger.error(SLACK_ERROR_MESSAGE)
            error = telemetry.sanitize_error(*sys.exc_info())
            raise e
        finally:
            duration = time.time() - start_time
            self.capture_execute_telemetry(
                error, _final_vars, inputs, overrides, run_successful, duration
            )

    def raw_execute(
        self,
        final_vars: List[str],
        overrides: Dict[str, Any] = None,
        display_graph: bool = False,
        inputs: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        """Raw execute function that does the meat of execute.

        It does not try to stitch anything together. Thus allowing wrapper executes around this to shape the output
        of the data.

        :param final_vars: Final variables to compute
        :param overrides: Overrides to run.
        :param display_graph: DEPRECATED. DO NOT USE. Whether or not to display the graph when running it
        :param inputs: Runtime inputs to the DAG
        :return:
        """
        nodes, user_nodes = self.graph.get_upstream_nodes(final_vars, inputs)
        self.validate_inputs(
            user_nodes, inputs, nodes
        )  # TODO -- validate within the function graph itself
        if display_graph:  # deprecated flow.
            logger.warning(
                "display_graph=True is deprecated. It will be removed in the 2.0.0 release. "
                "Please use visualize_execution()."
            )
            self.visualize_execution(final_vars, "test-output/execute.gv", {"view": True})
            if self.has_cycles(final_vars):  # here for backwards compatible driver behavior.
                raise ValueError("Error: cycles detected in you graph.")
        memoized_computation = dict()  # memoized storage
        self.graph.execute(nodes, memoized_computation, overrides, inputs)
        outputs = {
            c: memoized_computation[c] for c in final_vars
        }  # only want request variables in df.
        del memoized_computation  # trying to cleanup some memory
        return outputs


class DriverV2(DriverCommon):
    """Represents a V2 driver. This utilizes task-based execution, and will have the capability
    to handle materialization + chaining of drivers. This is currently separate (but inherits
    from a common class) to avoid bloat of the prvious driver.

    Note that this should be instantiated through the builder, *not* through the constructor,
    as that is liable to change.
    """

    def __init__(
        self,
        *,
        modules: List[ModuleType],
        config: Dict[str, Any] = None,
        execution_manager: executors.ExecutionManager = None,
        grouping_strategy: grouping.GroupingStrategy = None,
        result_builder: base.ResultMixin = None,
    ):
        """Initializes a DriverV2. This takes in execution-specific parameters.
        Note you don't actually want to call this directly -- instead call the builder.

        :param modules: Modules to crawl for hamilton functions.
        :param config: Configuration to use.
        :param execution_manager: Decides which executor to assign to which tasks.
        :param grouping_strategy: Strategy for grouping nodes into tasks.
        :param result_builder: Result builder to use.
        """
        super(DriverV2, self).__init__(config, *modules)
        self.execution_manager = execution_manager
        self.grouping_strategy = grouping_strategy
        self.result_builder = result_builder if result_builder is not None else base.DictResult()

    def execute(
        self,
        final_vars: List[Union[str, Callable, Variable]],
        overrides: Dict[str, Any] = None,
        inputs: Dict[str, Any] = None,
    ) -> Any:
        """Executes a Hamilton DAG. Note this currently does not utilize a results builder.
        This will change -- we will be appending the results builder to the DAG if supplied.

        :param final_vars: Variables to request form the DAG.
        :param overrides: Overrides -- this short-circuits computation of variables and instead
        returns the specified override.
        :param inputs: Parameterized inputs to the DAG
        :return: A Dictionary containing the restults of the DAG.
        """

        overrides = overrides if overrides is not None else {}
        inputs = inputs if inputs is not None else {}
        nodes, user_nodes = self.graph.get_upstream_nodes(final_vars, inputs)
        self.validate_inputs(user_nodes, inputs, nodes)
        (
            transform_nodes_required_for_execution,
            user_defined_nodes_required_for_execution,
        ) = self.graph.get_upstream_nodes(
            final_vars, runtime_inputs=inputs, runtime_overrides=overrides
        )

        all_nodes_required_for_execution = list(
            set(transform_nodes_required_for_execution).union(
                user_defined_nodes_required_for_execution
            )
        )
        grouped_nodes = self.grouping_strategy.group_nodes(
            all_nodes_required_for_execution
        )  # pure function transform
        # Instantiate a result cache so we can use later
        # Pass in inputs so we can pre-populate the results cache
        prehydrated_results = {**overrides, **inputs}
        results_cache = state.DictBasedResultCache(prehydrated_results)
        # Create tasks from the grouped nodes, filtering/pruning as we go
        tasks = grouping.create_task_plan(
            grouped_nodes, final_vars, overrides, [self.graph.adapter]
        )
        # Create a task graph and execution state
        execution_state = state.ExecutionState(tasks, results_cache)  # Stateful storage for the DAG
        # Run the graph (Stateless while loop for executing the DAG)
        graph_runner = executors.GraphRunner(execution_state, self.execution_manager)
        # Blocking call to run through until completion
        graph_runner.run_until_complete()
        # Read the final variables from the result cache
        raw_result = results_cache.read(final_vars)
        return self.result_builder.build_result(**raw_result)


class Builder:
    """Utility class to handle building the driver. This is meant to allow the user
    to specify *just* what they need, and rely on reasonable defaults for the rest.

    """

    def __init__(self):
        """Creates a driver with all fields unset/defaulted"""
        # Toggling versions
        self.v2_driver = False

        # common fields
        self.config = {}
        self.modules = []
        # V1 fields
        self.adapter = None

        # V2 fields
        self.execution_manager = None
        self.local_executor = None
        self.remote_executor = None
        self.grouping_strategy = None
        self.result_builder = None

    def _require_v2(self, message: str):
        if not self.v2_driver:
            raise ValueError(message)

    def _require_field_unset(self, field: str, message: str, unset_value: Any = None):
        if getattr(self, field) != unset_value:
            raise ValueError(message)

    def _require_field_set(self, field: str, message: str, unset_value: Any = None):
        if getattr(self, field) == unset_value:
            raise ValueError(message)

    def enable_v2_driver(self, *, allow_experimental_mode: bool = False) -> "Builder":
        """Enables the new driver. This enables:
            1. Grouped execution into tasks
            2. Parallel execution

            and in the future:
            3. Materialization of results
            4. Custom execution hooks
            5. Cachine/more powerful tooling

        :return: self
        """
        if not allow_experimental_mode:
            raise ValueError(
                "Remote execution is currently experimental. "
                "Please set allow_experiemental_mode=True to enable it."
            )
        self._require_field_unset("adapter", "Cannot enable remote execution with an adapter set.")
        self.v2_driver = True
        return self

    def with_config(self, config: Dict[str, Any]) -> "Builder":
        """Adds the specified configuration to the config.
        This can be called multilple times -- later calls will take precedence.

        :param config: Config to use.
        :return: self
        """
        self.config.update(config)
        return self

    def with_modules(self, *modules: ModuleType) -> "Builder":
        """Adds the specified modules to the modules list.
        This can be called multiple times -- later calls will take precedence.

        :param modules: Modules to use.
        :return: self
        """
        self.modules.extend(modules)
        return self

    def with_adapter(self, adapter: base.HamiltonGraphAdapter) -> "Builder":
        """Sets the adapter to use.

        :param adapter: Adapter to use.
        :return: self
        """
        self._require_field_unset("v2_driver", "Cannot set adapter with v2 driver enabled.")
        self._require_field_unset("adapter", "Cannot set adapter twice.")
        self.adapter = adapter
        return self

    def with_execution_manager(self, execution_manager: executors.ExecutionManager) -> "Builder":
        """Sets the execution manager to use. Note that this cannot be used if local_executor
        or remote_executor are also set

        :param execution_manager:
        :return:
        """
        self._require_v2("Cannot set execution manager without first enabling the V2 Driver")
        self._require_field_unset("execution_manager", "Cannot set execution manager twice")
        self._require_field_unset(
            "remote_executor",
            "Cannot set execution manager with remote " "executor set -- these are disjoint",
        )

        self.execution_manager = execution_manager
        return self

    def with_remote_executor(self, remote_executor: executors.TaskExecutor) -> "Builder":
        """Sets the execution manager to use. Note that this cannot be used if local_executor
        or remote_executor are also set

        :param execution_manager:
        :return:
        """
        self._require_v2("Cannot set execution manager without first enabling the V2 Driver")
        self._require_field_unset("remote_executor", "Cannot set remote executor twice")
        self._require_field_unset(
            "execution_manager",
            "Cannot set remote executor with execution " "manager set -- these are disjoint",
        )
        self.remote_executor = remote_executor
        return self

    def with_local_executor(self, local_executor: executors.TaskExecutor) -> "Builder":
        """Sets the execution manager to use. Note that this cannot be used if local_executor
        or remote_executor are also set

        :param execution_manager:
        :return:
        """
        self._require_v2("Cannot set execution manager without first enabling the V2 Driver")
        self._require_field_unset("local_executor", "Cannot set local executor twice")
        self._require_field_unset(
            "execution_manager",
            "Cannot set local executor with execution " "manager set -- these are disjoint",
        )
        self.local_executor = local_executor
        return self

    def with_grouping_strategy(self, grouping_strategy: grouping.GroupingStrategy) -> "Builder":
        """Sets a node grouper, which tells the driver how to group nodes into tasks for execution.

        :param node_grouper:
        :return:
        """
        self._require_v2("Cannot set grouping strategy without first enabling the V2 Driver")
        self._require_field_unset("grouping_strategy", "Cannot set grouping strategy twice")
        self.grouping_strategy = grouping_strategy
        return self

    def with_result_builder(self, result_builder: base.ResultMixin) -> "Builder":
        """Sets a result builder, which tells the driver how to build the result.

        :param result_builder:
        :return:
        """
        self._require_v2("Cannot set result builder without first enabling the V2 Driver")
        self._require_field_unset("result_builder", "Cannot set result builder twice")
        self.result_builder = result_builder
        return self

    @capture_function_usage
    def build(self) -> DriverCommon:
        """Builds the driver -- note that this can return a different class, so you'll likely
        want to have a sense of what it returns.

        :return: The driver you specified.
        """
        if not self.v2_driver:
            return Driver(self.config, *self.modules, adapter=self.adapter)
        execution_manager = self.execution_manager
        if execution_manager is None:
            local_executor = self.local_executor or executors.SynchronousLocalTaskExecutor()
            remote_executor = self.remote_executor or executors.MultiProcessingExecutor(
                max_tasks=10
            )
            execution_manager = executors.DefaultExecutionManager(
                local_executor=local_executor, remote_executor=remote_executor
            )
        grouping_strategy = self.grouping_strategy or grouping.GroupByRepeatableBlocks()
        return DriverV2(
            config=self.config,
            modules=self.modules,
            execution_manager=execution_manager,
            grouping_strategy=grouping_strategy,
            result_builder=self.result_builder,
        )


if __name__ == "__main__":
    """some example test code"""
    import importlib

    formatter = logging.Formatter("[%(levelname)s] %(asctime)s %(name)s(%(lineno)s): %(message)s")
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(logging.INFO)

    if len(sys.argv) < 2:
        logger.error("No modules passed")
        sys.exit(1)
    logger.info(f"Importing {sys.argv[1]}")
    module = importlib.import_module(sys.argv[1])

    x = pd.date_range("2019-01-05", "2020-12-31", freq="7D")
    x.index = x

    dr = Driver(
        {
            "VERSION": "kids",
            "as_of": datetime.strptime("2019-06-01", "%Y-%m-%d"),
            "end_date": "2020-12-31",
            "start_date": "2019-01-05",
            "start_date_d": datetime.strptime("2019-01-05", "%Y-%m-%d"),
            "end_date_d": datetime.strptime("2020-12-31", "%Y-%m-%d"),
            "segment_filters": {"business_line": "womens"},
        },
        module,
    )
    df = dr.execute(
        ["date_index", "some_column"]
        # ,overrides={'DATE': pd.Series(0)}
        ,
        display_graph=False,
    )
    print(df)
