from typing import Any, Union

from networkx import DiGraph
from sqllineage.core.graph.networkx import NetworkXGraphOperator
from sqllineage.core.graph_operator import GraphOperator


def to_cytoscape(go: Union[GraphOperator, DiGraph], compound=False) -> list[dict[str, dict[str, Any]]]:
    if isinstance(go, DiGraph):
        go = NetworkXGraphOperator(go)
    return go.to_cytoscape(compound=compound)
