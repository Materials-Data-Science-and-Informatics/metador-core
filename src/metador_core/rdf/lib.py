"""Wrappers and helpers for RDFlib."""
from typing import Any, Dict, Union

import rdflib
import wrapt
from rdflib.term import Identifier

# NOTE: could evaluate oxrdflib if rdflib turns out being too slow


def at_most_one(g):
    """Return at most one element from a generator.

    If there are more, will raise ValueError.
    """
    ret = next(g, None)
    if next(g, None) is None:
        return ret
    else:
        raise ValueError()


class GraphNode(wrapt.ObjectProxy):
    """Wrapper for rdflib helping to navigate entities in an `rdflib.Graph`."""

    _self_graph: rdflib.Graph
    obj: Identifier

    def __init__(self, graph: rdflib.Graph, obj: Identifier = None):
        super().__init__(obj)
        self._self_graph = graph

    @property
    def graph(self):
        return self._self_graph

    @property
    def node(self):
        return self.__wrapped__

    def wrap(self, obj: Identifier):
        return GraphNode(self.graph, obj)

    def is_literal(self):
        return isinstance(self.__wrapped__, rdflib.Literal)

    def is_uriref(self):
        return isinstance(self.__wrapped__, rdflib.URIRef)

    # ----

    def edges_in(self):
        return map(self.wrap, self.graph.predicates(object=self.node, unique=True))  # type: ignore

    def edges_out(self):
        return map(self.wrap, self.graph.predicates(subject=self.node, unique=True))  # type: ignore

    def objects(self, pred):
        return map(self.wrap, self.graph.objects(self.node, pred, unique=True))  # type: ignore

    def subjects(self, pred):
        return map(self.wrap, self.graph.subjects(pred, self.node, unique=True))  # type: ignore

    def subject(self, pred):
        try:
            return at_most_one(self.subjects(pred))
        except ValueError:
            msg = f"Expected to get exactly one match for: (*, {pred}, {self.obj})"
            raise ValueError(msg)

    def object(self, pred):
        try:
            return at_most_one(self.objects(pred))
        except ValueError:
            msg = f"Expected to get exactly one match for: ({self.obj}, {pred}, *)"
            raise ValueError(msg)


class RDFParser(wrapt.ObjectProxy):
    """Helper wrapper to access entity properties backed by an RDFlib graph.

    Ensures that queries are only performed when needed (more efficient).

    Use this e.g. to parse linked data with expected structure into your custom
    data structures, e.g. for use in schemas.
    """

    __wrapped__: GraphNode

    def __init__(self, node: GraphNode):
        super().__init__(node)
        self._self_parsed: Dict[str, Any] = {}

    def __getattr__(self, key: str):
        # special case: return the method
        if key.startswith("parse_"):
            return wrapt.ObjectProxy.__getattr__(self, key)

        # try to return the attribute
        if val := self._self_parsed.get(key):
            return val
        # need first to compute the attribute
        if pfunc := getattr(self, f"parse_{key}"):
            val = pfunc(self.__wrapped__)
            self._self_parsed[key] = val
            return val
        # no parser defined -> pass through
        return getattr(self.__wrapped__, key)

    @classmethod
    def from_graph(cls, g: rdflib.Graph, obj: Union[str, rdflib.URIRef]):
        return cls(GraphNode(g, rdflib.URIRef(obj)))
