"""Parse a SKOS ConceptScheme, explore it and generate enums for schemas.

Assumptions:
* ConceptScheme collects Concepts via hasTopConcept
* Concepts have 0-1 broader Concept and 0-n narrower Concept
* Concepts define a prefLabel
* Concepts have a unique IRI (ideally resolving to the concept sub-graph)

All of this applies to e.g. https://data.nist.gov/od/dm/nmrr/vocab
"""
from __future__ import annotations

from enum import Enum
from typing import List, Optional, Tuple, Type, cast

import rdflib
from rdflib.namespace import SKOS

from ..util import pythonize_name
from .lib import GraphNode, RDFParser


class SemanticEnum(Enum):
    """Enum subclass for Enums generated from a semantic taxonomy."""

    __self__term__: Tuple[str, str]


class Concept(RDFParser):
    """A concept is a node in a taxonomy defining a term."""

    _depth: int = 0
    """Depth of the concept in the taxonomy forest."""

    id: str
    """ID of the concept (should be IRI resolving to concept definition)."""

    prefLabel: str
    """Canonical name of the concept."""

    broader: Optional[Concept]
    """Unique more general concept in the taxonomy (unless it is a root)."""

    narrower: List[Concept]
    """A list of more specific sub-concepts."""

    def __eq__(self, other):
        """Return whether two concept objects are equal.

        For our purposes, concepts are the same if they come from the same graph
        and refer to the same IRI.
        """
        return (
            isinstance(other, Concept)
            and self.__wrapped__.graph == other.__wrapped__.graph
            and self.id == other.id
        )

    def new_subconcept(self, node: GraphNode):
        ret = Concept(node)
        ret._depth = self._depth + 1
        return ret

    def new_superconcept(self, node: GraphNode):
        ret = Concept(node)
        ret._depth = self._depth - 1
        return ret

    # ----

    def parse_id(self, node) -> str:
        assert isinstance(node, rdflib.URIRef)
        return node.toPython()

    def parse_prefLabel(self, node: GraphNode) -> str:
        val = node.object(SKOS.prefLabel)
        assert val.is_literal()
        return val.value

    def parse_broader(self, node: GraphNode) -> Optional[Concept]:
        if v := node.object(SKOS.broader):
            return self.new_superconcept(v)
        return None

    def parse_narrower(self, node: GraphNode) -> List[Concept]:
        q = node.objects(SKOS.narrower)
        return list(map(self.new_subconcept, q))

    # ----

    def pretty_print(
        self, *, max_depth=None, indent: Optional[int] = None, indent_unit: str = "\t"
    ):
        indent = indent or 0
        line = f"{indent*indent_unit}{self.prefLabel} -> {self.id}"
        lines = [line]

        if max_depth is None or max_depth > 0:
            max_depth_next = max_depth - 1 if max_depth else None
            lines += list(
                map(
                    lambda x: x.pretty_print(
                        indent=indent + 1, max_depth=max_depth_next
                    ),
                    self.narrower,
                )
            )

        return "\n".join(lines)

    def __str__(self):
        return self.pretty_print()

    # ----

    @property
    def term(self):
        """Return (ID, string) pair for this concept."""
        return (self.id, pythonize_name(self.prefLabel))

    def sub_terms(self, *, deep: bool = False):
        """Return dict of subconcepts (recursively, if desired)."""
        ret = dict(map(lambda x: x.term, self.narrower))
        if deep:
            _, pyname = self.term
            ret.update(
                dict(
                    t
                    for dct in map(lambda x: x.sub_terms(deep=deep), self.narrower)
                    for t in dct.items()
                )
            )
        return ret

    def to_enum(self, deep: bool = False) -> Type[SemanticEnum]:
        """Return Enum with immediate child concepts as possible values."""
        if deep:  # pragma: no cover
            # TODO: think how to combine the enums in the best way
            raise NotImplementedError

        ts = self.sub_terms(deep=deep)
        assert len(ts) == len(
            set(ts.values())
        )  # expect that human values are also unique
        ret = cast(
            Type[SemanticEnum],
            SemanticEnum(  # type: ignore
                f"{self.term[1].capitalize()}_Enum", {v: k for k, v in ts.items()}
            ),
        )
        # useful information for "deep" mode:
        ret.__self_term__ = self.term  # type: ignore
        return ret


class ConceptScheme(RDFParser):
    """A concept scheme points to the roots of a taxonomy forest.

    The the top level concepts are assumed to be unrelated
    (otherwise they should be united by the broader super-concept).

    For this reason, you cannot generate an enum on this level.
    """

    id: str
    hasTopConcept: List[Concept]

    def parse_id(self, node):
        assert isinstance(node, rdflib.URIRef)
        return node.toPython()

    def parse_hasTopConcept(self, node: GraphNode):
        q = node.objects(SKOS.hasTopConcept)
        return list(map(Concept, q))

    # ----

    def pretty_print(self, **kwargs):
        return "\n".join(map(lambda x: x.pretty_print(**kwargs), self.hasTopConcept))

    def __str__(self):
        return self.pretty_print()
