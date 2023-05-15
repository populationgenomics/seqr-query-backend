from typing import Dict, Optional, List

import json
from enum import Enum
from abc import abstractmethod, ABC
from unittest import TestCase
from textwrap import indent


MAX_ROWS = 10000
INDENT = "  "

ARROW_PARTITIONS = [
    "file:///data/part-00000-2d5ce851-c379-4eab-94ba-1e51f996109b-c000.zstd.arrow",
    "file:///data/part-00001-2d5ce851-c379-4eab-94ba-1e51f996109b-c000.zstd.arrow",
    "file:///data/part-00002-2d5ce851-c379-4eab-94ba-1e51f996109b-c000.zstd.arrow",
]


class ExpressionType(Enum):
    COLUMN = 1
    LITERAL = 2
    CALL = 3


class Expression(ABC):
    def __init__(self, _type: ExpressionType) -> None:
        self.type = _type

    @abstractmethod
    def to_protobuff(self) -> str:
        pass

    def to_output_query(self, columns: List[str], arrow_urls: List[str]) -> str:

        headers = []
        # arrow links
        headers.extend(f'arrow_urls: "{url}"' for url in arrow_urls)

        # fields to select
        headers.extend(f'projection_columns: "{col}"' for col in columns)
        str_headers = "\n".join(headers)

        return f"""\
{str_headers}

filter_expression {{
{indent(self.to_protobuff(), INDENT)}
}}

max_rows: {MAX_ROWS}
"""


class Column(Expression):
    def __init__(self, name: str):
        super().__init__(ExpressionType.COLUMN)
        self.name = name

    def to_protobuff(self) -> str:
        return f'column: "{self.name}"'


class Literal(Expression):
    def __init__(self, literal: any, literal_type: Optional[str]):
        super().__init__(ExpressionType.LITERAL)
        self.literal = literal
        self.type = literal_type or self.infer_type(literal)

    @staticmethod
    def infer_type(literal: any):
        if isinstance(literal, str):
            return "string_value"
        if isinstance(literal, int):
            # 32 bit: https://stackoverflow.com/a/49049072
            if abs(literal) <= 0xFFFFFFFF:
                return "int32_value"
            return "int64_value"
        if isinstance(literal, float):
            # python 'float' is dependent on system, but is usually 64 bits
            # import sys,math; print(sys.float_info.mant_dig + math.ceil(math.log2(sys.float_info.max_10_exp - sys.float_info.min_10_exp)) + 1)
            return "double_value"
        if isinstance(literal, bool):
            return "bool_value"

    def to_protobuff(self) -> str:
        literal_value = self.literal
        if self.type == "string_value":
            escaped = literal_value.replace('"', '\\"')
            literal_value = f'"{escaped}"'

        return f"""\
literal {{
{INDENT}{self.type}: {literal_value}
}}"""


class Call(Expression):
    def __init__(
        self,
        function_name: str,
        arguments: List[Expression],
        options: Dict[str, Dict[str, str]] = None,
    ):
        if not isinstance(arguments, list):
            raise ValueError(
                f"Expected arguments to be a list, received: {type(arguments)}"
            )

        if options is not None and not isinstance(options, dict):
            raise ValueError(f"Expected options to be dict, received {type(options)}")

        self.function_name = function_name
        self.arguments = arguments
        self.options = options or {}

    def to_protobuff(self) -> str:

        _str_arguments = "\n".join(
            f"""\
arguments {{
{indent(a.to_protobuff(), INDENT)}
}}"""
            for a in self.arguments
        )

        internals = [f'function_name: "{self.function_name}"', _str_arguments]

        if self.options:
            for k, d in self.options.items():
                inner = "\n".join(f'{kk}: "{vv}"' for kk, vv in d.items())
                internals.append(f"{k} {{\n{indent(inner, INDENT)}\n}}")

        str_internals = indent("\n".join(internals), INDENT)
        return f"call {{\n{str_internals}\n}}"


def main_input(es: Dict) -> str:

    fields = es.get("_source")
    fields = ["xpos", "variantId"]

    expr = unwrap_query(es["query"])

    _query = expr.to_output_query(columns=fields, arrow_urls=ARROW_PARTITIONS)
    return _query


def unwrap_query(query) -> Expression:
    if "bool" not in query or len(query) > 1:
        invalid_keys = ", ".join(q for q in query if q != "bool")
        raise NotImplementedError(f"Can't support invalid keys: {invalid_keys}")

    return handle_generic_query(query["bool"])


def b(el: str):
    return f"({el})"


def split_into_calls_of_two(function_name: str, iterable: List[Expression]):
    if len(iterable) == 0:
        return None
    if len(iterable) == 1:
        return iterable[0]

    call = Call(function_name=function_name, arguments=iterable[-2:])
    for i in range(len(iterable) - 2, 0, -1):
        call = Call(function_name=function_name, arguments=[iterable[i], call])

    return call


def handle_generic_query(query):
    if isinstance(query, list):
        raise ValueError(f"Can't handle list {query}")

    if isinstance(query, (str, int, float, bool)):
        return Literal(literal=query, literal_type=None)

    if "bool" in query:
        return handle_generic_query(query["bool"])

    terms: List[Expression] = []
    if "field" in query:
        terms.append(Column(query["field"]))
    if "should" in query:
        should = query["should"]
        inner = list(map(handle_generic_query, should))
        unwrapped_should = split_into_calls_of_two("or", inner)
        if unwrapped_should:
            terms.append(unwrapped_should)
    if "must_not" in query:
        must_not = query["must_not"]
        unwrapped_must_not = split_into_calls_of_two(
            "and", list(map(handle_generic_query, must_not))
        )
        terms.append(Call("invert", arguments=[unwrapped_must_not]))
    if "filter" in query:
        filters = query["filter"]
        terms.extend(map(handle_generic_query, filters))

    if "must" in query:
        filters = query["must"]
        terms.extend(map(handle_generic_query, filters))

    if "range" in query:
        terms.append(handle_range(query["range"]))

    if "exists" in query:
        terms.append(handle_exists(query["exists"]))

    if "terms" in query:
        for field, value in query["terms"].items():
            terms.append(handle_terms(field, value))

    if "term" in query:
        for field, value in query["term"].items():
            terms.append(handle_terms(field, value))

    if "match" in query:
        for field, value in query["match"].items():
            terms.append(handle_match(field, value))

    if len(terms) == 0:
        raise ValueError("Not enough terms")
    if len(terms) == 1:
        return terms[0]
    if len(terms) > 1:
        return split_into_calls_of_two("and", terms)

    # raise NotImplementedError(f'Unhandled keys {keys}')


def handle_range(query) -> Expression:
    statements = []
    for field, inner in query.items():
        op_map = {
            "lt": "less",
            "lte": "less_equal",
            "gt": "greater",
            "gte": "greater_equal",
        }
        f = Column(name=field)
        for op in op_map:
            if op in inner:
                statements.append(
                    Call(
                        function_name=op_map[op],
                        arguments=[f, handle_generic_query(inner[op])],
                    )
                )
                # statements.append(f"{field} {op_map[op]} {inner[op]}")

    return split_into_calls_of_two("and", statements)


def handle_exists(exists) -> Expression:
    field = handle_generic_query(exists)
    return Call("is_valid", arguments=[field])


def quote_value(value):
    if isinstance(value, str):
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"
    return value


def handle_terms(field, value):
    if isinstance(value, list):

        prepped_values = ", ".join(map(quote_value, value))
        raise NotImplementedError(f"{field} in ({prepped_values})")

    return Call(
        "string_list_contains_any",
        arguments=[Column(name=field)],
        options={"set_lookup_options": {"values": value}},
    )
    # return f"{field}={quote_value(value)}"


def handle_match(field, value):
    # return f"{field} like '%{value}%'"
    # might be something simpler here
    return Call(
        "match_substring_regex",
        arguments=[handle_generic_query(field)],
        options=[f"pattern = .+{value}.+"],
    )
