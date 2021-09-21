from typing import Dict, Optional, List

from enum import Enum
from abc import abstractmethod, ABC
from unittest import TestCase
from types import SimpleNamespace
from textwrap import indent


MAX_ROWS = 10000
INDENT = "  "


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
        return f"""\
literal {{
{INDENT}{self.type}: {self.literal}
}}"""


class Call(Expression):
    def __init__(
        self, function_name: str, arguments: List[Expression], options: List[str] = None
    ):
        self.function_name = function_name
        self.arguments = arguments
        self.options = options or []

    def to_protobuff(self) -> str:

        if self.options:
            raise NotImplementedError(self.to_protobuff)

        _str_arguments = "\n".join(
            f"""\
arguments {{
{indent(a.to_protobuff(), INDENT)}
}}"""
            for a in self.arguments
        )

        return f"""\
call {{
{INDENT}function_name: "{self.function_name}"

{indent(_str_arguments, INDENT)}
}}"""


def main_input(es: Dict):
    est = SimpleNamespace(**es)

    sort = est.sort
    skip = getattr(est, "from")
    size = est.size
    fields = est._source

    _str_arrow_urls = ""
    _str_columns = "\n".join(f'projection_columns: "{col}"' for col in fields)

    _str_where = unwrap_query(est.query)

    _query = f"""\
{_str_arrow_urls}
{_str_columns}
filter_expression {{
{indent(_str_where.to_protobuff(), INDENT)}
}}

max_rows: {MAX_ROWS}
"""
    print(_query)
    return _query




def unwrap_query(query) -> Expression:
    if "bool" not in query or len(query) > 1:
        invalid_keys = ", ".join(q for q in query if q != "bool")
        raise NotImplemented(f"Can't support invalid keys: {invalid_keys}")

    return handle_generic_query(query["bool"])


def b(el: str):
    return f"({el})"


def handle_generic_query(query):
    if isinstance(query, list):
        raise ValueError(f"Can't handle list {query}")

    if isinstance(query, (str, int, float, bool)):
        raise ValueError(f"Expected dictionary, received: {query}")

    if "bool" in query:
        return handle_generic_query(query["bool"])

    terms = []

    keys = ", ".join(query.keys())

    if "should" in query:
        should = query["should"]
        inner = map(handle_generic_query, should)
        if len(should) > 1:
            terms.append(" OR ".join(map(b, inner)))
        else:
            terms.append(next(inner))
    if "must_not" in query:
        must_not = query["must_not"]
        inner = map(handle_generic_query, must_not)
        if len(must_not) > 1:
            inner_str = " AND ".join(inner)
        else:
            inner_str = next(inner)
        terms.append(f"NOT {inner_str}")
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
        return " AND ".join(map(b, terms))

    # raise NotImplementedError(f'Unhandled keys {keys}')


def handle_range(query):
    statements = []
    for field, inner in query.items():
        op_map = {
            "lt": "<",
            "lte": "<=",
            "gt": ">",
            "gte": ">=",
        }
        for op in op_map:
            if op in inner:
                statements.append(f"{field} {op_map[op]} {inner[op]}")

    if len(statements) > 1:
        return " AND ".join(map(b, statements))
    return statements[0]


def handle_exists(exists):
    field = exists["field"]
    return f"{field} IS NOT NULL"


def quote_value(value):
    if isinstance(value, str):
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"
    return value


def handle_terms(field, value):
    if isinstance(value, list):
        prepped_values = ", ".join(map(quote_value, value))
        return f"{field} in ({prepped_values})"

    return f"{field}={quote_value(value)}"


def handle_match(field, value):
    return f"{field} like '%{value}%'"


class ProtobuffStrTests(TestCase):
    def test_example_query(self):
        expression = Call(
            "less",
            arguments=[Column("gnomad_exomes_AF"), Literal(0.0001, "float_value")],
        )
        val = expression.to_protobuff()
        print(val)


if __name__ == "__main__":
    ProtobuffStrTests().test_example_query()
