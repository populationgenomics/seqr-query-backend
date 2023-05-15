from typing import Dict

from types import SimpleNamespace


def main_input(es: Dict):
    est = SimpleNamespace(**es)

    sort = est.sort
    skip = getattr(est, "from")
    size = est.size
    fields = est._source

    _str_fields = "*"  # '", ".join(fields)
    _str_sortby = ""
    _str_skip = ""
    _str_limit = ""
    if sort:
        _str_sortby = f'ORDER BY {", ".join(sort)}'

    if skip:
        _str_skip = f"SKIP {skip}"
    if size is not None:
        _str_limit = f"LIMIT {size}"

    _str_where = unwrap_query(est.query)

    _query = f"""
SELECT {_str_fields} 
FROM table 
WHERE {_str_where}
{_str_sortby}
{_str_skip} {_str_limit}
"""
    print(_query)


def unwrap_query(query):
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