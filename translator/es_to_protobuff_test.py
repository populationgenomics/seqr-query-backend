from unittest import TestCase

from es_to_protobuff import *

ARROW_PARTITIONS = [
    "file:///data/part-00000-2d5ce851-c379-4eab-94ba-1e51f996109b-c000.zstd.arrow",
    "file:///data/part-00001-2d5ce851-c379-4eab-94ba-1e51f996109b-c000.zstd.arrow",
    "file:///data/part-00002-2d5ce851-c379-4eab-94ba-1e51f996109b-c000.zstd.arrow",
]


class ProtobuffStrTests(TestCase):
    def test_example_query(self):
        expression = Call(
            "less",
            arguments=[Column("gnomad_exomes_AF"), Literal(0.0001, "float_value")],
        )
        val = expression.to_output_query(
            columns=["xpos", "variantId"], arrow_urls=ARROW_PARTITIONS
        )

        print(val)

    def test_loaded_query(self):
        file = "translator/more-complex-query.json"
        with open(file) as f:
            d = json.load(f)
        print(main_input(d))

    def test_na12878_not_benign_query(self):
        file = "translator/query_na12878.json"
        with open(file) as f:
            d = json.load(f)
        print(main_input(d))
