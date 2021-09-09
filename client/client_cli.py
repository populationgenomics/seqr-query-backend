#!/usr/bin/env python3

import click
import grpc
import math
import seqr_query_service_pb2
import seqr_query_service_pb2_grpc
import time
import pyarrow


@click.command()
@click.option(
    '--query_text_proto_file',
    required=True,
    help='File name to a QueryRequest protobuf in text format (not binary!).',
)
def main(query_text_proto_file):
    channel = grpc.insecure_channel('localhost:8080')
    stub = seqr_query_service_pb2_grpc.QueryServiceStub(channel)

    with open(query_text_proto_file, 'rt') as text_proto:
        import google.protobuf.text_format

        request = google.protobuf.text_format.Parse(
            text_proto.read(), seqr_query_service_pb2.QueryRequest()
        )

    response = stub.Query(request)

    print(f'Number of rows: {response.num_rows}')
    print(f'Serialized size: {len(response.record_batches)} bytes')

    start_time = time.time()
    buffer = pyarrow.py_buffer(response.record_batches)
    table = pyarrow.ipc.RecordBatchFileReader(buffer).read_all()
    end_time = time.time()
    print(f'Table deserialization took {math.ceil(1000 * (end_time - start_time))}ms')

    assert table.num_rows == response.num_rows


if __name__ == '__main__':
    main()

