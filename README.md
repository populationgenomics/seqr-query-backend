# seqr query backend

An experimental backend that could be an alternative to the Elasticsearch deployment that seqr currently uses.

Implements a simple [query API](proto/seqr_query_service.proto) using [gRPC](https://grpc.io/).

To convert seqr's annotated Hail tables to the [Apache Arrow](https://arrow.apache.org/) format that this backend uses, see the [`pipeline`](pipeline) directory.

## Docker stages

To reduce repeated image build times and reduce final image size, the build is split
into multiple stages:

- `base`: build dependencies
- `server`: compilation of the server application and tests
- `deploy`: stripped down image for deployment

There are separate [`prod` and `dev` deployments](.github/workflows/deploy.yaml), corresponding to the `main` and `dev` branches, respectively.

## Local testing

```bash
docker build --tag=seqr-query-backend .

docker run --init -it -e PORT=8080 -p 8080:8080 seqr-query-backend
```

In another terminal, run the client:

```bash
cd client

pip3 install -r requirements.txt

./client_cli.py --query_text_proto_file=example_query.textproto
```

For debug builds, run:

```bash
docker build --tag=seqr-query-backend-debug --build-arg=CMAKE_BUILD_TYPE=Debug --target=server .

docker run --privileged --init -it -e PORT=8080 -p 8080:8080 seqr-query-backend-debug
```

Within the container, run the server in `gdb`:

```bash
gdb /app/build/server/seqr_query_backend
```
