#pragma once

#include <absl/status/statusor.h>
#include <grpcpp/grpcpp.h>

#include <memory>

#include "url_reader.h"

namespace seqr {

class GrpcServer {
 public:
  virtual ~GrpcServer() {
    if (server != nullptr) {
      server->Shutdown();
    }
  }

  std::unique_ptr<grpc::Server> server;
};

absl::StatusOr<std::unique_ptr<GrpcServer>> CreateServer(
    int port, const UrlReader& url_reader);

}  // namespace seqr

