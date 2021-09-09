#include <absl/flags/parse.h>

#include <cstdlib>

#include "server.h"

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);

  const char* const port_env = std::getenv("PORT");
  if (port_env == nullptr) {
    std::cerr << "PORT environment variable not set" << std::endl;
    return 1;
  }

  int port = 0;
  if (!absl::SimpleAtoi(port_env, &port)) {
    std::cerr << "Failed to parse PORT environment variable" << std::endl;
    return 1;
  }

  auto gcs_reader = seqr::MakeGcsReader();
  if (!gcs_reader.ok()) {
    std::cerr << "Failed to create GCS reader: " << gcs_reader.status()
              << std::endl;
    return 1;
  }

  auto grpc_server = seqr::CreateServer(port, **gcs_reader);
  if (!grpc_server.ok()) {
    std::cerr << "Failed to create server: " << grpc_server.status()
              << std::endl;
    return 1;
  }

  (*grpc_server)->server->Wait();

  return 0;
}
