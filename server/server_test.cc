#include "server.h"

#include <absl/strings/str_cat.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <grpcpp/grpcpp.h>
#include <gtest/gtest.h>

#include <fstream>

#include "seqr_query_service.grpc.pb.h"

namespace seqr {

TEST(Server, EndToEnd) {
  constexpr int kPort = 12345;
  const auto local_file_reader = MakeLocalFileReader();
  ASSERT_TRUE(local_file_reader.ok());
  auto server = CreateServer(kPort, **local_file_reader);
  ASSERT_TRUE(server.ok()) << server.status();

  auto channel = grpc::CreateChannel(absl::StrCat("localhost:", kPort),
                                     grpc::InsecureChannelCredentials());
  auto stub = QueryService::NewStub(channel);
  ASSERT_TRUE(stub != nullptr);

  const char kQueryTextProtoFilename[] =
      "testdata/na12878_trio_query.textproto";
  std::ifstream ifs{kQueryTextProtoFilename};
  ASSERT_TRUE(ifs);
  google::protobuf::io::IstreamInputStream iis{&ifs};
  QueryRequest request;
  ASSERT_TRUE(google::protobuf::TextFormat::Parse(&iis, &request));

  grpc::ClientContext context;
  QueryResponse response;
  auto status = stub->Query(&context, request, &response);
  ASSERT_TRUE(status.ok()) << status.error_message();

  EXPECT_EQ(response.num_rows(), 6);

  // Expected results:
  // (1) 1001050069 1-1050069-G-A
  // (2) 1001054900 1-1054900-C-T
  // (3) 1002024923 1-2024923-G-A
  // (4) 1002302812 1-2302812-A-G
  // (5) 1011145001 1-11145001-C-T
  // (6) 1011241657 1-11241657-A-G

  // TODO(@lgruen): implement result table comparison
}

}  // namespace seqr
