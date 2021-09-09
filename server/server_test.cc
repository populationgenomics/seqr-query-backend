#include "server.h"

#include <absl/container/flat_hash_set.h>
#include <absl/strings/str_cat.h>
#include <arrow/array.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/table.h>
#include <arrow/util/string_view.h>
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

  constexpr size_t kNumExpectedRows = 6;
  EXPECT_EQ(response.num_rows(), kNumExpectedRows);

  auto record_batch_file_reader = arrow::ipc::RecordBatchFileReader::Open(
      std::make_shared<arrow::io::BufferReader>(response.record_batches()));
  ASSERT_TRUE(record_batch_file_reader.ok())
      << record_batch_file_reader.status();
  const size_t num_record_batches =
      (*record_batch_file_reader)->num_record_batches();
  arrow::RecordBatchVector record_batches;
  record_batches.reserve(num_record_batches);
  for (size_t i = 0; i < num_record_batches; ++i) {
    auto record_batch = (*record_batch_file_reader)->ReadRecordBatch(i);
    ASSERT_TRUE(record_batch.ok()) << record_batch.status();
    record_batches.push_back(*record_batch);
  }

  const auto table = arrow::Table::FromRecordBatches(record_batches);
  ASSERT_TRUE(table.ok()) << table.status();

  const auto xpos_col = (*table)->GetColumnByName("xpos");
  ASSERT_TRUE(xpos_col != nullptr);
  std::vector<int64_t> xpos_vals;
  xpos_vals.reserve(kNumExpectedRows);
  for (int i = 0; i < xpos_col->num_chunks(); ++i) {
    const auto chunk = xpos_col->chunk(i);
    ASSERT_TRUE(chunk != nullptr);
    const auto int64_array = std::static_pointer_cast<arrow::Int64Array>(chunk);
    for (int j = 0; j < int64_array->length(); ++j) {
      ASSERT_FALSE(int64_array->IsNull(j));
      xpos_vals.push_back(int64_array->Value(j));
    }
  }
  ASSERT_EQ(xpos_vals.size(), kNumExpectedRows);

  const auto variant_id_col = (*table)->GetColumnByName("variantId");
  ASSERT_TRUE(variant_id_col != nullptr);
  std::vector<arrow::util::string_view> variant_id_vals;
  variant_id_vals.reserve(kNumExpectedRows);
  for (int i = 0; i < variant_id_col->num_chunks(); ++i) {
    const auto chunk = variant_id_col->chunk(i);
    ASSERT_TRUE(chunk != nullptr);
    const auto string_array =
        std::static_pointer_cast<arrow::StringArray>(chunk);
    for (int j = 0; j < string_array->length(); ++j) {
      ASSERT_FALSE(string_array->IsNull(j));
      variant_id_vals.push_back(string_array->Value(j));
    }
  }
  ASSERT_EQ(variant_id_vals.size(), kNumExpectedRows);

  absl::flat_hash_set<std::tuple<int64_t, arrow::util::string_view>> actual;
  actual.reserve(kNumExpectedRows);
  for (size_t i = 0; i < kNumExpectedRows; ++i) {
    actual.insert(std::make_pair(xpos_vals[i], variant_id_vals[i]));
  }

  // Compare with values validated using BigQuery.
  const absl::flat_hash_set<std::tuple<int64_t, arrow::util::string_view>>
      expected{{1001050069, "1-1050069-G-A"},  {1001054900, "1-1054900-C-T"},
               {1002024923, "1-2024923-G-A"},  {1002302812, "1-2302812-A-G"},
               {1011145001, "1-11145001-C-T"}, {1011241657, "1-11241657-A-G"}};

  EXPECT_EQ(actual, expected);
}

}  // namespace seqr
