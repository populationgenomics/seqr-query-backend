#include "server.h"

#include <absl/base/thread_annotations.h>
#include <absl/flags/flag.h>
#include <absl/status/statusor.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/blocking_counter.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>
#include <arrow/array/builder_binary.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/function.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/scanner.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/options.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include <cassert>
#include <cstddef>
#include <functional>
#include <iostream>
#include <optional>
#include <queue>
#include <string_view>
#include <thread>  // NOLINT(build/c++11)
#include <vector>

#include "seqr_query_service.grpc.pb.h"
#include "string_list_contains_any.h"

ABSL_FLAG(int, num_threads, 16,
          "The number of thread pool workers. This implicitly puts a limit on "
          "the amount of memory that's required, which is important for Cloud "
          "Run deployments that only have 8 GB of RAM.");

namespace seqr {
namespace {

absl::Status MaxRowsExceededError(const size_t max_rows) {
  return absl::CancelledError(
      absl::StrCat("More than ", max_rows,
                   " rows matched; please use a more restrictive search"));
}

// Adapted from the Abseil thread pool.
class ThreadPool {
 public:
  explicit ThreadPool(const int num_threads) {
    assert(num_threads > 0);
    for (int i = 0; i < num_threads; ++i) {
      threads_.push_back(std::thread(&ThreadPool::WorkLoop, this));
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  ~ThreadPool() {
    {
      absl::MutexLock l(&mu_);
      for (size_t i = 0; i < threads_.size(); i++) {
        queue_.push(nullptr);  // Shutdown signal.
      }
    }
    for (auto& t : threads_) {
      t.join();
    }
  }

  // Schedule a function to be run on a ThreadPool thread immediately.
  void Schedule(std::function<void()> func) {
    assert(func != nullptr);
    absl::MutexLock l(&mu_);
    queue_.push(std::move(func));
  }

 private:
  bool WorkAvailable() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_) {
    return !queue_.empty();
  }

  void WorkLoop() {
    while (true) {
      std::function<void()> func;
      {
        absl::MutexLock l(&mu_);
        mu_.Await(absl::Condition(this, &ThreadPool::WorkAvailable));
        func = std::move(queue_.front());
        queue_.pop();
      }
      if (func == nullptr) {  // Shutdown signal.
        break;
      }
      func();
    }
  }

  absl::Mutex mu_;
  std::queue<std::function<void()>> queue_ ABSL_GUARDED_BY(mu_);
  std::vector<std::thread> threads_;
};

// Returns an Arrow compute expression from the protobuf specification.
absl::StatusOr<arrow::compute::Expression> BuildFilterExpression(
    const seqr::QueryRequest::Expression& filter_expression) {
  namespace cp = arrow::compute;
  switch (filter_expression.type_case()) {
    case seqr::QueryRequest::Expression::TYPE_NOT_SET:
      return absl::InvalidArgumentError("Expression type not set");

    case seqr::QueryRequest::Expression::kColumn:
      return cp::field_ref(filter_expression.column());

    case seqr::QueryRequest::Expression::kLiteral: {
      const auto& literal = filter_expression.literal();
      switch (literal.type_case()) {
        case seqr::QueryRequest::Expression::Literal::TYPE_NOT_SET:
          return absl::InvalidArgumentError("Literal type not set");
        case seqr::QueryRequest::Expression::Literal::kBoolValue:
          return cp::literal(literal.bool_value());
        case seqr::QueryRequest::Expression::Literal::kInt32Value:
          return cp::literal(literal.int32_value());
        case seqr::QueryRequest::Expression::Literal::kInt64Value:
          return cp::literal(literal.int64_value());
        case seqr::QueryRequest::Expression::Literal::kFloatValue:
          return cp::literal(literal.float_value());
        case seqr::QueryRequest::Expression::Literal::kDoubleValue:
          return cp::literal(literal.double_value());
        case seqr::QueryRequest::Expression::Literal::kStringValue:
          return cp::literal(literal.string_value());
      }
    }

    case seqr::QueryRequest::Expression::kCall: {
      const auto& call = filter_expression.call();
      std::vector<cp::Expression> arguments;
      arguments.reserve(call.arguments_size());
      for (const auto& argument : call.arguments()) {
        auto expression = BuildFilterExpression(argument);
        if (!expression.ok()) {
          return expression.status();
        }
        arguments.push_back(*std::move(expression));
      }

      std::shared_ptr<cp::FunctionOptions> options;
      switch (call.options_case()) {
        case seqr::QueryRequest::Expression::Call::OPTIONS_NOT_SET:
          break;
        case seqr::QueryRequest::Expression::Call::kSetLookupOptions: {
          arrow::StringBuilder builder;
          for (const auto& str : call.set_lookup_options().values()) {
            if (const auto status = builder.Append(str); !status.ok()) {
              return absl::InvalidArgumentError(absl::StrCat(
                  "Failed to append string value: ", status.message()));
            }
          }
          std::shared_ptr<arrow::StringArray> value_set;
          if (const auto status = builder.Finish(&value_set); !status.ok()) {
            return absl::InvalidArgumentError(absl::StrCat(
                "Failed to build string array: ", status.message()));
          }
          options =
              std::make_shared<cp::SetLookupOptions>(value_set,
                                                     /* skip_nulls */ true);
        }
      }

      return cp::call(call.function_name(), std::move(arguments), options);
    }
  }

  return absl::InternalError(
      absl::StrCat("Unhandled case: ", filter_expression.type_case()));
}

struct ScannerOptions {
  std::vector<std::string> projection_columns;
  arrow::compute::Expression filter_expression;
  size_t max_rows = 0;
};

absl::StatusOr<ScannerOptions> BuildScannerOptions(
    const seqr::QueryRequest& request) {
  auto filter_expression = BuildFilterExpression(request.filter_expression());
  if (!filter_expression.ok()) {
    return filter_expression.status();
  }

  if (request.max_rows() <= 0) {
    return absl::InvalidArgumentError(
        absl::StrCat("Invalid max_rows value of ", request.max_rows()));
  }

  return ScannerOptions{{request.projection_columns().begin(),
                         request.projection_columns().end()},
                        *std::move(filter_expression),
                        static_cast<size_t>(request.max_rows())};
}

absl::StatusOr<arrow::RecordBatchVector> ProcessArrowUrl(
    const UrlReader& url_reader, const std::string_view url,
    const ScannerOptions& scanner_options,
    std::atomic<size_t>* const num_rows) {
  // Early cancellation.
  if (*num_rows > scanner_options.max_rows) {
    return MaxRowsExceededError(scanner_options.max_rows);
  }

  const auto data = url_reader.Read(url);
  if (!data.ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to read ", url, ": ", data.status().message()));
  }

  arrow::ipc::IpcReadOptions ipc_read_options;
  // We parallelize over URLs already, no need for nested parallelism.
  ipc_read_options.use_threads = false;

  arrow::io::BufferReader buffer_reader{{data->data(), data->size()}};
  auto record_batch_file_reader =
      arrow::ipc::RecordBatchFileReader::Open(&buffer_reader, ipc_read_options);
  if (!record_batch_file_reader.ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to open record batch reader for ", url, ": ",
                     record_batch_file_reader.status().ToString()));
  }

  const auto schema = (*record_batch_file_reader)->schema();

  const int num_record_batches =
      (*record_batch_file_reader)->num_record_batches();
  arrow::RecordBatchVector record_batches;
  record_batches.reserve(num_record_batches);
  for (int i = 0; i < num_record_batches; ++i) {
    auto record_batch = (*record_batch_file_reader)->ReadRecordBatch(i);
    if (!record_batch.ok()) {
      return absl::InvalidArgumentError(
          absl::StrCat("Failed to read record batch ", i, " for ", url, ": ",
                       record_batch.status().ToString()));
    }
    record_batches.push_back(std::move(*record_batch));
  }

  auto in_memory_dataset = std::make_shared<arrow::dataset::InMemoryDataset>(
      schema, std::move(record_batches));
  auto scanner_builder = in_memory_dataset->NewScan();
  if (!scanner_builder.ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to create scanner builder for ", url, ": ",
                     scanner_builder.status().ToString()));
  }

  if (const auto status =
          (*scanner_builder)->Project(scanner_options.projection_columns);
      !status.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Failed to set projection columns for ", url, ": ", status.ToString()));
  }

  if (const auto status =
          (*scanner_builder)->Filter(scanner_options.filter_expression);
      !status.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Failed to set scanner filter for ", url, ": ", status.ToString()));
  }

  // We parallelize over URLs already, no need for nested parallelism.
  if (const auto status = (*scanner_builder)->UseThreads(false); !status.ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to disable scanner threads for ", url, ": ",
                     status.ToString()));
  }

  const auto scanner = (*scanner_builder)->Finish();
  if (!scanner.ok()) {
    return absl::InvalidArgumentError(
        absl::StrCat("Failed to create scanner for ", url, ": ",
                     scanner.status().ToString()));
  }

  arrow::RecordBatchVector result;
  if (const auto status = (*scanner)->Scan(
          [&result,
           num_rows](arrow::dataset::TaggedRecordBatch tagged_record_batch) {
            auto& record_batch = tagged_record_batch.record_batch;
            if (record_batch->num_rows() > 0) {
              *num_rows += record_batch->num_rows();
              result.push_back(std::move(record_batch));
            }
            return arrow::Status::OK();
          });
      !status.ok()) {
    return absl::InvalidArgumentError(absl::StrCat(
        "Failed to run scanner on ", url, ": ", status.ToString()));
  }

  return result;
}

class QueryServiceImpl final : public seqr::QueryService::Service {
 public:
  QueryServiceImpl(const UrlReader& url_reader) : url_reader_(url_reader) {}

 private:
  grpc::Status Query(grpc::ServerContext* const context,
                     const seqr::QueryRequest* const request,
                     seqr::QueryResponse* const response) override {
    // Build options that are shared between worker threads.
    const auto scanner_options = BuildScannerOptions(*request);
    if (!scanner_options.ok()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          absl::StrCat("Failed to build scanner options: ",
                                       scanner_options.status().message()));
    }

    // Process the URLs in parallel.
    const size_t num_arrow_urls = request->arrow_urls_size();
    std::vector<absl::StatusOr<arrow::RecordBatchVector>> partial_results(
        num_arrow_urls);
    std::atomic<size_t> num_rows = 0;  // Number of filtered rows across URLs.
    absl::BlockingCounter blocking_counter(num_arrow_urls);
    for (size_t i = 0; i < num_arrow_urls; ++i) {
      thread_pool_.Schedule([&url_reader = url_reader_,
                             &url = request->arrow_urls(i),
                             &result = partial_results[i], &scanner_options,
                             &num_rows, &blocking_counter] {
        result = ProcessArrowUrl(url_reader, url, *scanner_options, &num_rows);
        blocking_counter.DecrementCount();
      });
    }

    blocking_counter.Wait();

    if (num_rows > scanner_options->max_rows) {
      return grpc::Status(
          grpc::StatusCode::CANCELLED,
          std::string(
              MaxRowsExceededError(scanner_options->max_rows).message()));
    }

    // Serialize the result record batches to the response proto.
    std::shared_ptr<arrow::Schema> schema;
    for (const auto& result : partial_results) {
      if (!result.ok()) {
        return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                            std::string(result.status().message()));
      }
      if (!result->empty()) {
        schema = result->front()->schema();
        break;
      }
    }

    if (schema == nullptr) {  // No results found.
      return grpc::Status::OK;
    }

    auto buffer_output_stream = arrow::io::BufferOutputStream::Create();
    if (!buffer_output_stream.ok()) {
      return grpc::Status(
          grpc::StatusCode::INVALID_ARGUMENT,
          absl::StrCat("Failed to create buffer output stream: ",
                       buffer_output_stream.status().message()));
    }

    auto file_writer =
        arrow::ipc::MakeFileWriter(*buffer_output_stream, schema);
    if (!file_writer.ok()) {
      return grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                          absl::StrCat("Failed to create file writer: ",
                                       file_writer.status().message()));
    }

    for (const auto& result : partial_results) {
      for (const auto& record_batch : *result) {
        if (const auto status = (*file_writer)->WriteRecordBatch(*record_batch);
            !status.ok()) {
          return grpc::Status(
              grpc::StatusCode::INVALID_ARGUMENT,
              absl::StrCat("Failed to write record batch: ", status.message()));
        }
      }
    }

    if (const auto status = (*file_writer)->Close(); !status.ok()) {
      return grpc::Status(
          grpc::StatusCode::INVALID_ARGUMENT,
          absl::StrCat("Failed to close file writer: ", status.message()));
    }

    const auto buffer = (*buffer_output_stream)->Finish();
    if (!buffer.ok()) {
      return grpc::Status(
          grpc::StatusCode::INVALID_ARGUMENT,
          absl::StrCat("Failed to finish buffer output stream: ",
                       buffer.status().message()));
    }

    response->set_num_rows(num_rows);
    response->set_record_batches((*buffer)->ToString());

    return grpc::Status::OK;
  }

  ThreadPool thread_pool_{absl::GetFlag(FLAGS_num_threads)};
  const UrlReader& url_reader_;
};

absl::Status RegisterArrowComputeFunctions() {
  auto* const registry = arrow::compute::GetFunctionRegistry();
  if (const auto status = RegisterStringListContainsAny(registry);
      !status.ok()) {
    return absl::InternalError(absl::StrCat(
        "Error calling RegisterStringListContainsAny: ", status.message()));
  }
  return absl::OkStatus();
}

class GrpcServerImpl : public GrpcServer {
 public:
  GrpcServerImpl(const UrlReader& url_reader)
      : query_service_impl(url_reader) {}

  // The server does not take ownership of the services, which is why we keep
  // the service alive here.
  QueryServiceImpl query_service_impl;
};

}  // namespace

absl::StatusOr<std::unique_ptr<GrpcServer>> CreateServer(
    const int port, const UrlReader& url_reader) {
  if (const auto status = seqr::RegisterArrowComputeFunctions(); !status.ok()) {
    return absl::InternalError(absl::StrCat(
        "Failed to register Arrow compute functions: ", status.message()));
  }

  grpc::EnableDefaultHealthCheckService(true);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();

  const std::string server_address = absl::StrCat("[::]:", port);
  std::cout << "Starting server on " << server_address << std::endl;
  grpc::ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

  auto result = std::make_unique<GrpcServerImpl>(url_reader);
  builder.RegisterService(&result->query_service_impl);
  result->server = builder.BuildAndStart();
  return result;
}

}  // namespace seqr
