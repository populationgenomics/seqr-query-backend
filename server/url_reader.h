#pragma once

#include <absl/status/statusor.h>

#include <memory>
#include <string_view>
#include <vector>

namespace seqr {

class UrlReader {
 public:
  virtual ~UrlReader() = default;

  virtual absl::StatusOr<std::vector<char>> Read(
      std::string_view url) const = 0;
};

// Reads from a local file system.
absl::StatusOr<std::unique_ptr<UrlReader>> MakeLocalFileReader();

// Reads from Google Cloud Storage.
absl::StatusOr<std::unique_ptr<UrlReader>> MakeGcsReader();

}  // namespace seqr
