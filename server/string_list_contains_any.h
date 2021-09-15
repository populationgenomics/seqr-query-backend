#pragma once

#include <arrow/compute/registry.h>
#include <arrow/status.h>

namespace seqr {

// Call this function once at startup time to register the Arrow compute
// function "string_list_contains_any".
arrow::Status RegisterStringListContainsAny(
    arrow::compute::FunctionRegistry* registry);

}  // namespace seqr

