#include "string_list_contains_any.h"

#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/compute/exec.h>
#include <arrow/testing/gtest_util.h>
#include <gtest/gtest.h>

namespace seqr {
namespace cp = arrow::compute;

void CheckStringListContainsAny(
    const std::vector<std::string>& lookup_values,
    const std::vector<std::vector<std::string>>& string_values,
    const std::vector<bool>& list_validity,
    const std::vector<std::vector<bool>>& string_validity,
    const std::vector<bool>& expected_values) {
  ASSERT_EQ(string_values.size(), list_validity.size());
  ASSERT_EQ(string_values.size(), string_validity.size());
  ASSERT_EQ(string_values.size(), expected_values.size());

  for (size_t i = 0; i < string_values.size(); ++i) {
    ASSERT_EQ(string_values[i].size(), string_validity[i].size());
  }

  auto* const memory_pool = arrow::default_memory_pool();
  arrow::ListBuilder list_builder(
      memory_pool, std::make_shared<arrow::StringBuilder>(memory_pool));
  arrow::StringBuilder& string_builder =
      static_cast<arrow::StringBuilder&>(*list_builder.value_builder());

  for (size_t i = 0; i < string_values.size(); ++i) {
    if (list_validity[i]) {
      ASSERT_OK(list_builder.Append());
    } else {
      ASSERT_OK(list_builder.AppendNull());
    }

    for (size_t j = 0; j < string_values[i].size(); ++j) {
      if (string_validity[i][j]) {
        ASSERT_OK(string_builder.Append(string_values[i][j]));
      } else {
        ASSERT_OK(string_builder.AppendNull());
      }
    }
  }

  std::shared_ptr<arrow::ListArray> input;
  ASSERT_OK(list_builder.Finish(&input));

  // Build the SetLookupOptions (values to search for).
  arrow::StringBuilder value_set_builder(memory_pool);
  for (const auto& val : lookup_values) {
    ASSERT_OK(value_set_builder.Append(val));
  }
  std::shared_ptr<arrow::StringArray> value_set;
  ASSERT_OK(value_set_builder.Finish(&value_set));
  const cp::SetLookupOptions options{value_set, false};

  // Don't clobber the global registry.
  const auto registry = cp::FunctionRegistry::Make();
  ASSERT_OK(RegisterStringListContainsAny(registry.get()));

  // Execute the function.
  cp::ExecContext ctx(memory_pool, nullptr, registry.get());
  auto result =
      cp::CallFunction("string_list_contains_any", {input}, &options, &ctx);
  ASSERT_OK(result);

  // Compare with the expected result.
  arrow::BooleanBuilder expected_builder(memory_pool);
  for (const auto& val : expected_values) {
    ASSERT_OK(expected_builder.Append(val));
  }
  std::shared_ptr<arrow::BooleanArray> expected;
  ASSERT_OK(expected_builder.Finish(&expected));
  ASSERT_EQ(*result, *expected);
}

TEST(TestStringListContainsAny, OneLookupValues) {
  // One lookup value triggers the fast path.
  const std::vector<std::string> lookup_values{"s02"};

  const std::vector<std::vector<std::string>> string_values{
      {"s01", "s02", "s03"},           // true: "s02"
      {},                              // false
      {},                              // false
      {"s02", "s01", "s01", "s02"},    // true: "s02"
      {"s02", "s01", "s01", "s02"},    // false: "s02", but string value invalid
      {"s02"},                         // true: "s02"
      {"s03", "s04", "s05"},           // false
      {"s01"},                         // false
      {"s02"},                         // false: "s02", but list value invalid
      {},                              // false
      {"s01", "", "", "s03"},          // false
      {"s12", "s42", "s02", "s5784"},  // true: "s02"
  };

  const std::vector<bool> list_validity{true, true, false, true,  true, true,
                                        true, true, false, false, true, true};

  const std::vector<std::vector<bool>> string_validity{
      {true, true, true},
      {},
      {},
      {true, true, true, true},
      {false, true, true, false},
      {true},
      {true, true, true},
      {true},
      {true},
      {},
      {true, true, true, true},
      {true, true, true, true}};

  const std::vector<bool> expected_values{true,  false, false, true,
                                          false, true,  false, false,
                                          false, false, false, true};

  CheckStringListContainsAny(lookup_values, string_values, list_validity,
                             string_validity, expected_values);
}

TEST(TestStringListContainsAny, TwoLookupValues) {
  const std::vector<std::string> lookup_values{"s02", "s04"};

  const std::vector<std::vector<std::string>> string_values{
      {"s01", "s02", "s03"},           // true: "s02"
      {},                              // false
      {},                              // false
      {"s02", "s01", "s01", "s02"},    // true: "s02"
      {"s02", "s01", "s01", "s02"},    // false: "s02", but string value invalid
      {"s02"},                         // true: "s02"
      {"s03", "s04", "s05"},           // true: "s04"
      {"s01"},                         // false
      {"s02"},                         // false: "s02", but list value invalid
      {},                              // false
      {"s01", "", "", "s03"},          // false
      {"s12", "s42", "s02", "s5784"},  // true: "s02"
  };

  const std::vector<bool> list_validity{true, true, false, true,  true, true,
                                        true, true, false, false, true, true};

  const std::vector<std::vector<bool>> string_validity{
      {true, true, true},
      {},
      {},
      {true, true, true, true},
      {false, true, true, false},
      {true},
      {true, true, true},
      {true},
      {true},
      {},
      {true, true, true, true},
      {true, true, true, true}};

  const std::vector<bool> expected_values{true,  false, false, true,
                                          false, true,  true,  false,
                                          false, false, false, true};

  CheckStringListContainsAny(lookup_values, string_values, list_validity,
                             string_validity, expected_values);
}

}  // namespace seqr
