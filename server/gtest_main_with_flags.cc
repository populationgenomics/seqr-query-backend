#include <absl/flags/parse.h>
#include <gtest/gtest.h>

// Like gtest_main, but also parses Abseil flags.
int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  absl::ParseCommandLine(argc, argv);
  return RUN_ALL_TESTS();
}
