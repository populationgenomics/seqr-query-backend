find_package(absl REQUIRED)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(GTest REQUIRED)
find_package(google_cloud_cpp_storage REQUIRED)
find_package(Arrow REQUIRED)
find_package(ArrowDataset REQUIRED)

find_library(TCMALLOC_LIB NAMES tcmalloc)
if(TCMALLOC_LIB)
   message("Found TCMALLOC_LIB: ${TCMALLOC_LIB}")
else()
   message(FATAL_ERROR "TCMALLOC_LIB library not found")
endif()


add_compile_options(-Wall -Werror)

add_executable(seqr_query_backend
    main.cc
)

target_link_libraries(seqr_query_backend PRIVATE
    ${TCMALLOC_LIB}
    absl::flags_parse
    server
)

add_library(server
    server.cc
    url_reader.cc
)

target_link_libraries(server PRIVATE
    absl::base
    absl::flags
    absl::status
    absl::statusor
    absl::strings
    arrow_shared
    arrow_dataset_shared
    gRPC::grpc++_reflection
    google-cloud-cpp::storage
    proto
    string_list_contains_any
)

add_library(gtest_main_with_flags
    gtest_main_with_flags.cc
)

target_link_libraries(gtest_main_with_flags PRIVATE
    gtest
    absl::flags_parse
)

add_executable(server_test
    server_test.cc
)

target_link_libraries(server_test PRIVATE
    ${TCMALLOC_LIB}
    gtest
    gtest_main_with_flags
    proto
    server
)

add_test(NAME server_test COMMAND server_test WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_library(string_list_contains_any
    string_list_contains_any.cc
)

target_link_libraries(string_list_contains_any PRIVATE
    absl::flat_hash_set
    arrow_shared
)

add_executable(string_list_contains_any_test
    string_list_contains_any_test.cc
)

target_link_libraries(string_list_contains_any_test PRIVATE
    ${TCMALLOC_LIB}
    gtest
    gtest_main_with_flags
    string_list_contains_any
)

add_test(NAME string_list_contains_any_test COMMAND string_list_contains_any_test WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
