cmake_minimum_required(VERSION 3.13)
project(arrow-in-mem LANGUAGES CXX)

enable_testing()

add_subdirectory(server)
add_subdirectory(proto)

