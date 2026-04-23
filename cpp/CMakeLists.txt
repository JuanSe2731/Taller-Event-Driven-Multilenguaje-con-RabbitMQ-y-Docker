cmake_minimum_required(VERSION 3.16)
project(rabbitmq_cpp_delta CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(producer_delta producer_delta.cpp)
add_executable(consumer_delta consumer_delta.cpp)

find_library(RABBITMQ_LIB rabbitmq REQUIRED)
target_link_libraries(producer_delta PRIVATE ${RABBITMQ_LIB})
target_link_libraries(consumer_delta PRIVATE ${RABBITMQ_LIB})
