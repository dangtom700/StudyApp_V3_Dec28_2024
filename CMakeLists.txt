cmake_minimum_required(VERSION 3.20)
project(retrieval_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(SQLite3 REQUIRED)

add_executable(retrieval_engine
    src/cpp/main.cpp
    src/cpp/relational_distance.cpp
    src/cpp/tfidf.cpp
    src/cpp/comparison.cpp
    src/cpp/prompt_ranking.cpp
)
target_include_directories(retrieval_engine PRIVATE src/cpp)
target_link_libraries(retrieval_engine PRIVATE SQLite3::SQLite3)
