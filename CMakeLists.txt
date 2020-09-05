cmake_minimum_required(VERSION 2.8.0)
project(test)

set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -O3 -g")

set(LOCAL_CACHE_INC "./local-cache")
set(THREAD_POOL_INC "./CTPL")
set(TBB_INC "/usr/local/include/tbb_U5")

include_directories(${LOCAL_CACHE_INC})
include_directories(${THREAD_POOL_INC})
include_directories(${TBB_INC})

set(TBB_LIB "/usr/local/lib/tbb_U5_lib")

link_directories(${TBB_LIB})

add_executable(test
		test.cpp
		concurrent_warmup.cpp
		)

target_link_libraries(test
		tbb
		tbbmalloc
		pthread
		)


