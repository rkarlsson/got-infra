cmake_minimum_required(VERSION 3.10)

# set the project name
project( got-infra )

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# set(CMAKE_BUILD_TYPE Debug)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# set(CMAKE_CXX_FLAGS "-Wall -Wextra")
set(CMAKE_CXX_FLAGS "-Wall -fPIC -std=c++20 -Wno-pessimizing-move")
set(CMAKE_CXX_FLAGS_DEBUG "-g")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native")

# OS provided Libraries
find_library(CURL_LIB curl)
if(NOT CURL_LIB)
  message(FATAL_ERROR "curl library not found")
endif()

find_library(PTHREAD_LIB pthread)
if(NOT PTHREAD_LIB)
  message(FATAL_ERROR "pthread library not found")
endif()

find_library(OPENSSL_LIB ssl)
if(NOT OPENSSL_LIB)
  message(FATAL_ERROR "ssl library not found")
endif()

find_library(CRYPTO_LIB crypto)
if(NOT CRYPTO_LIB)
  message(FATAL_ERROR "crypto library not found")
endif()

find_library(Z_LIB z)
if(NOT Z_LIB)
  message(FATAL_ERROR "z library not found")
endif()

find_package(Git QUIET)
if(GIT_FOUND AND EXISTS "${PROJECT_SOURCE_DIR}/.git")
# Update submodules as needed
    option(GIT_SUBMODULE "Check submodules during build" ON)
    if(GIT_SUBMODULE)
        message(STATUS "Submodule update")
        execute_process(COMMAND ${GIT_EXECUTABLE} submodule update --init --recursive
                        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                        RESULT_VARIABLE GIT_SUBMOD_RESULT)
        if(NOT GIT_SUBMOD_RESULT EQUAL "0")
            message(FATAL_ERROR "git submodule update --init --recursive failed with ${GIT_SUBMOD_RESULT}, please checkout submodules")
        endif()
    endif()
endif()

if(NOT EXISTS "${PROJECT_SOURCE_DIR}/submodules/got-base/CMakeLists.txt")
    message(FATAL_ERROR "The submodules were not downloaded! GIT_SUBMODULE was turned off or failed. Please update submodules and try again.")
endif()

add_subdirectory(submodules/got-base)
add_subdirectory(submodules/influxdb-cxx)
add_subdirectory(submodules/wolfssl)
add_subdirectory(submodules/prometheus-cpp)

# INCLUDE PATHS
###################################################
set(GOT_BASE_INCL submodules/got-base/include )
set(MYSQL_INCL submodules/got-base/submodules/mysql-connector-cpp/include/mysqlx )
set(AERON_INCL submodules/got-base/submodules/aeron/aeron-client/src/main/cpp )
set(INFLUX_INCL submodules/influxdb-cxx/include )
set(WOLFSSL_INCL submodules/wolfssl )
set(JSON_INCL ./submodules/json )
set(PROMETHEUS_INCL_DETAIL build/submodules/prometheus-cpp/core/include )
set(PROMETHEUS_INCL_PULL_DETAIL build/submodules/prometheus-cpp/pull/include )
set(PROMETHEUS_INCL submodules/prometheus-cpp/core/include )
set(PROMETHEUS_INCL_PULL submodules/prometheus-cpp/pull/include )

include_directories( 
    ${CMAKE_CURRENT_SOURCE_DIR}/ 
    ${GOT_BASE_INCL} 
    ${WOLFSSL_INCL} 
    ${JSON_INCL} 
    ${MYSQL_INCL} 
    ${AERON_INCL} 
    ${INFLUX_INCL}
    ${PROMETHEUS_INCL_DETAIL}
    ${PROMETHEUS_INCL_PULL}
    ${PROMETHEUS_INCL_PULL_DETAIL}
    ${PROMETHEUS_INCL}
    ./include
)

# Set variables I can use for collected library dependencies
###################################################
set(EXTERNAL_LIBRARIES ${CURL_LIB} ${PTHREAD_LIB} aeron_client ${Z_LIB} )

add_subdirectory(src)
