cmake_minimum_required(VERSION 3.16)
project(spark_urma LANGUAGES CXX C)

if(NOT CMAKE_CONFIGURATION_TYPES AND NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()
set(CMAKE_SKIP_BUILD_RPATH TRUE)
set(CMAKE_SKIP_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)

# ---- Paths ----
set(URMA_ROOT "$ENV{OU_UMDK_BUILD}" CACHE PATH "UMDK build directory")
if(NOT URMA_ROOT)
    message(FATAL_ERROR "Set OU_UMDK_BUILD or -DURMA_ROOT to the built UMDK tree")
endif()

set(URMA_VENDOR "${URMA_ROOT}/../../vendor/umdk/src/urma")
find_path(URMA_INC urma_api.h
    HINTS "$ENV{OU_UMDK_INCLUDE}" "${URMA_VENDOR}/lib/urma/core/include"
    REQUIRED NO_DEFAULT_PATH)
find_path(URMA_COMMON_INC ub_util.h
    HINTS "$ENV{OU_UMDK_COMMON_INCLUDE}" "${URMA_VENDOR}/common/include"
    REQUIRED NO_DEFAULT_PATH)
find_library(URMA_LIBRARY NAMES urma
    HINTS "${URMA_ROOT}/urma/lib/urma/core" REQUIRED NO_DEFAULT_PATH)
find_library(URMA_COMMON_LIBRARY NAMES urma_common
    HINTS "${URMA_ROOT}/urma/common" REQUIRED NO_DEFAULT_PATH)

# ---- JNI headers (the library does not link libjvm/AWT) ----
set(JAVA_HOME "$ENV{JAVA_HOME}" CACHE PATH "JDK root")
if(NOT JAVA_HOME)
    message(FATAL_ERROR "Set JAVA_HOME or -DJAVA_HOME to a JDK root")
endif()
find_path(JNI_INCLUDE_DIR jni.h HINTS "${JAVA_HOME}/include" REQUIRED NO_DEFAULT_PATH)
find_path(JNI_MD_INCLUDE_DIR jni_md.h
    HINTS "${JAVA_HOME}/include/linux" "${JAVA_HOME}/include/darwin"
    REQUIRED NO_DEFAULT_PATH)
set(JNI_INCLUDE_DIRS "${JNI_INCLUDE_DIR}" "${JNI_MD_INCLUDE_DIR}")

# ---- Library ----
set(SOURCES
    src/urma_transport.cpp
    src/tcp_control_channel.cpp
)

set(HEADERS
    include/urma_transport.h
    include/urma_types.h
    include/chunk_protocol.h
    include/transport_metrics.h
)

add_library(spark_urma_obj OBJECT ${SOURCES})
target_include_directories(spark_urma_obj PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${URMA_INC}
    ${URMA_COMMON_INC}
    ${JNI_INCLUDE_DIRS}
)
target_compile_features(spark_urma_obj PUBLIC cxx_std_17)
target_compile_options(spark_urma_obj PRIVATE -Wall -Wextra -Werror)
set_target_properties(spark_urma_obj PROPERTIES POSITION_INDEPENDENT_CODE ON)

option(BUILD_STANDALONE "Build standalone C++ transport library" OFF)
if(BUILD_STANDALONE)
    add_library(spark_urma SHARED $<TARGET_OBJECTS:spark_urma_obj>)
    target_link_libraries(spark_urma PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS})
endif()

# JNI shared library (libSparkUrma.so)
add_library(SparkUrma SHARED
    $<TARGET_OBJECTS:spark_urma_obj>
    src/spark_urma_jni.cpp
)
target_include_directories(SparkUrma PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC} ${JNI_INCLUDE_DIRS})
target_compile_features(SparkUrma PRIVATE cxx_std_17)
target_compile_options(SparkUrma PRIVATE -Wall -Wextra -Werror)
target_link_libraries(SparkUrma PRIVATE
    ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread ${CMAKE_DL_LIBS}
)
include(GNUInstallDirs)
install(TARGETS SparkUrma LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})

# ---- Tests ----
option(BUILD_TESTS "Build standalone tests" ON)
if(BUILD_TESTS)
    add_executable(native_lifecycle_test tests/native_lifecycle_test.cpp src/chunk_protocol.cpp
        $<TARGET_OBJECTS:spark_urma_obj>)
    target_include_directories(native_lifecycle_test PRIVATE
        ${URMA_INC} ${URMA_COMMON_INC} ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_link_libraries(native_lifecycle_test PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread)
    add_executable(request_state_test tests/request_state_test.cpp
        $<TARGET_OBJECTS:spark_urma_obj>)
    target_include_directories(request_state_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC})
    target_compile_options(request_state_test PRIVATE -Wall -Wextra -Werror)
    target_link_libraries(request_state_test PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread)
    add_executable(remote_import_cache_test tests/remote_import_cache_test.cpp
        $<TARGET_OBJECTS:spark_urma_obj>)
    target_include_directories(remote_import_cache_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include ${URMA_INC} ${URMA_COMMON_INC})
    target_compile_options(remote_import_cache_test PRIVATE -Wall -Wextra -Werror)
    target_link_libraries(remote_import_cache_test PRIVATE
        ${URMA_LIBRARY} ${URMA_COMMON_LIBRARY} pthread)
endif()
