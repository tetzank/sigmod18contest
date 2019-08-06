cmake_minimum_required(VERSION 3.12)

if(NOT DEFINED CMAKE_CXX_FLAGS)
	set(CMAKE_CXX_FLAGS "-Wall -Wextra -march=native -fno-rtti -fopenmp" CACHE STRING "Flags used by the compiler during all build types.")
endif()
# build type defaults to release
if(NOT DEFINED CMAKE_BUILD_TYPE)
	set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build, options are: None(CMAKE_CXX_FLAGS or CMAKE_C_FLAGS) Debug Release RelWithdebInfo MinSizeRel.")
endif()

project(dangerousAssembly CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories("${CMAKE_SOURCE_DIR}/include/")

#FIXME: add COAT as submodule and search for it
include_directories("${CMAKE_SOURCE_DIR}/../coat_git/include/")

add_subdirectory(src)
